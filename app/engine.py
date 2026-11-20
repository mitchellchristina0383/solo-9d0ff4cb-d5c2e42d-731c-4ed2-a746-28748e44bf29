"""归并引擎：k 路归并 + 分页发布 + 段文件回收 + 断点续跑。

设计要点
--------
1. 内存占用：每一路任意时刻只保留一条待比较记录，用堆选最小
   ``(key, 路序 order, 路内行序 seq)``。整段文件不进内存，
   通过字节偏移 ``offset`` 顺序扫描。
2. 页是发布单元：先写 ``page-XXXXX.jsonl.tmp``，fsync 后原子
   ``os.replace`` 成正式页文件，再写新的 ``state.json``（同样
   tmp + fsync + replace）。任何时刻崩溃，已发布页都保持原样。
3. 回收（两阶段提交 + 文件身份围栏）：某路全部记录随页发布后，
   先提交 ``exhausted + 偏移`` 状态，再 **核对当前路径上的文件
   与登记时捕获的身份（``FileIdentity``）一致** 才 unlink，最后
   提交 ``reclaimed`` 标记。上游在同路径放下一批段（即使字节
   完全相同）时，身份核对失败 -> 保留新文件、告警并置
   ``reclaim_deferred``，旧作业绝不会误删下一批。任何一步崩溃，
   重启时 ``_open_job`` 都能依据状态对账并补做回收。
4. 续跑：状态只记录各路下一条待比较记录的字节偏移和已发布页数，
   进程重启后打开文件 ``seek(offset)`` 继续，未发布记录重新参与
   归并，整批不丢不重。
"""

from __future__ import annotations

import hashlib
import heapq
import json
import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from .models import (
    AdvanceResult,
    FileIdentity,
    JobCreate,
    JobStatus,
    PageMeta,
    RunResult,
    SegmentState,
)

logger = logging.getLogger("merge_service")

STATE_FILENAME = "state.json"
PAGE_PREFIX = "page-"
PAGE_SUFFIX = ".jsonl"
TMP_SUFFIX = ".tmp"

# 崩溃注入点（测试用）：
#  after_page_tmp       页临时文件写完并 fsync、尚未 rename（正式页不存在）
#  after_page_rename    页已原子发布、state.json 尚未更新
#  after_state_commit   state.json 已原子提交（tmp+rename 均完成）
#  after_reclaim_unlink 段文件已 unlink、reclaimed 标记尚未提交
CrashHook = Callable[[str], None]


def _fsync_dir(path: Path) -> None:
    """fsync 目录，保证 rename / unlink 落盘。"""
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(
    dst: Path,
    data: bytes,
    crash_hook: CrashHook | None = None,
    pre_rename_hook: str | None = None,
    post_rename_hook: str | None = None,
) -> None:
    """同目录 tmp + fsync + 原子 rename，崩溃后不会出现半截正式文件。

    可选注入点分别位于 rename 前后，用于模拟两类进程中断。
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_name(dst.name + TMP_SUFFIX)
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    if crash_hook is not None and pre_rename_hook is not None:
        crash_hook(pre_rename_hook)
    os.replace(tmp, dst)
    _fsync_dir(dst.parent)
    if crash_hook is not None and post_rename_hook is not None:
        crash_hook(post_rename_hook)


def page_filename(page_no: int) -> str:
    return f"{PAGE_PREFIX}{page_no:05d}{PAGE_SUFFIX}"


def _page_no_of(name: str) -> int:
    return int(name[len(PAGE_PREFIX) : -len(PAGE_SUFFIX)])


class EngineError(Exception):
    """引擎层错误（输入不合法、状态损坏等）。"""


class JobNotFound(EngineError):
    pass


@dataclass
class _RunReader:
    """某一路在一次 advance 内的运行态。"""

    index: int
    state: SegmentState
    f: Any  # 已打开的文本文件对象
    seq: int  # 路内下一条记录的行序号（仅用于堆排序，不持久化）


@dataclass
class _HeapEntry:
    """堆条目。堆只按 (key, order, seq) 排序，三者天然唯一：
    order 区分路，seq 区分路内行序；其余字段不参与比较。"""

    key: int
    order: int
    seq: int
    run_index: int = field(compare=False)
    offset: int = field(default=0, compare=False)
    raw: bytes = field(default=b"", compare=False)

    def __lt__(self, other: "_HeapEntry") -> bool:
        return (self.key, self.order, self.seq) < (other.key, other.order, other.seq)


class MergeService:
    """归并服务，按挂载目录管理多个作业。"""

    # 类级锁表：同进程内多个服务实例（如测试、多 app）操作同一作业也互斥。
    # key = (工作目录, job_id)，不同工作目录互不影响。
    _global_locks: dict[tuple[str, str], threading.Lock] = {}
    _global_locks_guard = threading.Lock()

    def __init__(self, mount_dir: Path, work_dir: Path, crash_hook: CrashHook | None = None):
        self.mount_dir = Path(mount_dir).resolve()
        self.work_dir = Path(work_dir).resolve()
        self.mount_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self._crash_hook: CrashHook | None = crash_hook

    # ------------------------------------------------------------------ #
    # 路径与校验
    # ------------------------------------------------------------------ #
    def _job_dir(self, job_id: str) -> Path:
        if "/" in job_id or "\\" in job_id or ".." in job_id:
            raise EngineError(f"非法 job_id: {job_id}")
        return (self.work_dir / job_id).resolve()

    def _state_path(self, job_id: str) -> Path:
        return self._job_dir(job_id) / STATE_FILENAME

    def _segment_path(self, rel: str) -> Path:
        p = (self.mount_dir / rel).resolve()
        # 防目录穿越：段文件必须在挂载目录内
        if self.mount_dir not in p.parents and p != self.mount_dir:
            raise EngineError(f"段路径越出挂载目录: {rel}")
        return p

    # ------------------------------------------------------------------ #
    # 段文件身份（回收围栏，防止误删上游放下的下一批同名段）
    # ------------------------------------------------------------------ #
    @staticmethod
    def _sha256_of(path: Path) -> str:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()

    @staticmethod
    def _stat_matches(identity: FileIdentity, st: os.stat_result) -> bool:
        return (
            st.st_dev == identity.dev
            and st.st_ino == identity.ino
            and st.st_size == identity.size
            and st.st_mtime_ns == identity.mtime_ns
        )

    def _is_same_file(self, path: Path, identity: FileIdentity) -> bool:
        """路径上的当前文件是否就是登记时的那一批。

        先比对 stat 身份（dev/inode/size/mtime_ns）；stat 全部吻合时
        再比对 SHA-256 兜底。两份字节完全相同的替换文件 inode 或
        mtime 不同，stat 阶段即被拦下，不会被当成同一批。
        """
        try:
            st = path.stat()
        except OSError:
            return False
        if not self._stat_matches(identity, st):
            return False
        return self._sha256_of(path) == identity.sha256

    def _warn(self, status: JobStatus, message: str) -> bool:
        """作业级去告警去重并落日志；返回是否为新告警。"""
        if message in status.warnings:
            return False
        status.warnings.append(message)
        logger.warning("job=%s %s", status.job_id, message)
        return True

    def _read_state(self, job_id: str) -> JobStatus:
        path = self._state_path(job_id)
        if not path.exists():
            raise JobNotFound(f"作业不存在: {job_id}")
        try:
            return JobStatus.model_validate(json.loads(path.read_text(encoding="utf-8")))
        except (ValueError, OSError) as e:
            raise EngineError(f"状态文件损坏: {path}: {e}") from e

    def _write_state(self, job_id: str, status: JobStatus) -> None:
        data = (json.dumps(status.model_dump(), ensure_ascii=False) + "\n").encode("utf-8")
        atomic_write_bytes(
            self._state_path(job_id),
            data,
            crash_hook=self._crash_hook,
            post_rename_hook="after_state_commit",
        )

    def _get_lock(self, job_id: str) -> threading.Lock:
        key = (str(self.work_dir), job_id)
        with self._global_locks_guard:
            lock = self._global_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._global_locks[key] = lock
            return lock

    # ------------------------------------------------------------------ #
    # 登记
    # ------------------------------------------------------------------ #
    def register_job(self, req: JobCreate) -> JobStatus:
        job_dir = self._job_dir(req.job_id)
        with self._get_lock(req.job_id):
            state_path = job_dir / STATE_FILENAME
            if state_path.exists():
                existing = self._read_state(req.job_id)
                # 幂等：登记参数一致则直接返回当前状态，便于客户端重试
                same = (
                    existing.key_field == req.key_field
                    and existing.page_size == req.page_size
                    and [s.path for s in existing.segments] == [s.path for s in req.segments]
                    and [s.order for s in existing.segments] == [s.order for s in req.segments]
                )
                if same:
                    return existing
                raise EngineError(f"作业 {req.job_id} 已存在且登记参数不一致")

            # 校验段文件：存在、可读、键为整数且全局非降序；
            # 同一次流式扫描顺带计算 SHA-256，登记时固化文件身份
            seg_states: list[SegmentState] = []
            for seg in req.segments:
                p = self._segment_path(seg.path)
                if not p.is_file():
                    raise EngineError(f"段文件不存在: {seg.path}")
                digest = self._validate_sorted_segment(p, req.key_field, seg.path)
                st = p.stat()
                identity = FileIdentity(
                    dev=st.st_dev,
                    ino=st.st_ino,
                    size=st.st_size,
                    mtime_ns=st.st_mtime_ns,
                    sha256=digest,
                )
                seg_states.append(
                    SegmentState(path=seg.path, order=seg.order, file_id=identity)
                )

            job_dir.mkdir(parents=True, exist_ok=True)
            status = JobStatus(
                job_id=req.job_id,
                key_field=req.key_field,
                page_size=req.page_size,
                segments=seg_states,
                published_pages=0,
                total_published_rows=0,
                finished=False,
                next_page_no=1,
                page_counts=[],
            )
            self._write_state(req.job_id, status)
            return status

    def _validate_sorted_segment(self, path: Path, key_field: str, rel: str) -> str:
        """流式校验：每行是 JSON 对象，键为整数，且键非降序。

        同一次扫描计算整文件 SHA-256 并返回，供登记时固化文件身份，
        避免为算摘要再读一遍大段。
        """
        digest = hashlib.sha256()
        prev: int | None = None
        with open(path, "rb") as f:
            for lineno, raw in enumerate(f, 1):
                digest.update(raw)
                if not raw.strip():
                    continue
                try:
                    rec = json.loads(raw)
                except json.JSONDecodeError as e:
                    raise EngineError(f"{rel}:{lineno} 不是合法 JSON: {e}") from e
                if not isinstance(rec, dict):
                    raise EngineError(f"{rel}:{lineno} 不是 JSON 对象")
                key = rec.get(key_field)
                if not isinstance(key, int) or isinstance(key, bool):
                    raise EngineError(f"{rel}:{lineno} 键字段 {key_field!r} 不是整数")
                if prev is not None and key < prev:
                    raise EngineError(f"{rel}:{lineno} 键 {key} 小于前一键 {prev}，段未排序")
                prev = key
        return digest.hexdigest()

    # ------------------------------------------------------------------ #
    # 启动恢复
    # ------------------------------------------------------------------ #
    def _open_job(self, job_id: str) -> tuple[JobStatus, list[str], list[str]]:
        """载入作业并做崩溃对账。

        返回 ``(状态, 本次对账实际删除的段路径, 本次对账保留未删的段路径)``。
        """
        status = self._read_state(job_id)
        job_dir = self._job_dir(job_id)

        # 1) 清理所有 tmp（页 tmp / state tmp），它们从未提交
        for tmp in job_dir.glob(f"*{TMP_SUFFIX}"):
            tmp.unlink()

        # 2) 正式页只允许 1..published_pages；多余的是 rename 后、
        #    state 落盘前崩溃留下的孤儿页，删掉重新发布
        valid_pages = set(range(1, status.published_pages + 1))
        for pf in job_dir.glob(f"{PAGE_PREFIX}*{PAGE_SUFFIX}"):
            no = _page_no_of(pf.name)
            if no not in valid_pages:
                pf.unlink()

        # 3) 段文件回收对账（带文件身份围栏）：
        #    - exhausted 且尚未 reclaimed：两阶段回收阶段 2/3 之间崩溃，
        #      或正常推进中尚未执行回收；仅当路径上仍是登记的同一批文件
        #      才补删补标，否则保留并告警（上游已放下一批）。
        #    - 已 reclaimed 但文件又出现：同样先核身份，是本批才补删，
        #      非本批（下一批同名段）一律保留。
        #    - 旧版状态没有 file_id：无法确认当前文件属于哪一批，
        #      保留文件并把路径报进 warnings。
        changed = False
        reclaimed_now: list[str] = []
        deferred_now: list[str] = []
        for seg in status.segments:
            if not seg.exhausted and not seg.reclaimed:
                continue
            if seg.reclaim_deferred:
                # 此前已决定保留：永不回头删它，只保留既有告警
                continue
            deleted = self._fenced_unlink(status, seg)
            if deleted is True:
                # 确认是本批文件（如 unlink 未落盘的补删）：置回收标记
                reclaimed_now.append(seg.path)
                if not seg.reclaimed:
                    seg.reclaimed = True
                    changed = True
            elif deleted is False:
                # 同路径是非本批文件 / 旧版状态无法确认身份：
                # 保留文件并置 reclaim_deferred；此前可能误置过
                # reclaimed=True，以保留决定为准清除它
                seg.reclaimed = False
                changed = True
                deferred_now.append(seg.path)
            elif seg.exhausted and not seg.reclaimed:
                # 路径上没有文件且未标回收（正常 unlink 后崩溃的中间态）：补标
                seg.reclaimed = True
                changed = True
        if changed:
            self._write_state(job_id, status)

        return status, reclaimed_now, deferred_now

    def _fenced_unlink(self, status: JobStatus, seg: SegmentState) -> bool | None:
        """对一个已耗尽段执行带身份围栏的删除。

        返回值：
          True  -> 确认是登记的同一批文件，已 unlink（调用方置 reclaimed）
          False -> 同路径存在非本批文件，或旧版状态无法确认身份：
                   保留文件、置 reclaim_deferred、写入告警
          None  -> 路径上当前没有文件（无需动作）
        """
        p = self._segment_path(seg.path)
        if not p.exists():
            return None

        if seg.reclaim_deferred:
            # 此前已判定同路径为非本批文件（或身份不可确认）并决定保留：
            # 决定永久有效，之后任何对账都不再回头删除它
            return None

        if seg.file_id is None:
            # 旧版状态：没有登记时的文件身份，无法确认当前文件属于哪一批
            seg.reclaim_deferred = True
            self._warn(
                status,
                f"段 {seg.path}（{p}）的旧版状态缺少文件身份，"
                "无法确认当前文件属于哪一批，已保留文件、跳过回收",
            )
            return False

        if self._is_same_file(p, seg.file_id):
            p.unlink()
            _fsync_dir(p.parent)
            return True

        # 同路径上的文件不是登记的那一批：上游已放入下一批，绝不能删
        seg.reclaim_deferred = True
        self._warn(
            status,
            f"段 {seg.path}（{p}）与登记的文件身份不一致，"
            "疑似上游已放入下一批同名段，已保留文件、跳过回收",
        )
        return False

    # ------------------------------------------------------------------ #
    # 推进一页
    # ------------------------------------------------------------------ #
    def advance(self, job_id: str) -> AdvanceResult:
        with self._get_lock(job_id):
            return self._advance_locked(job_id)

    def _advance_locked(self, job_id: str) -> AdvanceResult:
        status, reconciled_reclaimed, reconciled_deferred = self._open_job(job_id)
        if status.finished:
            deferred = list(dict.fromkeys(
                reconciled_deferred
                + [s.path for s in status.segments if s.reclaim_deferred]
            ))
            return AdvanceResult(
                page_no=None,
                page_size=status.page_size,
                rows=0,
                finished=True,
                reclaimed=reconciled_reclaimed,
                deferred=deferred,
                warnings=list(status.warnings),
                status=status,
            )

        key_field = status.key_field
        page_size = status.page_size
        readers: dict[int, _RunReader] = {}
        heap: list[_HeapEntry] = []

        # 打开每一路，seek 到持久化偏移，载入第一条待比较记录。
        # 先统一做存在性与文件身份核对（不持有句柄），避免中途报错泄漏 fd。
        live_segments = [
            (idx, seg) for idx, seg in enumerate(status.segments) if not seg.exhausted
        ]
        for idx, seg in live_segments:
            p = self._segment_path(seg.path)
            if not p.exists():
                raise EngineError(
                    f"段文件 {seg.path}（{p}）已不存在但作业尚未消费完，无法续跑"
                )
            # 作业仍在消费该段时，同路径文件必须仍是登记的那一批。
            # 这里走 stat 快速核对（替换文件会产生新 inode/mtime）；
            # 回收删除时再用 stat + SHA-256 双重围栏。
            if seg.file_id is not None and not self._stat_matches(
                seg.file_id, p.stat()
            ):
                raise EngineError(
                    f"段文件 {seg.path}（{p}）与登记的文件身份不一致，"
                    "疑似在作业消费期间被替换为其他批次，已保留文件，请人工核对"
                )

        for idx, seg in live_segments:
            p = self._segment_path(seg.path)
            f = open(p, "rb")
            f.seek(seg.offset)
            reader = _RunReader(index=idx, state=seg, f=f, seq=0)
            readers[idx] = reader
            entry = self._next_entry(reader, key_field)
            if entry is not None:
                heapq.heappush(heap, entry)
            # 第一条就为空（offset 已到 EOF）：留到发布后统一标记 exhausted

        page_records: list[bytes] = []
        try:
            while heap and len(page_records) < page_size:
                top = heapq.heappop(heap)
                page_records.append(top.raw)
                reader = readers[top.run_index]
                nxt = self._next_entry(reader, key_field)
                if nxt is not None:
                    heapq.heappush(heap, nxt)

            if not page_records:
                # 没有任何待处理记录：收尾结束（无新页，不改动已发布内容）
                for reader in readers.values():
                    reader.state.exhausted = True
                status.finished = True
                reclaimed, deferred = self._commit_and_reclaim(job_id, status)
                reclaimed = list(dict.fromkeys(reconciled_reclaimed + reclaimed))
                deferred = list(dict.fromkeys(reconciled_deferred + deferred))
                return AdvanceResult(
                    page_no=None,
                    page_size=page_size,
                    rows=0,
                    finished=True,
                    reclaimed=reclaimed,
                    deferred=deferred,
                    warnings=list(status.warnings),
                    status=status,
                )

            page_no = status.next_page_no
            page_path = self._job_dir(job_id) / page_filename(page_no)

            # 1) 原子发布页（tmp -> fsync -> rename）
            payload = b"".join(page_records)
            atomic_write_bytes(
                page_path,
                payload,
                crash_hook=self._crash_hook,
                pre_rename_hook="after_page_tmp",
                post_rename_hook="after_page_rename",
            )

            # 2) 推进各路偏移 / 耗尽标记
            # 堆里还留着的路说明仍有待处理记录；不在堆里的路已 EOF。
            # 注意：堆中持有的那条记录已经从文件预读出来，因此存活路
            # 必须持久化该记录的起始偏移（entry.offset），而不是
            # f.tell()，否则重启 seek 会跳过它造成丢记录。
            alive_indexes = {e.run_index for e in heap}
            held_offsets = {e.run_index: e.offset for e in heap}
            for idx, reader in readers.items():
                seg = reader.state
                if idx not in alive_indexes:
                    seg.exhausted = True
                    seg.offset = reader.f.tell()
                else:
                    seg.offset = held_offsets[idx]

            # 3) 更新作业状态（此时先不置 reclaimed，见 _commit_and_reclaim）
            status.published_pages += 1
            status.total_published_rows += len(page_records)
            status.page_counts.append(len(page_records))
            if not heap:
                # 所有路都耗尽：只有末页允许短
                status.finished = True
            status.next_page_no = status.published_pages + 1

            # 4) 先提交“耗尽 + 偏移”状态，再在文件身份围栏内回收段
            #    文件，最后补提交 reclaimed/deferred 标记。这样“未消费
            #    但段文件缺失”不可能由本服务自身的崩溃造成；unlink 后
            #    崩溃由启动对账补做；同路径出现下一批文件时绝不误删。
            reclaimed_now, deferred_now = self._commit_and_reclaim(job_id, status)
            reclaimed_now = list(dict.fromkeys(reconciled_reclaimed + reclaimed_now))
            deferred_now = list(dict.fromkeys(reconciled_deferred + deferred_now))

            return AdvanceResult(
                page_no=page_no,
                page_size=page_size,
                rows=len(page_records),
                finished=status.finished,
                reclaimed=reclaimed_now,
                deferred=deferred_now,
                warnings=list(status.warnings),
                status=status,
            )
        finally:
            for reader in readers.values():
                reader.f.close()

    def _next_entry(self, reader: _RunReader, key_field: str) -> _HeapEntry | None:
        """从该路读出下一条记录，返回堆条目；EOF 返回 None。"""
        f = reader.f
        start = f.tell()
        line = f.readline()
        if line == b"":
            return None
        if not line.strip():
            # 容忍空行：跳过并继续
            reader.seq += 1
            return self._next_entry(reader, key_field)
        try:
            rec = json.loads(line)
        except json.JSONDecodeError as e:
            raise EngineError(
                f"{reader.state.path} 偏移 {start} 处不是合法 JSON: {e}"
            ) from e
        key = rec.get(key_field)
        if not isinstance(key, int) or isinstance(key, bool):
            raise EngineError(
                f"{reader.state.path} 偏移 {start} 处键字段 {key_field!r} 不是整数"
            )
        entry = _HeapEntry(
            key=key,
            order=reader.state.order,
            seq=reader.seq,
            run_index=reader.index,
            offset=start,
            raw=line if line.endswith(b"\n") else line + b"\n",
        )
        reader.seq += 1
        return entry

    def _commit_and_reclaim(
        self, job_id: str, status: JobStatus
    ) -> tuple[list[str], list[str]]:
        """两阶段回收 + 文件身份围栏，保证崩溃后可对账且不误删新批：

        阶段1  先原子提交状态（段已 exhausted、偏移已落盘，reclaimed=False）。
               此后即使段文件被删，重启也知道它已消费完，不会再去读它。
        阶段2  对每个新耗尽段调用 :meth:`_fenced_unlink`：仅当路径上仍是
               登记时的同一批文件（stat 身份 + SHA-256 双核对）才 unlink；
               同路径已被上游换成下一批（哪怕字节完全相同），或旧版状态
               没有身份可核对时，保留文件、置 reclaim_deferred 并告警。
               若在 unlink 之后、阶段3 之前崩溃，启动时由 ``_open_job``
               依据 exhausted=True、reclaimed=False 补做同样的围栏对账。
        阶段3  再次提交状态（reclaimed=True / reclaim_deferred=True、
               warnings 落盘）。

        返回 ``(本次实际删除的路径, 本次保留未删的路径)``。
        """
        # 阶段1：提交耗尽/偏移
        self._write_state(job_id, status)

        # 阶段2：带身份围栏的回收
        reclaimed: list[str] = []
        deferred: list[str] = []
        any_unlinked = False
        for seg in status.segments:
            if seg.reclaimed or seg.reclaim_deferred or not seg.exhausted:
                continue
            outcome = self._fenced_unlink(status, seg)
            if outcome is True:
                seg.reclaimed = True
                reclaimed.append(seg.path)
                any_unlinked = True
            elif outcome is False:
                deferred.append(seg.path)
            else:
                if seg.reclaim_deferred:
                    # 已决定保留的段永不改判为回收
                    continue
                # 本批文件已不在路径上（正常情况下不会走到这里，
                # 因为 exhausted 段此前必然存在）：按已回收补标
                seg.reclaimed = True
                reclaimed.append(seg.path)

        # 与历史行为一致：至少真实 unlink 过文件才触发该崩溃注入点
        if self._crash_hook is not None and any_unlinked:
            self._crash_hook("after_reclaim_unlink")

        # 阶段3：提交回收/保留标记与告警（有变化才写）
        if reclaimed or deferred:
            self._write_state(job_id, status)
        return reclaimed, deferred

    # ------------------------------------------------------------------ #
    # 整批跑完
    # ------------------------------------------------------------------ #
    def run_to_end(self, job_id: str, max_pages: int | None = None) -> RunResult:
        """连续推进到作业结束（或达到 max_pages）。

        与反复调用 :meth:`advance` 等价，但只获取一次作业锁，
        因为 threading.Lock 不可重入。
        """
        published: list[int] = []
        reclaimed: list[str] = []
        deferred: list[str] = []
        with self._get_lock(job_id):
            while True:
                result = self._advance_locked(job_id)
                if result.page_no is not None:
                    published.append(result.page_no)
                reclaimed.extend(result.reclaimed)
                for rel in result.deferred:
                    if rel not in deferred:
                        deferred.append(rel)
                if result.finished or (max_pages is not None and len(published) >= max_pages):
                    return RunResult(
                        published=published,
                        finished=result.finished,
                        reclaimed=reclaimed,
                        deferred=deferred,
                        warnings=list(result.status.warnings),
                        status=result.status,
                    )

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def list_jobs(self) -> list[JobStatus]:
        if not self.work_dir.exists():
            return []
        out: list[JobStatus] = []
        for child in sorted(self.work_dir.iterdir()):
            if (child / STATE_FILENAME).exists():
                try:
                    out.append(self._read_state(child.name))
                except EngineError:
                    continue
        return out

    def get_status(self, job_id: str) -> JobStatus:
        with self._get_lock(job_id):
            return self._read_state(job_id)

    def list_pages(self, job_id: str) -> list[PageMeta]:
        status = self.get_status(job_id)
        job_dir = self._job_dir(job_id)
        metas: list[PageMeta] = []
        for no in range(1, status.published_pages + 1):
            path = job_dir / page_filename(no)
            with open(path, "rb") as f:
                rows = sum(1 for line in f if line.strip())
            metas.append(PageMeta(page_no=no, rows=rows, filename=path.name))
        return metas

    def read_page_lines(self, job_id: str, page_no: int) -> tuple[PageMeta, Iterable[bytes]]:
        """流式读取一页，逐行 yield 原始 JSONL 字节，不整页入内存。"""
        status = self.get_status(job_id)
        if page_no < 1 or page_no > status.published_pages:
            raise EngineError(
                f"页 {page_no} 不存在；已发布 1..{status.published_pages}"
            )
        path = self._job_dir(job_id) / page_filename(page_no)

        def _iter() -> Iterable[bytes]:
            with open(path, "rb") as f:
                for line in f:
                    if line.strip():
                        yield line if line.endswith(b"\n") else line + b"\n"

        rows = status.page_counts[page_no - 1]
        return PageMeta(page_no=page_no, rows=rows, filename=path.name), _iter()
