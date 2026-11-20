"""回收围栏测试：上游在同路径放下一批段时，旧作业不得误删新段。

覆盖：
- 正常回收后同路径放下一批（字节完全相同），旧作业再推进保留新段；
- unlink 后、reclaimed 落盘前崩溃，重启补删时识别并保留新批；
- 新作业登记同名段后能完整读完自己那一批；
- 已发布页在整个过程中字节不变；
- 消费期间被替换 -> 报错且保留文件；
- 旧版状态（无 file_id）无法确认身份 -> 保留文件并在 warnings 报路径。
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.engine import MergeService, page_filename
from app.models import JobCreate, SegmentSpec

from .conftest import write_segment


class CrashSimulated(Exception):
    pass


def _job(job_id: str, page_size: int = 2) -> JobCreate:
    return JobCreate(
        job_id=job_id,
        key_field="key",
        page_size=page_size,
        segments=[
            SegmentSpec(path="segments/run-0.jsonl", order=0),
            SegmentSpec(path="segments/run-1.jsonl", order=1),
        ],
    )


def _write_batch(mount: Path, batch: str) -> None:
    """两批使用完全相同的键与行文本（字节级完全相同）。"""
    write_segment(mount, "segments/run-0.jsonl", [1, 2], extra=batch)
    write_segment(mount, "segments/run-1.jsonl", [3, 4, 5, 6], extra=batch)


def _replace_reclaimed_segment(mount: Path) -> bytes:
    """模拟上游只在被回收的 run-0 同路径放下一批；run-1 仍归旧作业消费。

    新文件与旧文件 **字节完全相同**（同样的键与行文本），只能靠
    stat 身份（inode/mtime）区分两批；返回新文件字节供断言。
    """
    write_segment(mount, "segments/run-0.jsonl", [1, 2], extra="b1")
    return (mount / "segments/run-0.jsonl").read_bytes()


def _read_all_pages(work: Path, job_id: str, n_pages: int) -> list[dict]:
    out: list[dict] = []
    for no in range(1, n_pages + 1):
        p = work / job_id / page_filename(no)
        out.extend(json.loads(line) for line in p.read_text().splitlines() if line.strip())
    return out


def _same_bytes(path: Path) -> bytes:
    return path.read_bytes()


# --------------------------------------------------------------------- #
# 1. 正常回收后同路径放下一批：旧作业再推进不得删新段
# --------------------------------------------------------------------- #
def test_normal_reclaim_then_next_batch_survives(dirs):
    mount, work = dirs
    _write_batch(mount, "b1")
    svc = MergeService(mount, work)
    svc.register_job(_job("A"))

    r1 = svc.advance("A")
    seg0_rel = "segments/run-0.jsonl"
    assert seg0_rel in r1.reclaimed
    assert not (mount / seg0_rel).exists()

    # 上游在原路径放入下一批（与旧文件字节完全相同）
    new_bytes = _replace_reclaimed_segment(mount)

    # 旧作业继续推进直到结束：对账时必须识别出这是另一批并保留
    result = svc.run_to_end("A")
    assert (mount / seg0_rel).exists()
    assert _same_bytes(mount / seg0_rel) == new_bytes
    status = result.status
    seg0_state = next(s for s in status.segments if s.path == seg0_rel)
    assert seg0_state.reclaim_deferred and not seg0_state.reclaimed
    assert any(seg0_rel in w for w in status.warnings)
    assert seg0_rel in result.deferred and seg0_rel not in result.reclaimed

    # 上游交付完整的下一批（run-1 此前也被旧作业正常回收），
    # 新作业登记同名段，完整读完自己那一批，不漏不重
    _write_batch(mount, "b1")
    svc.register_job(_job("B"))
    run_b = svc.run_to_end("B")
    assert run_b.finished
    got = [r["key"] for r in _read_all_pages(work, "B", run_b.status.published_pages)]
    assert got == [1, 2, 3, 4, 5, 6]


# --------------------------------------------------------------------- #
# 2. 崩溃在 unlink 之后、reclaimed 落盘之前：重启保留新批
# --------------------------------------------------------------------- #
def test_crash_between_unlink_and_marker_keeps_next_batch(dirs):
    mount, work = dirs
    _write_batch(mount, "b1")
    seg0 = mount / "segments/run-0.jsonl"

    def hook(point: str) -> None:
        if point == "after_reclaim_unlink":
            raise CrashSimulated(point)

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(_job("A"))
    with pytest.raises(CrashSimulated):
        svc.advance("A")
    assert not seg0.exists()  # 旧批已 unlink

    # 上游立刻在同路径放下一批（字节完全相同），随后进程重启
    new_bytes = _replace_reclaimed_segment(mount)

    svc2 = MergeService(mount, work)
    result = svc2.run_to_end("A")
    assert seg0.exists() and _same_bytes(seg0) == new_bytes
    seg0_state = next(s for s in result.status.segments if s.path == "segments/run-0.jsonl")
    assert seg0_state.reclaim_deferred
    assert any("segments/run-0.jsonl" in w for w in result.status.warnings)

    # 旧作业已发布的页保持原样，且整批不漏不重
    got_a = [
        r["key"]
        for r in _read_all_pages(work, "A", result.status.published_pages)
    ]
    assert got_a == [1, 2, 3, 4, 5, 6]

    # 上游交付完整的下一批后，新作业 B 读完自己登记的同名段
    _write_batch(mount, "b1")
    MergeService(mount, work).register_job(_job("B"))
    run_b = MergeService(mount, work).run_to_end("B")
    got_b = [r["key"] for r in _read_all_pages(work, "B", run_b.status.published_pages)]
    assert got_b == [1, 2, 3, 4, 5, 6]


# --------------------------------------------------------------------- #
# 3. 多次重启后已发布页字节不变，且新批始终存活
# --------------------------------------------------------------------- #
def test_pages_immutable_and_new_batch_survives_restarts(dirs):
    mount, work = dirs
    _write_batch(mount, "b1")
    svc = MergeService(mount, work)
    svc.register_job(_job("A", page_size=3))
    svc.advance("A")  # 发布第 1 页，段0 耗尽回收
    page1 = (work / "A" / page_filename(1)).read_bytes()

    _replace_reclaimed_segment(mount)  # 同路径下一批（仅替换已回收的 run-0）
    for _ in range(3):  # 反复“重启”推进
        MergeService(mount, work).advance("A")

    assert (work / "A" / page_filename(1)).read_bytes() == page1
    assert (mount / "segments/run-0.jsonl").exists()
    status = MergeService(mount, work).get_status("A")
    assert status.finished
    deferred = [s.path for s in status.segments if s.reclaim_deferred]
    assert deferred == ["segments/run-0.jsonl"]
    # 告警去重：不会因多次重启而堆积
    assert sum("run-0.jsonl" in w for w in status.warnings) == 1


# --------------------------------------------------------------------- #
# 4. 消费期间同路径被替换：推进报错且保留文件
# --------------------------------------------------------------------- #
def test_live_segment_replacement_is_rejected_and_kept(dirs):
    mount, work = dirs
    _write_batch(mount, "b1")
    svc = MergeService(mount, work)
    svc.register_job(_job("A", page_size=2))
    svc.advance("A")  # 段0 回收；段1 仍在消费（offset 指向中段）

    # 段1 在作业尚未消费完时被另一批替换（内容不同、inode 不同）
    write_segment(mount, "segments/run-1.jsonl", [30, 40, 50, 60], extra="bx")
    from app.engine import EngineError

    with pytest.raises(EngineError, match="文件身份不一致"):
        svc.advance("A")
    assert (mount / "segments/run-1.jsonl").exists()  # 文件被保留


# --------------------------------------------------------------------- #
# 5. 旧版状态（无 file_id）：无法确认身份 -> 保留文件并报路径
# --------------------------------------------------------------------- #
def test_legacy_state_without_identity_keeps_file_and_warns(dirs):
    mount, work = dirs
    _write_batch(mount, "b1")
    svc = MergeService(mount, work)
    svc.register_job(_job("A", page_size=2))
    svc.advance("A")  # 段0 耗尽

    # 手工把状态回退成旧版 schema：去掉 file_id，并重置 reclaimed
    state_path = work / "A" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    for seg in state["segments"]:
        seg.pop("file_id", None)
        seg["reclaim_deferred"] = False
        if seg["exhausted"]:
            seg["reclaimed"] = False
    state_path.write_text(json.dumps(state, ensure_ascii=False) + "\n", encoding="utf-8")

    # 同路径上的文件来源无法确认（写入与旧批字节完全相同的新文件）
    kept = _replace_reclaimed_segment(mount)

    svc2 = MergeService(mount, work)
    result = svc2.run_to_end("A")
    assert (mount / "segments/run-0.jsonl").read_bytes() == kept
    seg0_state = next(s for s in result.status.segments if s.path == "segments/run-0.jsonl")
    assert seg0_state.reclaim_deferred and not seg0_state.reclaimed
    assert any("segments/run-0.jsonl" in w and "旧版状态" in w for w in result.status.warnings)

    # 再次重启：决定保留后不再回头处理该段，告警不重复堆积
    again = MergeService(mount, work).advance("A")
    assert (mount / "segments/run-0.jsonl").exists()
    assert sum("run-0.jsonl" in w for w in again.status.warnings) == 1


# --------------------------------------------------------------------- #
# 6. 正常无替换场景：围栏不影响原有两阶段回收
# --------------------------------------------------------------------- #
def test_fence_does_not_block_normal_reclaim(dirs):
    mount, work = dirs
    _write_batch(mount, "b1")
    svc = MergeService(mount, work)
    svc.register_job(_job("A"))
    result = svc.run_to_end("A")
    assert result.finished and result.deferred == []
    assert not list((mount / "segments").glob("*.jsonl"))
    assert all(s.reclaimed and not s.reclaim_deferred for s in result.status.segments)
    got = [r["key"] for r in _read_all_pages(work, "A", result.status.published_pages)]
    assert got == [1, 2, 3, 4, 5, 6]


# --------------------------------------------------------------------- #
# 7. stat 身份被伪造吻合（如 inode 复用）但内容不同：SHA-256 兜底拦下
# --------------------------------------------------------------------- #
def test_sha256_fallback_when_stat_identity_matches_but_content_differs(dirs):
    mount, work = dirs
    write_segment(mount, "s.jsonl", [1, 2, 3], extra="old")
    svc = MergeService(mount, work)
    req = JobCreate(
        job_id="J", key_field="key", page_size=10,
        segments=[SegmentSpec(path="s.jsonl", order=0)],
    )
    svc.register_job(req)
    fid = svc.get_status("J").segments[0].file_id

    p = (mount / "s.jsonl").resolve()
    p.write_text(json.dumps({"key": 99, "payload": "different"}) + "\n")

    class FakeStat:
        st_dev = fid.dev
        st_ino = fid.ino
        st_size = fid.size
        st_mtime_ns = fid.mtime_ns

    orig_stat = Path.stat

    def fake_stat(self):  # type: ignore[override]
        if self == p:
            return FakeStat()
        return orig_stat(self)

    Path.stat = fake_stat
    try:
        assert svc._is_same_file(p, fid) is False
    finally:
        Path.stat = orig_stat


# --------------------------------------------------------------------- #
# 8. 重启对账的混合场景：同一次对账中补删本批 + 保留下一批，
#    且删除/保留路径都如实进入推进结果
# --------------------------------------------------------------------- #
def test_reconcile_mixes_same_batch_unlink_and_next_batch_keep(dirs):
    import hashlib

    mount, work = dirs
    write_segment(mount, "a.jsonl", [1, 2], extra="x")
    write_segment(mount, "b.jsonl", [3, 4], extra="x")
    req = JobCreate(
        job_id="J", key_field="key", page_size=10,
        segments=[
            SegmentSpec(path="a.jsonl", order=0),
            SegmentSpec(path="b.jsonl", order=1),
        ],
    )
    svc = MergeService(mount, work)
    svc.register_job(req)
    svc.run_to_end("J")

    # 回退到“阶段1 已提交、unlink 前”的中间态
    state_path = work / "J" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    for seg in state["segments"]:
        seg["reclaimed"] = False
        seg["reclaim_deferred"] = False
        seg["exhausted"] = True
    # a 放回与状态身份完全一致的本批文件（模拟 unlink 从未发生）
    write_segment(mount, "a.jsonl", [1, 2], extra="x")
    a = mount / "a.jsonl"
    stt = a.stat()
    for seg in state["segments"]:
        if seg["path"] == "a.jsonl":
            seg["file_id"] = {
                "dev": stt.st_dev, "ino": stt.st_ino, "size": stt.st_size,
                "mtime_ns": stt.st_mtime_ns,
                "sha256": hashlib.sha256(a.read_bytes()).hexdigest(),
            }
    state_path.write_text(json.dumps(state, ensure_ascii=False) + "\n", encoding="utf-8")
    # b 的同路径换成下一批
    b_new = write_segment(mount, "b.jsonl", [30, 40], extra="next")
    b_bytes = b_new.read_bytes()

    result = MergeService(mount, work).advance("J")
    assert result.reclaimed == ["a.jsonl"]
    assert result.deferred == ["b.jsonl"]
    assert not a.exists()
    assert b_new.read_bytes() == b_bytes

    # 再重启：保留决定稳定，告警不堆积。已结束作业的推进结果仍会
    # 信息性列出当前处于保留状态的段（deferred），但不会再产生新动作
    again = MergeService(mount, work).advance("J")
    assert b_new.exists()
    assert again.reclaimed == []
    assert again.deferred == ["b.jsonl"]
    final = MergeService(mount, work).get_status("J")
    assert sum("b.jsonl" in w for w in final.warnings) == 1
