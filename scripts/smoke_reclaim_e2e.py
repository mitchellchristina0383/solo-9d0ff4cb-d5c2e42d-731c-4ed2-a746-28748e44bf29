#!/usr/bin/env python3
"""端到端冒烟（两批同名段 + kill -9 中断续跑）。

在仓库既有 smoke_e2e.py 的真实 uvicorn 进程模式上，额外验证回收修复：

  场景 A 正常回收复用：
    作业 A 推进 -> 段被回收 -> 上游在同路径放下一批（字节完全相同）
    -> 旧作业再推进不得删除新段；新作业 B 登记同名段并完整读完。
  场景 B 中断续跑：
    崩溃注入（after_reclaim_unlink）后上游放新段 -> 重启服务，
    补做回收对账时保留新段；已发布页字节不变，作业 A 不漏不重。

只依赖标准库；用法： PYTHONPATH=/tmp/pylibs python3 -I scripts/smoke_reclaim_e2e.py
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
BASE = "http://127.0.0.1:8766"


def http(method: str, path: str, body: object = None) -> tuple[int, object]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            ctype = resp.headers.get("Content-Type", "")
            if ctype.split(";")[0].strip() == "application/json":
                return resp.status, json.loads(raw)
            return resp.status, raw
    except urllib.error.HTTPError as e:
        payload = e.read()
        try:
            return e.code, json.loads(payload)
        except json.JSONDecodeError:
            return e.code, {"raw": payload.decode(errors="replace")}
    except (urllib.error.URLError, ConnectionError, OSError) as e:
        # 崩溃注入点在响应写回前触发：连接被重置/对端关闭，属预期
        return 0, {"connection_error": str(e)}


def wait_ready(proc: subprocess.Popen) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            raise RuntimeError("服务进程提前退出")
        try:
            code, _ = http("GET", "/healthz")
            if code == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("服务未在 10s 内就绪")


def start_server(work: Path, mount: Path, crash_point: str | None = None) -> subprocess.Popen:
    env = os.environ.copy()
    env["MERGE_MOUNT_DIR"] = str(mount)
    env["MERGE_WORK_DIR"] = str(work)
    extra = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + extra if extra else "")
    if crash_point:
        env["MERGE_CRASH_HOOK"] = crash_point
    # 不用 -I：依赖位于外部 site/target 目录，需要 PYTHONPATH 生效；
    # 本脚本处理的都是仓库自带或本脚本生成的数据，无外部不可信输入。
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", "8766", "--log-level", "warning"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    wait_ready(proc)
    return proc


def stop(proc: subprocess.Popen, sig: int = signal.SIGTERM) -> None:
    proc.send_signal(sig)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()


def write_segments(mount: Path) -> None:
    seg_dir = mount / "segments"
    seg_dir.mkdir(parents=True, exist_ok=True)
    runs = {
        "run-0.jsonl": [1, 2],
        "run-1.jsonl": [3, 4, 5, 6],
    }
    for name, keys in runs.items():
        with open(seg_dir / name, "w", encoding="utf-8") as f:
            for i, k in enumerate(keys):
                f.write(json.dumps({"key": k, "payload": f"{name}-{i}"}) + "\n")


def replace_reclaimed_segment(mount: Path) -> None:
    """只在已回收的 run-0 同路径放下一批；run-1 仍归旧作业消费。"""
    seg_dir = mount / "segments"
    with open(seg_dir / "run-0.jsonl", "w", encoding="utf-8") as f:
        for i, k in enumerate([1, 2]):
            f.write(json.dumps({"key": k, "payload": f"run-0.jsonl-{i}"}) + "\n")


def job_body(job_id: str) -> dict:
    return {
        "job_id": job_id,
        "key_field": "key",
        "page_size": 2,
        "segments": [
            {"path": "segments/run-0.jsonl", "order": 0},
            {"path": "segments/run-1.jsonl", "order": 1},
        ],
    }


def all_page_keys(job_id: str) -> list[int]:
    code, pages = http("GET", f"/jobs/{job_id}/pages")
    assert code == 200, pages
    keys: list[int] = []
    for meta in pages:
        code, raw = http("GET", f"/jobs/{job_id}/pages/{meta['page_no']}")
        assert code == 200
        keys.extend(json.loads(line)["key"] for line in raw.decode().splitlines())
    return keys


def scenario_a_normal_reuse(tmp: Path) -> None:
    print("[A] 正常回收后同路径复用")
    mount, work = tmp / "a-input", tmp / "a-work"
    mount.mkdir(), work.mkdir()
    write_segments(mount)
    seg0 = mount / "segments/run-0.jsonl"
    original_bytes = seg0.read_bytes()

    proc = start_server(work, mount)
    try:
        code, _ = http("POST", "/jobs", job_body("A"))
        assert code == 201
        code, adv = http("POST", "/jobs/A/advance")
        assert code == 200 and "segments/run-0.jsonl" in adv["reclaimed"], adv
        assert not seg0.exists()

        # 上游在同路径放下一批（字节完全相同，只动已回收的 run-0）
        replace_reclaimed_segment(mount)
        assert seg0.read_bytes() == original_bytes  # 两批字节级一致

        code, run = http("POST", "/jobs/A/run")
        assert code == 200 and run["finished"], run
        assert seg0.exists(), "旧作业不得误删下一批段"
        assert seg0.read_bytes() == original_bytes
        assert "segments/run-0.jsonl" in run["deferred"], run
        assert any("run-0.jsonl" in w for w in run["warnings"]), run

        # 已发布页整批校验
        assert all_page_keys("A") == [1, 2, 3, 4, 5, 6]

        # 上游补齐下一批 run-1，新作业 B 登记同名段并完整读完
        write_segments(mount)
        code, _ = http("POST", "/jobs", job_body("B"))
        assert code == 201
        code, run_b = http("POST", "/jobs/B/run")
        assert code == 200 and run_b["finished"], run_b
        assert all_page_keys("B") == [1, 2, 3, 4, 5, 6]
    finally:
        stop(proc)
    print("    OK：新段保留、告警上报、作业 B 完整读完自己登记的同名段")


def scenario_b_crash_resume(tmp: Path) -> None:
    print("[B] unlink 后崩溃、重启续跑保留新批")
    mount, work = tmp / "b-input", tmp / "b-work"
    mount.mkdir(), work.mkdir()
    write_segments(mount)
    seg0 = mount / "segments/run-0.jsonl"
    original_bytes = seg0.read_bytes()

    # 进程 #1：打开崩溃注入点
    proc = start_server(work, mount, crash_point="after_reclaim_unlink")
    try:
        code, _ = http("POST", "/jobs", job_body("A"))
        assert code == 201
        code, body = http("POST", "/jobs/A/advance")
        # 注入点在响应写回前抛异常，连接可能被重置（code=0）
        assert code in (0, 200, 500), (code, body)
        assert not seg0.exists(), "旧批应已 unlink"
    finally:
        stop(proc, signal.SIGKILL)

    # 上游在同路径放下一批（字节完全相同，只动已回收的 run-0）
    replace_reclaimed_segment(mount)
    assert seg0.read_bytes() == original_bytes

    # 进程 #2：干净进程，重启续跑
    proc2 = start_server(work, mount)
    try:
        code, status = http("GET", "/jobs/A")
        assert code == 200 and status["published_pages"] == 1, status
        code, run = http("POST", "/jobs/A/run")
        assert code == 200 and run["finished"], run
        assert seg0.exists() and seg0.read_bytes() == original_bytes
        assert "segments/run-0.jsonl" in run["deferred"], run
        assert any("run-0.jsonl" in w for w in run["warnings"])
        assert all_page_keys("A") == [1, 2, 3, 4, 5, 6]

        # 再重启一次：保留决定持久化，告警不堆积
    finally:
        stop(proc2)

    proc3 = start_server(work, mount)
    try:
        code, adv = http("POST", "/jobs/A/advance")
        assert code == 200 and adv["page_no"] is None
        assert seg0.exists()
        code, status = http("GET", "/jobs/A")
        assert sum("run-0.jsonl" in w for w in status["warnings"]) == 1
        seg0_state = next(s for s in status["segments"] if s["path"].endswith("run-0.jsonl"))
        assert seg0_state["reclaim_deferred"] is True and seg0_state["reclaimed"] is False

        # 新作业 B 读自己登记的同名段（补齐 run-1）
        write_segments(mount)
        code, _ = http("POST", "/jobs", job_body("B"))
        assert code == 201
        code, _ = http("POST", "/jobs/B/run")
        assert all_page_keys("B") == [1, 2, 3, 4, 5, 6]
    finally:
        stop(proc3)
    print("    OK：崩溃窗口后续跑保留新批、页不变、告警去重、作业 B 完整读完")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="merge-reclaim-smoke-"))
    scenario_a_normal_reuse(tmp)
    scenario_b_crash_resume(tmp)
    print(f"OK：两批同名段场景全部通过（临时目录 {tmp}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
