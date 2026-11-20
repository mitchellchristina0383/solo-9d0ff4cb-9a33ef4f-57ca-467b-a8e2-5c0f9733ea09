#!/usr/bin/env python3
"""共享段回收的真实进程端到端冒烟（标准库实现，不依赖第三方包）。

用法（在仓库根目录）::

    python scripts/smoke_shared.py

场景：
  1. 两个作业 A/B 登记同一份 JSONL 段（另各带一条独有段）；
  2. A 先跑完：共享段必须保留（B 仍登记），A 的独有段被回收；
  3. kill -9 中断进程后重启；
  4. B 续跑完整读完共享段，最后由 B 回收；A 的已发布页字节不变。

与 smoke_e2e.py 一样，子进程以仓库根为 cwd 启动 uvicorn，
不依赖 PYTHONPATH，便于在精简环境里运行。
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
        return e.code, json.loads(e.read())


def wait_ready(proc: subprocess.Popen) -> None:
    for _ in range(100):
        if proc.poll() is not None:
            out = proc.stdout.read().decode() if proc.stdout else ""
            raise RuntimeError(f"服务进程提前退出:\n{out}")
        try:
            code, _ = http("GET", "/healthz")
            if code == 200:
                return
        except OSError:
            pass
        time.sleep(0.1)
    raise RuntimeError("服务未在 10s 内就绪")


def start_server(work: Path, mount: Path) -> subprocess.Popen:
    env = os.environ.copy()
    env["MERGE_MOUNT_DIR"] = str(mount)
    env["MERGE_WORK_DIR"] = str(work)
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", "8766", "--log-level", "warning"],
        cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    wait_ready(proc)
    return proc


def write_jsonl(path: Path, keys: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for i, k in enumerate(keys):
            f.write(json.dumps({"key": k, "line": i}) + "\n")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="merge-shared-"))
    mount, work = tmp / "input", tmp / "work"
    mount.mkdir()
    work.mkdir()

    write_jsonl(mount / "shared.jsonl", list(range(1, 9)))      # 8 行
    write_jsonl(mount / "only-a.jsonl", [100, 101])
    write_jsonl(mount / "only-b.jsonl", [200, 201])

    job_a = {
        "job_id": "A", "key_field": "key", "page_size": 3,
        "segments": [
            {"path": "shared.jsonl", "order": 0},
            {"path": "only-a.jsonl", "order": 1},
        ],
    }
    job_b = {
        "job_id": "B", "key_field": "key", "page_size": 4,
        "segments": [
            {"path": "shared.jsonl", "order": 0},
            {"path": "only-b.jsonl", "order": 1},
        ],
    }

    print("[1] 启动服务进程 #1，登记两个共用 shared.jsonl 的作业")
    proc = start_server(work, mount)
    try:
        assert http("POST", "/jobs", job_a)[0] == 201
        assert http("POST", "/jobs", job_b)[0] == 201

        print("[2] A 整批跑完：共享段保留，A 的独有段回收")
        code, run_a = http("POST", "/jobs/A/run")
        assert code == 200 and run_a["finished"], run_a
        assert (mount / "shared.jsonl").exists(), "B 仍登记 shared.jsonl，不得删除"
        assert not (mount / "only-a.jsonl").exists(), "A 的独有段应已回收"

        # 记录 A 已发布页字节，供重启后比对
        a_pages = sorted((work / "A").glob("page-*.jsonl"))
        a_bytes = {p.name: p.read_bytes() for p in a_pages}

        print("[3] kill -9 进程，模拟在 B 回收完成前宕机")
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()

    print("[4] 启动服务进程 #2，B 续跑")
    proc2 = start_server(work, mount)
    try:
        code, status = http("GET", "/jobs/B")
        assert code == 200 and status["published_pages"] == 0, status
        assert (mount / "shared.jsonl").exists(), "重启后共享段必须仍在"

        code, run_b = http("POST", "/jobs/B/run")
        assert code == 200 and run_b["finished"], run_b

        print("[5] 校验输出完整性与回收结果")
        # B 的全量输出：shared 8 行(1..8) + only-b 2 行(200,201)，归并有序
        pages = http("GET", "/jobs/B/pages")[1]
        all_keys: list[int] = []
        for meta in pages:
            code, raw = http("GET", f"/jobs/B/pages/{meta['page_no']}")
            all_keys.extend(json.loads(line)["key"] for line in raw.decode().splitlines())
        assert sorted(all_keys) == list(range(1, 9)) + [200, 201], all_keys

        assert not (mount / "shared.jsonl").exists(), "B 是最后引用方，应回收共享段"
        assert not (mount / "only-b.jsonl").exists()

        # A 的已发布页字节在重启后保持原样
        for name, data in a_bytes.items():
            assert (work / "A" / name).read_bytes() == data, f"{name} 被改动"

        # A/B 再打开时对账补标 reclaimed
        assert http("POST", "/jobs/A/advance")[0] == 200
        st_a = http("GET", "/jobs/A")[1]
        assert all(s["reclaimed"] for s in st_a["segments"]), st_a
        st_b = http("GET", "/jobs/B")[1]
        assert all(s["reclaimed"] for s in st_b["segments"]), st_b
    finally:
        proc2.send_signal(signal.SIGTERM)
        try:
            proc2.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc2.kill()

    print("OK：共享段保留到最后一个作业发完；kill -9 后续跑不漏不重；已发布页不变")
    return 0


if __name__ == "__main__":
    sys.exit(main())
