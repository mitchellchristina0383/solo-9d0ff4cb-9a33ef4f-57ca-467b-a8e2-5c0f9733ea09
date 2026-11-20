"""共享段回收测试：同一份 JSONL 段被多个作业登记时的回收正确性。

回收规则（单实例服务）：

- 只要还有已登记作业没发完该段，段文件就必须保留；
- 最后一个发完该段的作业负责回收，即使它还有别的段要归并；
- 登记与回收互斥，登记成功的作业一定能读完该段；
- 进程在页提交后、回收完成前中断，续跑时按各作业已发布进度
  补做回收，已发布页保持原样。
"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from app.engine import MergeService
from app.models import JobCreate, SegmentSpec

from .conftest import write_segment
from .test_engine import CrashSimulated, _all_published


def _shared_job(job_id: str, rel: str = "shared.jsonl", page_size: int = 3) -> JobCreate:
    return JobCreate(
        job_id=job_id,
        key_field="key",
        page_size=page_size,
        segments=[SegmentSpec(path=rel, order=0)],
    )


def _write_shared(mount: Path, keys: list[int], rel: str = "shared.jsonl") -> Path:
    return write_segment(mount, rel, keys)


# --------------------------------------------------------------------- #
# 1. 两个作业登记同一段：先发完的一方不得删除文件
# --------------------------------------------------------------------- #
def test_shared_segment_kept_until_all_jobs_exhausted(dirs):
    mount, work = dirs
    seg = _write_shared(mount, list(range(1, 7)))  # 6 行，每页 3 -> 2 页

    svc = MergeService(mount, work)
    svc.register_job(_shared_job("A"))
    svc.register_job(_shared_job("B"))

    result_a = svc.run_to_end("A")
    assert result_a.finished
    assert seg.exists(), "作业 B 仍登记该段，文件必须保留"

    # A 的状态停在 exhausted/reclaimed=False：它没资格回收
    status_a = svc.get_status("A")
    seg_a = status_a.segments[0]
    assert seg_a.exhausted and seg_a.reclaimed is False

    # B 仍可完整续跑
    result_b = svc.run_to_end("B")
    assert result_b.finished
    got = [r["key"] for r in _all_published(work, "B", result_b.status.published_pages)]
    assert got == [1, 2, 3, 4, 5, 6]
    # B 是最后一个发完的作业，由它回收
    assert not seg.exists()
    status_b = svc.get_status("B")
    assert status_b.segments[0].reclaimed is True


# --------------------------------------------------------------------- #
# 2. 最后一个作业发完即回收，即使它还有别的段要归并
# --------------------------------------------------------------------- #
def test_last_consumer_reclaims_even_with_other_segments_left(dirs):
    mount, work = dirs
    shared = _write_shared(mount, [1, 2], rel="shared.jsonl")
    write_segment(mount, "extra.jsonl", [10, 20, 30])

    svc = MergeService(mount, work)
    # 作业 A 只用共享段，2 行一页发完即耗尽
    svc.register_job(_shared_job("A", page_size=10))
    # 作业 B 用共享段 + 另一段；共享段先耗尽，extra 仍有待发布记录
    svc.register_job(JobCreate(
        job_id="B", key_field="key", page_size=2,
        segments=[
            SegmentSpec(path="shared.jsonl", order=0),
            SegmentSpec(path="extra.jsonl", order=1),
        ],
    ))

    svc.run_to_end("A")
    assert shared.exists(), "B 还没发完共享段"

    # 推进 B：第一页即耗尽共享段，但 extra 还没跑完
    r1 = svc.advance("B")
    assert r1.page_no == 1
    assert not shared.exists(), "B 是共享段最后一个耗尽者，应立即回收"
    assert (mount / "extra.jsonl").exists(), "B 自己的另一段还没跑完，必须保留"
    assert "shared.jsonl" in r1.reclaimed

    # B 继续跑完剩余段，整批不漏
    rest = svc.run_to_end("B")
    assert rest.finished
    assert not (mount / "extra.jsonl").exists()
    got = [r["key"] for r in _all_published(work, "B", svc.get_status("B").published_pages)]
    assert got == [1, 2, 10, 20, 30]


# --------------------------------------------------------------------- #
# 3. 登记与回收同时发生：登记成功的作业仍能读完该段（确定性竞态）
# --------------------------------------------------------------------- #
def test_registration_racing_reclaim_still_reads_segment(dirs):
    mount, work = dirs
    seg = _write_shared(mount, list(range(1, 4)))  # 3 行，一页发完

    gate = threading.Event()      # 回收线程到达扫描点后阻塞
    reached = threading.Event()

    def hook(point: str) -> None:
        if point == "before_reclaim_scan":
            reached.set()
            gate.wait(timeout=10)
            # 不立即放行：等登记线程完成并断言后由主线程释放

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(_shared_job("A", page_size=10))

    errors: list[Exception] = []

    def worker() -> None:
        local = MergeService(mount, work, crash_hook=hook)
        try:
            local.advance("A")  # 耗尽状态已提交，阻塞在全局回收扫描前
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    t = threading.Thread(target=worker)
    t.start()
    assert reached.wait(timeout=10), "回收线程未到达注入点"

    # 此刻 A 的 exhausted 状态已落盘，但回收扫描尚未发生；
    # 并发登记 B（不同 MergeService 实例，模拟并发请求）
    other = MergeService(mount, work)
    status_b = other.register_job(_shared_job("B", page_size=10))
    assert status_b.published_pages == 0

    # 放行回收扫描：B 已在登记表中，段不得被删
    gate.set()
    t.join(timeout=10)
    assert not errors, errors
    assert seg.exists(), "登记成功的 B 仍是引用方，段必须保留"

    # B 能完整读完该段，且最后由 B 回收
    result_b = other.run_to_end("B")
    assert result_b.finished
    got = [r["key"] for r in _all_published(work, "B", result_b.status.published_pages)]
    assert got == [1, 2, 3]
    assert not seg.exists()


# --------------------------------------------------------------------- #
# 4. 回收扫描期间登记被串行化：回收先完成则登记报“段不存在”，
#    不会出现“登记成功但文件已被删”的悬空状态
# --------------------------------------------------------------------- #
def test_register_after_reclaim_fails_cleanly(dirs):
    mount, work = dirs
    seg = _write_shared(mount, [1, 2])

    svc = MergeService(mount, work)
    svc.register_job(_shared_job("A", page_size=10))
    svc.run_to_end("A")
    assert not seg.exists()

    # 段已被最后一个引用方回收，新登记同一段应明确失败而非留下坏作业
    with pytest.raises(Exception, match="不存在"):
        svc.register_job(_shared_job("late"))


# --------------------------------------------------------------------- #
# 5. 崩溃在 unlink 之后、reclaimed 提交前：重启按各作业进度补做回收
# --------------------------------------------------------------------- #
def test_restart_reconciles_shared_reclaim_after_crash(dirs):
    mount, work = dirs
    seg = _write_shared(mount, list(range(1, 5)))  # 4 行

    def hook(point: str) -> None:
        if point == "after_reclaim_unlink":
            raise CrashSimulated(point)

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(_shared_job("A", page_size=10))
    svc.register_job(_shared_job("B", page_size=10))

    # A 发完：B 仍未耗尽，扫描结果决定不删 —— 不触发 unlink 钩子
    r_a = svc.run_to_end("A")
    assert r_a.finished and seg.exists()

    # B 发完成为最后引用方：unlink 生效后崩溃，reclaimed 尚未提交
    with pytest.raises(CrashSimulated):
        MergeService(mount, work, crash_hook=hook).advance("B")
    assert not seg.exists()
    mid = MergeService(mount, work).get_status("B")
    seg_b = mid.segments[0]
    assert seg_b.exhausted and seg_b.reclaimed is False

    # 重启对账：文件已不在，补标 reclaimed；已发布页保持原样。
    # 对账在打开作业（advance/run）时惰性发生，get_status 只读状态。
    page1_before = (work / "B" / "page-00001.jsonl").read_bytes()
    svc2 = MergeService(mount, work)
    rb = svc2.advance("B")  # 已结束：触发 _open_job 对账，无新页
    assert rb.page_no is None and rb.finished
    assert svc2.get_status("B").segments[0].reclaimed is True
    assert (work / "B" / "page-00001.jsonl").read_bytes() == page1_before

    # A 在自己的下次打开时同样完成对账，且不影响任何已发布页
    svc2.advance("A")
    status_a = svc2.get_status("A")
    assert status_a.segments[0].exhausted and status_a.segments[0].reclaimed
    got_a = [r["key"] for r in _all_published(work, "A", status_a.published_pages)]
    assert got_a == [1, 2, 3, 4]


# --------------------------------------------------------------------- #
# 6. 崩溃在“耗尽状态提交后、回收扫描前”：重启后段保留，B 可续跑
# --------------------------------------------------------------------- #
def test_restart_keeps_segment_when_other_job_unfinished(dirs):
    mount, work = dirs
    seg = _write_shared(mount, list(range(1, 5)))

    def hook(point: str) -> None:
        if point == "before_reclaim_scan":
            raise CrashSimulated(point)

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(_shared_job("A", page_size=10))
    svc.register_job(_shared_job("B", page_size=10))

    with pytest.raises(CrashSimulated):
        svc.advance("A")
    # 阶段1 已提交：A exhausted=True/reclaimed=False；扫描未发生，段保留
    assert seg.exists()
    mid = MergeService(mount, work).get_status("A")
    assert mid.segments[0].exhausted and mid.segments[0].reclaimed is False

    # 新进程：A 对账时发现 B 未耗尽 -> 不回收；B 正常跑完并收尾
    svc2 = MergeService(mount, work)
    svc2.advance("A")  # 触发 A 的打开对账：B 未耗尽，保持 reclaimed=False
    assert svc2.get_status("A").segments[0].reclaimed is False
    result_b = svc2.run_to_end("B")
    assert result_b.finished
    got = [r["key"] for r in _all_published(work, "B", result_b.status.published_pages)]
    assert got == [1, 2, 3, 4]
    assert not seg.exists()
    # B 回收后 A 再打开，也补上 reclaimed 标记
    svc2.advance("A")
    assert svc2.get_status("A").segments[0].reclaimed is True


# --------------------------------------------------------------------- #
# 7. 三个作业共享同一段：前两个发完都保留，第三个回收
# --------------------------------------------------------------------- #
def test_three_jobs_share_one_segment(dirs):
    mount, work = dirs
    seg = _write_shared(mount, [7, 8, 9])

    svc = MergeService(mount, work)
    for jid in ("j1", "j2", "j3"):
        svc.register_job(_shared_job(jid, page_size=2))

    svc.run_to_end("j1")
    assert seg.exists()
    svc.run_to_end("j2")
    assert seg.exists(), "j3 还没发完"
    r3 = svc.run_to_end("j3")
    assert r3.finished and not seg.exists()
    for jid in ("j1", "j2", "j3"):
        got = [r["key"] for r in _all_published(work, jid, svc.get_status(jid).published_pages)]
        assert got == [7, 8, 9]


# --------------------------------------------------------------------- #
# 8. 部分共享：一个作业的独有段正常回收，共享段按引用计数保留
# --------------------------------------------------------------------- #
def test_private_and_shared_segments_reclaim_independently(dirs):
    mount, work = dirs
    shared = _write_shared(mount, [1, 2, 3], rel="shared.jsonl")
    private = write_segment(mount, "private.jsonl", [4, 5, 6])

    svc = MergeService(mount, work)
    svc.register_job(JobCreate(
        job_id="A", key_field="key", page_size=2,
        segments=[SegmentSpec(path="shared.jsonl", order=0)],
    ))
    svc.register_job(JobCreate(
        job_id="B", key_field="key", page_size=2,
        segments=[
            SegmentSpec(path="shared.jsonl", order=0),
            SegmentSpec(path="private.jsonl", order=1),
        ],
    ))

    # 跑完 A：共享段被 B 引用而保留
    svc.run_to_end("A")
    assert shared.exists()

    # 跑完 B：两段都应回收（共享段 B 是最后引用方）
    svc.run_to_end("B")
    assert not shared.exists()
    assert not private.exists()
    status_b = svc.get_status("B")
    assert all(s.reclaimed for s in status_b.segments)
    got = [r["key"] for r in _all_published(work, "B", status_b.published_pages)]
    assert got == [1, 2, 3, 4, 5, 6]


# --------------------------------------------------------------------- #
# 9. 服务实例重启（新进程语义）后引用关系仍以磁盘状态为准
# --------------------------------------------------------------------- #
def test_reference_counting_survives_service_restart(dirs):
    mount, work = dirs
    seg = _write_shared(mount, list(range(1, 6)))

    svc = MergeService(mount, work)
    svc.register_job(_shared_job("A", page_size=2))
    svc.register_job(_shared_job("B", page_size=2))
    svc.run_to_end("A")
    assert seg.exists()

    # 换新实例模拟进程重启：A 已耗尽、B 未耗尽，段必须还在且 B 可续跑
    svc2 = MergeService(mount, work)
    result_b = svc2.run_to_end("B")
    assert result_b.finished
    got = [r["key"] for r in _all_published(work, "B", result_b.status.published_pages)]
    assert got == [1, 2, 3, 4, 5]
    assert not seg.exists()
    svc2.advance("A")  # 打开 A 触发对账，补上 reclaimed 标记
    assert svc2.get_status("A").segments[0].reclaimed is True


# --------------------------------------------------------------------- #
# 10. 共享段先耗尽、本作业其他段跨多页续跑：回收发生在耗尽当页，
#     不要求等整个作业结束
# --------------------------------------------------------------------- #
def test_reclaim_happens_on_exhaustion_page_not_job_end(dirs):
    mount, work = dirs
    shared = _write_shared(mount, [1], rel="shared.jsonl")  # 仅 1 行，第一页即耗尽
    write_segment(mount, "long.jsonl", list(range(100, 110)))  # 10 行，跨多页

    svc = MergeService(mount, work)
    svc.register_job(_shared_job("A", page_size=5))  # A 只用共享段
    svc.register_job(JobCreate(
        job_id="B", key_field="key", page_size=2,
        segments=[
            SegmentSpec(path="shared.jsonl", order=0),
            SegmentSpec(path="long.jsonl", order=1),
        ],
    ))

    svc.run_to_end("A")
    assert shared.exists(), "B 还没耗尽共享段"

    # B 第一页：共享段唯一一行被发布并耗尽；B 成为最后引用方，立即回收
    r1 = svc.advance("B")
    assert r1.page_no == 1
    assert not shared.exists()
    assert (mount / "long.jsonl").exists(), "long.jsonl 还没跑完"
    assert "shared.jsonl" in r1.reclaimed

    # 后续 advance 不再重复处理该段，B 正常跑完
    rest = svc.run_to_end("B")
    assert rest.finished
    assert not (mount / "long.jsonl").exists()
    status_b = svc.get_status("B")
    seg_states = {s.path: s for s in status_b.segments}
    assert seg_states["shared.jsonl"].reclaimed
    assert seg_states["long.jsonl"].reclaimed


# --------------------------------------------------------------------- #
# 11. 崩溃在最后引用方 unlink 之后：文件已缺失，重启仅补标记不报错
# --------------------------------------------------------------------- #
def test_restart_marks_reclaimed_when_file_already_gone(dirs):
    mount, work = dirs
    seg = _write_shared(mount, [1, 2])

    def hook(point: str) -> None:
        if point == "after_reclaim_unlink":
            raise CrashSimulated(point)

    svc = MergeService(mount, work, crash_hook=hook)
    svc.register_job(_shared_job("solo", page_size=10))
    with pytest.raises(CrashSimulated):
        svc.advance("solo")
    assert not seg.exists()
    mid = MergeService(mount, work).get_status("solo")
    assert mid.segments[0].exhausted and not mid.segments[0].reclaimed

    # 重启：文件已不存在，对账直接补标 reclaimed，不抛“段缺失”错误
    svc2 = MergeService(mount, work)
    r = svc2.advance("solo")  # 已结束，触发对账
    assert r.finished and r.page_no is None
    assert svc2.get_status("solo").segments[0].reclaimed is True
    got = [r2["key"] for r2 in _all_published(work, "solo", mid.published_pages)]
    assert got == [1, 2]
