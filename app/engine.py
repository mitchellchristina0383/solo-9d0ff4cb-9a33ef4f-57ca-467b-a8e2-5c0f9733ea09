"""归并引擎：k 路归并 + 分页发布 + 段文件回收 + 断点续跑。

设计要点
--------
1. 内存占用：每一路任意时刻只保留一条待比较记录，用堆选最小
   ``(key, 路序 order, 路内行序 seq)``。整段文件不进内存，
   通过字节偏移 ``offset`` 顺序扫描。
2. 页是发布单元：先写 ``page-XXXXX.jsonl.tmp``，fsync 后原子
   ``os.replace`` 成正式页文件，再写新的 ``state.json``（同样
   tmp + fsync + replace）。任何时刻崩溃，已发布页都保持原样。
3. 回收（跨作业引用感知的两阶段提交）：同一段 JSONL 可被多个
   作业登记。某路全部记录随页发布后，先提交本作业的
   ``exhausted + 偏移`` 状态，再在工作目录级“登记表锁”内扫描
   **所有** 已登记作业：只要还有任一作业登记了该段且尚未耗尽，
   文件就必须保留；当前作业是最后一个耗尽它的作业时才 unlink
   （哪怕它还有别的段要归并），最后提交本作业的 ``reclaimed``
   标记。登记与回收互斥，保证登记成功的作业一定仍能读完该段。
   任何一步崩溃，重启时 ``_open_job`` 都按各作业已发布的进度
   重新做全局回收判定并补做；“未消费但段文件缺失”不可能由本
   服务自身的崩溃产生。
4. 续跑：状态只记录各路下一条待比较记录的字节偏移和已发布页数，
   进程重启后打开文件 ``seek(offset)`` 继续，未发布记录重新参与
   归并，整批不丢不重。
"""

from __future__ import annotations

import heapq
import json
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from .models import (
    AdvanceResult,
    JobCreate,
    JobStatus,
    PageMeta,
    RunResult,
    SegmentState,
)

STATE_FILENAME = "state.json"
PAGE_PREFIX = "page-"
PAGE_SUFFIX = ".jsonl"
TMP_SUFFIX = ".tmp"

# 崩溃注入点（测试用）：
#  after_page_tmp        页临时文件写完并 fsync、尚未 rename（正式页不存在）
#  after_page_rename     页已原子发布、state.json 尚未更新
#  after_state_commit    state.json 已原子提交（tmp+rename 均完成）
#  before_reclaim_scan   本作业耗尽状态已提交、全局回收扫描之前（可阻塞，
#                        用于制造“回收与登记同时发生”的竞态窗口）
#  after_reclaim_unlink  段文件已 unlink、reclaimed 标记尚未提交
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

    # 类级“登记表锁”表：key = 工作目录。同一工作目录内，作业登记
    # （校验 + state 落盘）与跨作业回收判定（扫描全部 state + unlink）
    # 互斥，保证登记成功的作业必然被回收扫描计数，段文件不会被提前删。
    _registry_locks: dict[str, threading.Lock] = {}
    _registry_locks_guard = threading.Lock()

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

    def _get_registry_lock(self) -> threading.Lock:
        """工作目录级登记表锁：串行化“作业登记”与“跨作业回收判定”。"""
        key = str(self.work_dir)
        with self._registry_locks_guard:
            lock = self._registry_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._registry_locks[key] = lock
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

            # 登记表锁：从段校验到 state 落盘与回收判定互斥。
            # 回收方只有在本状态提交后才能扫描到本作业，因此登记
            # 成功的作业一定仍能读到它登记的每一个段文件。
            with self._get_registry_lock():
                # 双重检查：持锁后再确认一次，避免锁外判断后被并发抢先登记
                if state_path.exists():
                    existing = self._read_state(req.job_id)
                    same = (
                        existing.key_field == req.key_field
                        and existing.page_size == req.page_size
                        and [s.path for s in existing.segments] == [s.path for s in req.segments]
                        and [s.order for s in existing.segments] == [s.order for s in req.segments]
                    )
                    if same:
                        return existing
                    raise EngineError(f"作业 {req.job_id} 已存在且登记参数不一致")

                # 校验段文件：存在、可读、键为整数且全局非降序
                seg_states: list[SegmentState] = []
                for seg in req.segments:
                    p = self._segment_path(seg.path)
                    if not p.is_file():
                        raise EngineError(f"段文件不存在: {seg.path}")
                    self._validate_sorted_segment(p, req.key_field, seg.path)
                    seg_states.append(SegmentState(path=seg.path, order=seg.order))

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

    def _validate_sorted_segment(self, path: Path, key_field: str, rel: str) -> None:
        """流式校验：每行是 JSON 对象，键为整数，且键非降序。"""
        prev: int | None = None
        with open(path, "rb") as f:
            for lineno, raw in enumerate(f, 1):
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

    # ------------------------------------------------------------------ #
    # 启动恢复
    # ------------------------------------------------------------------ #
    def _open_job(self, job_id: str) -> JobStatus:
        """载入作业并做崩溃对账，返回恢复后的状态。"""
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

        # 3) 段文件回收对账（跨作业引用感知）：
        #    段可能被多个作业登记，不能仅凭本作业 exhausted 就删。
        #    按各作业已发布的进度重新做全局判定：所有引用方都耗尽
        #    才允许 unlink，并把本作业对应段标记为 reclaimed；仍有
        #    其他作业未耗尽则保留文件，本作业仅停在 exhausted 状态。
        #    已发布页不在此过程中改动，保持原样。
        changed = False
        with self._get_registry_lock():
            for seg in status.segments:
                if not seg.exhausted:
                    continue
                if not seg.reclaimed:
                    # 两阶段回收阶段2/3 之间崩溃，或当时还有其他作业未耗尽
                    if self._reclaim_if_unreferenced(seg.path):
                        seg.reclaimed = True
                        changed = True
                elif self._segment_path(seg.path).exists():
                    # 已标记回收但文件残留（unlink 落盘丢失/外部恢复）：
                    # 仍做全局判定，确认没有未耗尽的引用方才补删
                    self._reclaim_if_unreferenced(seg.path)
        if changed:
            self._write_state(job_id, status)

        return status

    # ------------------------------------------------------------------ #
    # 推进一页
    # ------------------------------------------------------------------ #
    def advance(self, job_id: str) -> AdvanceResult:
        with self._get_lock(job_id):
            return self._advance_locked(job_id)

    def _advance_locked(self, job_id: str) -> AdvanceResult:
        status = self._open_job(job_id)
        if status.finished:
            return AdvanceResult(
                page_no=None,
                page_size=status.page_size,
                rows=0,
                finished=True,
                reclaimed=[],
                status=status,
            )

        key_field = status.key_field
        page_size = status.page_size
        readers: dict[int, _RunReader] = {}
        heap: list[_HeapEntry] = []

        # 打开每一路，seek 到持久化偏移，载入第一条待比较记录
        for idx, seg in enumerate(status.segments):
            if seg.exhausted:
                continue
            p = self._segment_path(seg.path)
            if not p.exists():
                raise EngineError(
                    f"段文件 {seg.path} 已不存在但作业尚未消费完，无法续跑"
                )
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
                reclaimed = self._commit_and_reclaim(job_id, status)
                return AdvanceResult(
                    page_no=None,
                    page_size=page_size,
                    rows=0,
                    finished=True,
                    reclaimed=reclaimed,
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

            # 4) 先提交“耗尽 + 偏移”状态，再回收段文件，最后补提交
            #    reclaimed 标记。这样“未消费但段文件缺失”不可能由本
            #    服务自身的崩溃造成；unlink 后崩溃由启动对账补做回收。
            reclaimed_now = self._commit_and_reclaim(job_id, status)

            return AdvanceResult(
                page_no=page_no,
                page_size=page_size,
                rows=len(page_records),
                finished=status.finished,
                reclaimed=reclaimed_now,
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

    # ------------------------------------------------------------------ #
    # 跨作业回收判定
    # ------------------------------------------------------------------ #
    def _iter_job_states(self) -> Iterable[JobStatus]:
        """枚举工作目录下所有已登记作业的持久化状态。

        回收判定以磁盘上的 state.json 为准：登记事务在 state 落盘后
        才对回收方可见，未登记完的作业不计入引用，也不会读到已删段。
        """
        if not self.work_dir.exists():
            return
        for child in sorted(self.work_dir.iterdir()):
            state_path = child / STATE_FILENAME
            if not state_path.exists():
                continue
            # 状态损坏时直接报错：宁可不回收，也不能在引用关系不明时删段
            yield self._read_state(child.name)

    def _all_referrers_exhausted(self, seg_path: str) -> bool:
        """所有登记了该段的作业是否都已把它耗尽（按已发布进度判断）。"""
        for other in self._iter_job_states():
            for other_seg in other.segments:
                if other_seg.path == seg_path and not other_seg.exhausted:
                    return False
        return True

    def _reclaim_if_unreferenced(self, seg_path: str) -> bool:
        """跨作业回收判定，返回该段对当前作业而言是否已完成回收。

        必须在登记表锁内调用：与作业登记互斥，扫描结果与 unlink
        之间不会出现新的引用方。

        - 仍有任一已登记作业未耗尽该段：保留文件，返回 False；
        - 当前作业是最后一个耗尽它的作业：unlink 并返回 True
          （即使当前作业还有别的段要归并）；
        - 文件已不存在（如崩在 unlink 之后、标记提交之前）：
          同样视为回收完成，返回 True。
        """
        if not self._all_referrers_exhausted(seg_path):
            return False
        p = self._segment_path(seg_path)
        if p.exists():
            p.unlink()
            _fsync_dir(p.parent)
        return True

    def _commit_and_reclaim(self, job_id: str, status: JobStatus) -> list[str]:
        """跨作业引用感知的两阶段回收，保证崩溃后可对账：

        阶段1  先原子提交本作业状态（段已 exhausted、偏移已落盘，
               reclaimed=False）。此后本作业不会再去读这些段；同时
               该状态对回收扫描可见，本作业计入“已耗尽引用方”。
        阶段2  在登记表锁内逐段做全局判定：只有所有登记该段的作业
               都耗尽时才 unlink（最后一个耗尽者负责回收，哪怕它
               还有别的段要跑）；仍有其他作业未耗尽则保留文件。
               若在 unlink 后、阶段3 前崩溃，重启时 ``_open_job``
               会按各作业已发布进度重新判定并补做回收。
        阶段3  再次提交本作业状态（被实际回收的段 reclaimed=True）。
        """
        # 阶段1：提交本作业的耗尽/偏移
        self._write_state(job_id, status)

        # 竞态注入点：耗尽状态已提交、登记表锁尚未获取。此时阻塞
        # 不会挡住其他作业登记；登记方先落盘后，下面的全局扫描
        # 必然把它计入引用方。
        if self._crash_hook is not None:
            self._crash_hook("before_reclaim_scan")

        # 阶段2：全局判定后回收段文件
        reclaimed: list[str] = []
        with self._get_registry_lock():
            for seg in status.segments:
                if seg.exhausted and not seg.reclaimed:
                    if self._reclaim_if_unreferenced(seg.path):
                        reclaimed.append(seg.path)
        if self._crash_hook is not None and reclaimed:
            self._crash_hook("after_reclaim_unlink")

        # 阶段3：提交本作业的回收标记
        for path in reclaimed:
            for seg in status.segments:
                if seg.path == path:
                    seg.reclaimed = True
        if reclaimed:
            self._write_state(job_id, status)
        return reclaimed

    # ------------------------------------------------------------------ #
    # 整批跑完
    # ------------------------------------------------------------------ #
    def run_to_end(self, job_id: str, max_pages: int | None = None) -> RunResult:
        """连续推进到作业结束（或达到 max_pages）。

        与反复调用 :meth:`advance` 等价，但只获取一次作业锁，
        因为 threading.Lock 不可重入。
        """
        published: list[int] = []
        with self._get_lock(job_id):
            while True:
                result = self._advance_locked(job_id)
                if result.page_no is not None:
                    published.append(result.page_no)
                if result.finished or (max_pages is not None and len(published) >= max_pages):
                    return RunResult(
                        published=published,
                        finished=result.finished,
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
