# 离线归并发布服务（FastAPI）

离线排序最后一道归并：挂载目录里的多路 JSONL 段（每路已按整数键排序），
按路序做 k 路归并，逐页发布结果；每路任意时刻只保留一条待比较记录；
同键按 **路序 → 路内原行序** 输出；每页发布后回收记录已全部发布的段文件。
**同一段可被多个作业登记：只要还有任一已登记作业没发完该段，文件就保留；
最后一个发完该段的作业负责回收（即使它还有别的段要归并）。**
进程中断后已发布页保持原样，重启续跑、按各作业已发布进度补做回收，整批
**不漏不重**。

## 目录结构

```
app/
  config.py            环境变量配置（MERGE_MOUNT_DIR / MERGE_WORK_DIR）
  models.py            请求/响应模型与登记校验
  engine.py            归并引擎：堆归并、原子发布、两阶段回收、崩溃对账
  main.py              FastAPI 应用与 HTTP 端点
sample/
  input/segments/      3 路本地样例段（含跨段同键 key=5/9、路内同键 key=12）
  job.demo.json        样例登记请求体（page_size=4）
  job.demo-shared.json 第二个作业，与 demo 登记完全相同的三个段（演示共享回收）
scripts/
  generate_sample.py   生成样例
  smoke_e2e.py         真实 uvicorn 进程端到端冒烟（含 kill -9 中断重启）
  smoke_shared.py      两个作业共用段的端到端冒烟（保留/回收/中断补回收）
tests/                 34 项 pytest（引擎/API/边界/崩溃注入/共享段回收）
Dockerfile
docker-compose.yml
requirements.txt
```

## 快速开始（Docker Compose）

```bash
# 1. 准备样例段（仓库已自带，可重新生成）
python scripts/generate_sample.py

# 2. 启动（./sample/input 挂为输入目录，命名卷保存状态与已发布页）
docker compose up --build -d

# 3. 打开交互文档
#    http://127.0.0.1:8000/docs

# 4. 登记作业（可直接贴 sample/job.demo.json 的内容）
curl -X POST http://127.0.0.1:8000/jobs \
  -H 'Content-Type: application/json' \
  --data @sample/job.demo.json

# 5. 每次推进发布一页；发布后自动回收已消费完的段文件
curl -X POST http://127.0.0.1:8000/jobs/demo/advance

# 或一次跑完
curl -X POST http://127.0.0.1:8000/jobs/demo/run

# 6. 单页读取（NDJSON 流式；/json 后缀返回 JSON 数组）
curl http://127.0.0.1:8000/jobs/demo/pages/1
curl http://127.0.0.1:8000/jobs/demo/pages/1/json
```

段文件被回收后，`sample/input/segments/` 下对应文件会消失；重新运行
`python scripts/generate_sample.py` 可还原样例。作业状态与页文件在
`merge-work` 命名卷内，`docker compose down`（不加 `-v`）后重新 `up`
即可续跑。

## 在 /docs 演示两个作业共用输入段

同一份 JSONL 段允许被多个作业登记，回收按跨作业引用计数决定。仓库自带
两份请求体：`sample/job.demo.json`（作业 `demo`，page_size=4）与
`sample/job.demo-shared.json`（作业 `demo-shared`，page_size=3），二者
登记的是同样三个段。

```bash
# 1. 启动服务并打开 http://127.0.0.1:8000/docs
docker compose up --build -d

# 2. 在 /docs（或 curl）登记两个作业
curl -X POST http://127.0.0.1:8000/jobs -H 'Content-Type: application/json' \
  --data @sample/job.demo.json
curl -X POST http://127.0.0.1:8000/jobs -H 'Content-Type: application/json' \
  --data @sample/job.demo-shared.json

# 3. 先把 demo 整批发完：三个段被 demo-shared 共享，文件不会被删
curl -X POST http://127.0.0.1:8000/jobs/demo/run
ls sample/input/segments/          # 三个段文件仍在

# 4. demo-shared 跑完后才真正回收（它是最后一个发完的作业）
curl -X POST http://127.0.0.1:8000/jobs/demo-shared/run
ls sample/input/segments/          # 目录已空
```

在 `/docs` 页面也可以直接展开 `POST /jobs`，分别贴入两份 JSON 后
"Try it out"，再用 `POST /jobs/{id}/run`、`GET /jobs/{id}` 对照两个作业
的 `segments[].exhausted / reclaimed` 状态与 `/mount` 里段文件的可见性。
若段已被最后一个引用方回收，再登记同一段会得到明确的
`400 段文件不存在`，不会留下登记成功却无法续跑的坏作业。

## 不用容器的本地运行

```bash
python -m venv .venv && .venv/bin/python -m pip install -r requirements.txt
python scripts/generate_sample.py
MERGE_MOUNT_DIR=$PWD/sample/input MERGE_WORK_DIR=$PWD/sample/work \
  .venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

## HTTP 接口（/docs 可见完整 Schema）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET  | `/healthz` | 健康检查 |
| GET  | `/mount` | 列出挂载目录内的 JSONL，登记时照抄相对路径 |
| POST | `/jobs` | 登记作业：`job_id`、`key_field`、`page_size`、`segments[{path,order}]` |
| GET  | `/jobs` / `/jobs/{id}` | 作业列表 / 状态（偏移、耗尽、回收标记、页计数） |
| POST | `/jobs/{id}/advance` | 推进一轮：发布下一页并回收耗尽段；已结束时 `page_no=null` |
| POST | `/jobs/{id}/run?max_pages=` | 服务端连续推进到结束（可限页数） |
| GET  | `/jobs/{id}/pages` | 已发布页清单 |
| GET  | `/jobs/{id}/pages/{n}` | 单页读取，`application/x-ndjson` 流式输出 |
| GET  | `/jobs/{id}/pages/{n}/json` | 同页内容的 JSON 数组形式 |

分页规则：除末页外每页严格等于 `page_size`，只有末页可短。

## 实现要点

### 1. k 路归并，每路只留一条待比较记录

- 每路打开一个文件句柄并 `seek` 到状态中记录的字节偏移，先读一条进最小堆；
- 堆元素排序键为 `(整数键 key, 路序 order, 路内行序 seq)`，同键天然按
  路序、再按路内原行序输出；
- 每弹出一条，只从同一路补读一条进堆——任意时刻每路在堆中至多一条，
  整段文件不进内存（测试 `test_heap_never_holds_more_than_one_record_per_run`
  会监控堆规模）。

### 2. 页是发布单元，原子提交

- 页先写 `page-00001.jsonl.tmp`，`fsync` 文件与目录后 `os.replace`
  原子改名；状态 `state.json` 走同样的 tmp + fsync + replace；
- 已发布的正式页永远不会被改写。崩溃只可能落在三个窗口，
  启动时 `_open_job` 逐一对账：
  1. **页 rename 前**：正式页不存在，tmp 直接清理，该页重发；
  2. **页 rename 后、state 提交前**：孤儿页号不在 `1..published_pages`
     内，删除后按状态重发；
  3. **state 提交后**：状态即事实，从记录的偏移继续。

### 3. 回收：跨作业引用感知的两阶段提交

同一段 JSONL 可被多个作业登记。是否能删段，不再只看当前作业，而是扫描
工作目录下**所有**已登记作业的 `state.json`：只要还有任一作业登记了该段
且未 `exhausted`，文件就保留；当前作业是最后一个耗尽它的作业时才 `unlink`
——哪怕它自己还有别的段要归并。段记录全部随页发布（`exhausted`）后的
回收顺序：

1. 先原子提交本作业状态（`exhausted=True`、偏移落盘，`reclaimed=False`）；
2. 在工作目录级“登记表锁”内做全局判定，确认无未耗尽引用方才 `unlink`；
3. 再提交本作业的 `reclaimed=True`。

登记（段校验 + `state.json` 落盘）与回收判定（扫描 + `unlink`）在同一把
登记表锁下互斥：回收方只能扫描到已提交的登记，而登记事务在锁内完成，
因此**登记成功的作业一定仍能读到它登记的段**；若回收先于登记完成，
登记会因段文件缺失而明确失败，不会产生悬空作业。

这样“作业未消费完但段文件缺失”不可能由服务自身崩溃造成（出现即报
`EngineError`，通常意味着外部误删）。若崩在 2、3 之间，重启 `_open_job`
按各作业已发布进度重新做全局判定：所有引用方都耗尽才补删并补标
`reclaimed`，仍有未耗尽引用方则保留文件；已标 `reclaimed` 但文件残留时，
同样经全局判定确认安全后补删。

### 4. 续跑

状态只持久化各路 **下一条待比较记录的起始字节偏移**。注意堆中持有的
记录是预读出来的，存活路必须存堆条目里的 `entry.offset` 而不是
`f.tell()`，否则重启会跳过该记录（该 bug 由测试先行捕获并修复）。

## 自测

```bash
.venv/bin/python -m pytest -q                      # 34 项单元/API/崩溃注入/共享段测试
.venv/bin/python scripts/smoke_e2e.py              # 真实进程 + kill -9 端到端
.venv/bin/python scripts/smoke_shared.py           # 两作业共用段：保留/回收/中断补回收
```

覆盖内容：

- 跨段同键按路序、路内同键按原行序；分页只有末页可短；
- 堆规模不超过路数（每路一条待比较记录）；
- 段全部发布后才回收，未消费完的保留；
- **多作业共用段：先完成者不删、最后完成者回收（含他段仍在跑的情形）；
  登记与回收并发时登记成功即可读；三作业共享、独有段/共享段独立回收；**
- 页 rename 前 / 后两种崩溃注入，重启后不漏不重；
- unlink 后、回收标记提交前崩溃，重启按各作业进度补做回收，已发布页不变；
- 多次重启后已发布页字节不变；200 行数据逐页推进并穿插重启的全量校验；
- 空段、单段、页大小大于总行数、同键跨页、空行容忍、目录穿越与登记校验。

## 余项 / 约束

- 本环境未安装 Docker 运行时，镜像构建与 `docker compose up` 未在本机实跑；
  Dockerfile/compose 仅做常规 uvicorn 封装且本次未改动，等价流程已用
  venv + 真实 uvicorn 子进程（含 kill -9 重启、双作业共享段）验证。
- 状态与页文件面向单实例设计；多副本挂载同一卷不在支持范围
  （作业内有线程锁、工作目录内有登记表锁，但没有跨进程分布式锁）。
- 回收判定需扫描工作目录下所有作业的 `state.json`，开销与作业数线性相关；
  状态文件损坏时对应作业会令回收报错而不是贸然删段。
- 段文件登记时做一次全量流式排序校验，超大段的登记耗时与段大小线性相关。
- 输出页保留输入行的原始 JSON 文本（规范化行尾），不重排字段。
