# 离线归并发布服务（FastAPI）

离线排序最后一道归并：挂载目录里的多路 JSONL 段（每路已按整数键排序），
按路序做 k 路归并，逐页发布结果；每路任意时刻只保留一条待比较记录；
同键按 **路序 → 路内原行序** 输出；每页发布后回收记录已全部发布的段文件
（删除前核对登记时固化的文件身份，同路径出现下一批同名段时保留不删）；
进程中断后已发布页保持原样，重启续跑、补做回收，整批 **不漏不重**。

## 目录结构

```
app/
  config.py            环境变量配置（MERGE_MOUNT_DIR / MERGE_WORK_DIR / MERGE_CRASH_HOOK）
  models.py            请求/响应模型、文件身份 FileIdentity 与登记校验
  engine.py            归并引擎：堆归并、原子发布、两阶段回收、身份围栏、崩溃对账
  main.py              FastAPI 应用与 HTTP 端点
sample/
  input/segments/      3 路本地样例段（含跨段同键 key=5/9、路内同键 key=12）
  job.demo.json        样例登记请求体（page_size=4）
scripts/
  generate_sample.py   生成样例
  smoke_e2e.py         真实 uvicorn 进程端到端冒烟（含 kill -9 中断重启）
  smoke_reclaim_e2e.py 两批同名段回收冒烟（正常复用 + unlink 崩溃窗口续跑）
tests/                 29 项 pytest（引擎/API/边界/崩溃注入/回收围栏）
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
| GET  | `/jobs` / `/jobs/{id}` | 作业列表 / 状态（偏移、耗尽、回收/保留标记、warnings、页计数） |
| POST | `/jobs/{id}/advance` | 推进一轮：发布下一页并回收耗尽段；已结束时 `page_no=null`；`deferred`/`warnings` 给出保留未删的段 |
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

### 3. 回收的两阶段提交 + 文件身份围栏

段记录全部随页发布（`exhausted`）后的回收顺序：

1. 先原子提交状态（`exhausted=True`、偏移落盘，`reclaimed=False`）；
2. **核对同路径当前文件与登记时捕获的 `FileIdentity` 一致后** 才 `unlink`
   段文件；
3. 再提交 `reclaimed=True`。

登记作业时为每个段固化一份文件身份：`stat` 四元组
（`dev/inode/size/mtime_ns`）加整文件 `SHA-256`（摘要在排序校验的同一次
流式扫描中算出，不额外读盘）。这样上游在原路径放入下一批同名段时——
**即使两份文件字节完全相同**——inode/mtime 不同会在 stat 阶段被拦下；
万一 inode 被复用且 stat 恰好吻合，SHA-256 兜底。身份核对失败时：

- **保留文件**，绝不删除；段置 `reclaim_deferred=True`（决定持久化、
  永久有效，后续重启不再回头处理该段）；
- 在作业状态 `warnings` 与服务日志中报出段的相对路径与容器内绝对路径；
- 推进结果的 `deferred` 字段列出本次保留的段，`reclaimed` 只包含真正
  删除的段。新作业可正常登记同路径段并完整读完自己那一批。

若崩在 2、3 之间，重启对账发现 `exhausted && !reclaimed` 会走同一套
围栏补做：是本批才补删补标，非本批一律保留。已标 `reclaimed` 但文件
又出现（上游重新投放）同样先核身份再决定，不会盲删。

**旧版状态兼容**：升级前写入的 `state.json` 没有 `file_id`。对账时
无法确认同路径文件属于哪一批，按安全侧处理——保留文件、置
`reclaim_deferred` 并在 `warnings` 报出路径，等待人工核对。

### 4. 消费期间的替换防护

作业仍在消费（未 `exhausted`）的段若同路径文件被替换，推进前的
stat 身份核对会直接失败并返回 400，文件原样保留，避免在错误的文件
上 `seek` 续跑造成漏读/重读。

### 5. 续跑

状态只持久化各路 **下一条待比较记录的起始字节偏移**。注意堆中持有的
记录是预读出来的，存活路必须存堆条目里的 `entry.offset` 而不是
`f.tell()`，否则重启会跳过该记录（该 bug 由测试先行捕获并修复）。

## 自测

```bash
.venv/bin/python -m pytest -q                      # 29 项单元/API/崩溃注入/回收围栏测试
.venv/bin/python scripts/smoke_e2e.py             # 真实进程 + kill -9 端到端
.venv/bin/python scripts/smoke_reclaim_e2e.py     # 两批同名段：正常复用 + 中断续跑
```

覆盖内容：

- 跨段同键按路序、路内同键按原行序；分页只有末页可短；
- 堆规模不超过路数（每路一条待比较记录）；
- 段全部发布后才回收，未消费完的保留；
- 页 rename 前 / 后两种崩溃注入，重启后不漏不重；
- unlink 后、回收标记提交前崩溃，重启补做回收；
- 多次重启后已发布页字节不变；200 行数据逐页推进并穿插重启的全量校验；
- **回收围栏**：正常回收后 / unlink 崩溃窗口后，同路径出现下一批
  （字节完全相同）均保留新段；新作业登记同名段完整读完；stat 身份
  被伪造吻合时 SHA-256 兜底；消费期间替换报错保文件；旧版状态
  （无 `file_id`）保留文件并在 `warnings` 报路径；
- 空段、单段、页大小大于总行数、同键跨页、空行容忍、目录穿越与登记校验。

## 余项 / 约束

- 本环境未安装 Docker 且禁止 user namespace，镜像构建与
  `docker compose up` 未在本机实跑；Dockerfile/compose 未改动，
  同等流程（同样的环境变量、挂载布局与 HTTP 调用）已用真实 uvicorn
  进程验证，含 kill -9 重启与两批同名段的回收/续跑场景。
- 状态与页文件面向单实例设计；多副本挂载同一卷不在支持范围
  （作业内有线程锁，但没有跨进程分布式锁）。
- 段文件登记时做一次全量流式排序校验并计算 SHA-256，超大段的登记
  耗时与段大小线性相关（校验与摘要在同一次扫描完成）。
- 输出页保留输入行的原始 JSON 文本（规范化行尾），不重排字段。
- 触发 `reclaim_deferred` 的段需要人工介入：确认上游新批已被新作业
  登记消费后，可手动清理遗留文件；服务自身不会删除它。
