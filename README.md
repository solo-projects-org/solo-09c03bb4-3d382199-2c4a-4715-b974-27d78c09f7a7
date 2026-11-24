# 正射影像批量下架服务（Orthophoto Take-down Service）

同一测绘期冗余正射影像的批量下架接口。核心约束：**下架后下游取图不能少覆盖**。
每张被申请下架的影像，都必须由本批下架后仍在架、米/像素不大于它（即同等或更精细）
的影像**完整覆盖**，覆盖可以由多张影像拼合而成；任一条件不满足，整批不生效。

技术栈：FastAPI + SQLite（标准库 `sqlite3`）+ Pydantic v2，Docker Compose 启动，
Swagger UI（`/docs`）可直接试下架和查询在架目录。

## 业务规则

1. 提交一批片号（`POST /withdrawals`），片号去重后逐张校验。
2. 任一片号不在目录中或已下架 → 整批拒绝（HTTP 409），返回该片号。
3. 下架后每张申请片号的覆盖矩形，必须能被**其余在架影像**（不含本批其他片号）
   中分辨率 `米/像素 ≤ 该片号` 的影像并集完整覆盖；允许合用多张。
4. 存在面积缺口 → 整批拒绝，返回**失败片号 + 一处未覆盖矩形**
   （同一平面坐标系下的正面积轴对齐矩形）。
5. 全部通过才在同一事务内把这批片号置为下架；拒绝时目录数据零变更。
6. 只改供数目录（SQLite 的 `is_active` 标记），**不删除、不移动任何影像文件**；
   JSON 源目录文件只读。

## 目录结构

```
app/
  config.py      环境变量配置（DB/JSON 路径、忙等待、网格上限）
  geometry.py    精确矩形覆盖判定（坐标压缩网格，无栅格化误差）
  database.py    SQLite 初始化、查询、BEGIN IMMEDIATE 串行化下架事务
  schemas.py     Pydantic 请求/响应模型（OpenAPI）
  main.py        FastAPI 路由
data/
  catalog.json   演示目录（EPSG:32650，同一测绘期 2024Q3，17 张，16 个场景）
tests/           54 项 pytest（几何单测 / API 规则 / 多线程并发 / 重建对账）
Dockerfile, docker-compose.yml, requirements*.txt, pytest.ini
```

## 数据模型

JSON 目录（`data/catalog.json`）字段：

| 字段 | 含义 |
| --- | --- |
| `crs` / `units` | 全部矩形共用的平面坐标系与单位（演示为 EPSG:32650 / 米） |
| `images[].sheet_no` | 片号，目录内唯一 |
| `images[].period` | 测绘期 |
| `images[].bbox` | 有效覆盖矩形 `[min_x, min_y, max_x, max_y]` |
| `images[].resolution_mpp` | 分辨率，米/像素，越小越精细 |
| `images[].file_path` | 影像文件路径，仅作元数据保存，服务永不访问该文件 |

SQLite 表：`images`（片号、测绘期、矩形四角、分辨率、文件路径、`is_active`）、
`withdrawal_batches`（每次提交的提交/拒绝审计流水，含失败片号与缺口矩形）、
`catalog_meta`（坐标系等元数据与初始化标记）。

## 覆盖判定算法（精确，无误差）

轴对齐矩形求并集覆盖采用**坐标压缩**：取目标矩形与所有候选覆盖矩形的 x/y 边
投影成网格，网格内任一单元格要么被某矩形整体包含、要么完全不被包含。逐格用
中心点判定，找到第一个未覆盖格即返回该格矩形作为"一处未覆盖矩形"；零宽/零高
格跳过，共边不会产生伪缺口。另有 `IMAGERY_MAX_GRID_CELLS`（默认 200 万）兜底
防止病态目录造成过大网格。

## 并发正确性

- 每请求独立 SQLite 连接（不跨线程共享）。
- 写事务一律 `BEGIN IMMEDIATE`：进入事务即拿写锁，覆盖复核与状态更新在同一个
  串行化事务内完成，因此"本批生效时仍满足覆盖条件"——其他批次可能先于本批提交，
  但本批提交前看到的一定是最新在架状态。
- 锁冲突走 `busy_timeout` + 指数退避重试（默认 5 次），耗尽返回 503。
- 拒绝批次的审计记录用独立事务写入，不随校验事务回滚而丢失。
- 强制重建的比对在写锁之外，切库时用 `data_version` 栅栏确认比对后无他人提交，
  否则基于新状态重新比对；详见上文「重建（force=true）的语义」。

## 运行

### Docker Compose（要求环境有 Docker）

```bash
docker compose up --build
# Swagger:  http://localhost:8000/docs
# 健康检查: http://localhost:8000/health
```

`./data` 挂载到容器 `/data`：JSON 只读，SQLite 库 `imagery.db` 在该目录内生成。
换了 JSON 源目录后，在 Swagger 调 `POST /admin/reinitialize?force=true` 发布：
按当前 JSON **对账** SQLite，而不是整表重写（不带 `force` 时已初始化的库保持不变，幂等）。

### 重建（force=true）的语义

- **最小写入**：与 JSON 完全一致且仍在架的图幅不重写（行 `id`、`is_active`
  原样保留）；只写差异——新增、属性变更（测绘期/四角/分辨率/文件路径）、
  被下架需恢复在架的（`is_active=1`）、以及 JSON 中已不存在的片号（删除）。
  全部一致时零图像写入。
- **比对不占写锁**：整表比对在 `BEGIN IMMEDIATE` 之前用普通读完成，期间其他
  重建和下架都能正常提交（自动提交模式下逐条 SELECT，不持读锁）。
- **快照栅栏**：开始写事务时用 `PRAGMA data_version` 检测比对之后是否有人先提交；
  若有则丢弃旧差异、按新状态重新比对再写。JSON 只在请求开始时读一次，因此
  无论重试多少轮，最终结果都严格对应**本次读取的 JSON**：源中没有的片号删除，
  源中的图幅全部在架。
- **元数据随图幅更新**：`crs` / `units` 与图幅在同一次提交里更新。
- **流水清空**：与旧行为一致，强制重建会清空 `withdrawal_batches`（本就是空表时
  不产生写）。
- **失败零变更**：JSON 校验失败（400）或写入中途出错，整个事务回滚，目录与
  流水都保持调用前状态。等待锁/快照稳定超过重试预算返回 503。

响应示例（`changes` 为本次实际写入的行数，可直接核对没被重写的图幅）：

```json
{"initialized":true,"rebuilt":true,"changed":true,"image_count":17,
 "crs":"EPSG:32650",
 "changes":{"inserted":1,"updated":1,"reactivated":1,"deleted":1,
            "unchanged":14,"audit_cleared":2}}
```

### 本地直接运行（无 Docker 时）

```bash
python3 -m venv .venv && . .venv/bin/activate   # 或用 uv
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

路径可用环境变量覆盖：`IMAGERY_DB_PATH`、`IMAGERY_CATALOG_PATH`、
`IMAGERY_BUSY_TIMEOUT_MS`、`IMAGERY_MAX_GRID_CELLS`。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 服务状态、坐标系、在架/总数 |
| POST | `/withdrawals` | 批量下架（原子），body：`{"sheet_nos": ["ORTHO-001"]}` |
| GET | `/images?active=true` | 在架目录（`active=false` 查全目录含已下架） |
| GET | `/images/{sheet_no}` | 单片查询 |
| GET | `/withdrawals?limit=50` | 提交/拒绝审计流水 |
| POST | `/admin/reinitialize?force=false` | 从 JSON 初始化/重建 SQLite（force 时按最小差异对账） |

成功响应示例：

```json
{"batch_no":"WD-20261123-00001","status":"committed",
 "withdrawn_sheets":["ORTHO-001"],"withdrawn_count":1,
 "committed_at":"2026-11-23T16:18:09+00:00"}
```

拒绝响应示例（HTTP 409，缺口矩形为目标 footprint 内的正面积矩形）：

```json
{"detail":{"detail":"coverage gap after withdrawal","failed_sheet":"ORTHO-004",
 "uncovered_rect":{"min_x":503000.0,"min_y":3000000.0,
                   "max_x":503500.0,"max_y":3001000.0}}}
```

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

---

# 收尾自测报告

## 做成了什么

- FastAPI 批量下架服务：片号/测绘期/同一平面坐标系覆盖矩形（四角坐标）/
  米-像素分辨率全部入库 SQLite，库由 JSON 目录初始化（幂等，可 force 重建）。
- 完整实现"本批下架后仍在架、分辨率不大于目标、可多张拼合、完整覆盖"规则；
  整批原子生效/拒绝；拒绝时给出失败片号和一处正面积未覆盖矩形。
- 精确几何判定（坐标压缩网格），覆盖拼合、内部空洞、非对齐边缘、共边、
  分辨率更粗不算覆盖等情形均有对应测试与演示数据。
- `BEGIN IMMEDIATE` 串行化事务 + 忙等待重试，覆盖复核与提交同一事务，
  保证核算期间其他批次可生效、本批生效瞬间条件仍成立。
- 审计流水（含拒绝原因、失败片号、缺口矩形）；在架目录/全目录/单片查询；
  Swagger UI 与 OpenAPI schema；Dockerfile + docker-compose + 健康检查；
  全程只更新 `is_active`，不触碰影像文件，JSON 源目录哈希实测不变。
- **force 重建改为按最小差异对账**：一致的在架图幅不重写（保留行 `id`），
  只写新增/属性变更/恢复在架/删除；整表比对在写锁之外，`data_version`
  栅栏保证比对后被别人抢先提交时按新状态重算而结果仍对应本次读到的 JSON；
  `crs`/`units` 与图幅同一事务更新；流水照旧清空；校验或写入失败整体回滚。

## 实际验证过什么

- **54 项 pytest 全部通过**（`python -m pytest -q`，连跑 3 次稳定）：原 35 项
  几何/API/并发测试全部保持通过，新增 19 个重建用例（14 个函数，含 6 个参数化
  非法 JSON）——同步时零图像写入且行 `id` 不变、差异重建只产生 4 条预期写入
  （增/改/恢复/删）、仅流水非空时只清流水、仅元数据变化不碰图像、比对暂停期间
  下架可提交且事后被恢复在架、两笔重建交错时后完成者发布自己读到的 JSON
  （片号集合与 CRS 均对应）、他人持写锁时读比对不受阻且等锁后按新状态收敛、
  空操作重建不改变 `data_version`、6 类非法 JSON 均 400 且目录/流水逐行不变、
  写入中途异常整体回滚。
- 重建测试用 SQLite 触发器记录 `images` 的每一条 INSERT/UPDATE/DELETE，
  直接断言"哪些行被写了"，而不是只看最终状态。
- 真实 uvicorn 进程 + urllib 端到端：同 JSON 重建返回 `changed=false` 且行
  `id` 不变；下架后重建只 `reactivated=1, unchanged=16`；换源（删 1、改分辨率 1、
  增 1、改 CRS/单位）后重建计数与库内容/`/health` 全部一致、幂等重跑 `changed=false`。
- 真实 HTTP 交错压测：3 个下架线程 + 2 个重建线程并发 6 秒，无错误、
  无意外写入；最终重建后片号集合、分辨率、bbox、测绘期与 JSON 逐项相等，
  全部在架，`PRAGMA integrity_check` 为 `ok`。另以 3 下架 + 2 重建直连事务层
  压测 6 秒，同样零错误、零重复/缺失片号。
- `/docs` 返回 200，OpenAPI 中 `/admin/reinitialize` 的 200 响应引用
  `ReinitializeResponse`（含 `changes` 字段）；全部源码 `py_compile` 通过。

## 没完成 / 受限项

- **本环境没有 Docker 守护进程**，`docker compose up --build` 未实际执行；
  Compose/Dockerfile 仅做了语法与配置静态校验，容器内运行需在有 Docker 的
  机器上验证（流程与本地 uvicorn 一致，镜像基于 python:3.11-slim）。
- 覆盖模型限定为**轴对齐矩形**（题目所述"有效覆盖矩形"）；若后续目录出现
  旋转矩形或多边形 footprint，需要扩展几何模块（网格思路可复用，交集计算需替换）。
- 未做鉴权/多租户与分页（审计接口仅 `limit`）；当前定位为内网运维工具，
  如需开放应加认证与写操作权限控制。
- 未引入 SQL 迁移工具（Alembic 等）：表结构随初始化脚本创建，结构变更目前
  靠 `force` 重建；因下架只改标记位、源数据在 JSON，重建不丢业务状态语义，
  但正式环境仍建议补迁移方案。
- 重建的差异比对按图幅逐行读入内存（17 张演示规模无压力）；超大目录
  （数十万片）需要改为分块比对/临时表 diff，当前未实现。
- 未提交 git commit（按约定需明确指示再提交）；工作区改动均已落盘可直接审阅。
