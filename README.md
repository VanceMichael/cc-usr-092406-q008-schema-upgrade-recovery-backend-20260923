# q008 水产养殖服务

本项目是水产养殖管理后端，维护塘口、养殖批次、投苗、投喂、水质、用药、成本、销售与周期分析数据。业务数据保存在 SQLite 文件中，HTTP 接口由 FastAPI 提供。

## 测试命令

```bash
python3 -m unittest discover -s tests -v
```

迁移相关测试覆盖：全新库、沿用旧 SQLite 文件跨版本升级（v1→v2→v3）、从中间版本 v2 升级、故障注入（搬运中断续跑、切换后中断、校验失败自动回退、备份损坏转人工介入）、双进程单迁移者协调、未来版本拒绝降级、保留数据库后的重启，以及健康接口与流量闸门。

## 编译与构建命令

```bash
python3 -m compileall -q backend/app
```

## 依赖安装

```bash
pip install -r backend/requirements.txt
```

## 启动命令

```bash
cd backend
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

启动时不再无条件 `create_all`，而是执行**版本化结构迁移**：

* 空数据库：直接建立到当前版本，并登记全部版本；
* 沿用旧 SQLite 文件：先按结构指纹辨认版本（旧的无版本库必须与已知 v1 摘要逐列、逐索引一致才补登记），再沿版本链逐级升级；
* 结构已经是当前版本：幂等跳过。

启动后可访问 `/health` 检查服务状态。开发环境不得提交真实账号、连接凭据或生产数据。

## 结构版本与迁移机制

迁移代码位于 `backend/app/migrations/`。每个版本（`versions.py`）明确四要素：

1. **前置版本 `predecessor`**：升级链固定为 1→2→3，不允许跳级执行；开始每版前还会复核前置版本的结构摘要；
2. **校验摘要 `checksum`**：对规范化后的完整结构（列名/类型/NOT NULL/主键位、索引名/列序/唯一、外键）计算 SHA-256；升级前后都比对，结构漂移即拒绝；
3. **升级步骤 `steps`**：原生 DDL 或复制型重建；
4. **升级后不变量 `invariants`**：以“违规行数必须为 0”的 SQL 表达，外加每版前后**业务表行数不变**与外键检查。

版本历史：

| 版本 | 前置 | 变更 | 步骤类型 |
| --- | --- | --- | --- |
| v1 | — | 初版结构（旧 `create_all` 产物，无版本记账） | — |
| v2 | v1 | `ponds.location_code` 新增并按主键回填唯一编码（`P` + 8 位补零）；`feeding_records.feed_batch_number` 新增；投喂/成本各加 `(批次, 日期)` 复合索引 | 原生 `ALTER`/`CREATE INDEX` |
| v3 | v2 | `ponds.name` 收紧到 `VARCHAR(50) NOT NULL`、`location_code NOT NULL`；`feeding_records.feed_quantity` 由 `FLOAT` 改为 `NUMERIC(10,2)`；`cost_records.cost_type` 收紧到 `VARCHAR(30) NOT NULL` | 复制重建 |

记账表（下划线前缀，不参与业务结构指纹）：`_schema_migrations`（已提交版本与摘要）、`_migration_state`（中断续跑所需的版本/步骤/阶段/复制游标）、`_migration_row_counts`（行数不变量）。`PRAGMA user_version` 记录已提交版本号。

### SQLite 不支持的表变更：可回滚的复制与校验

SQLite 无法改列类型或收紧约束，v3 的每张表按四阶段执行：

1. `prepare`：建影子表 `__migrate_<table>`；
2. `copy`：按主键游标分批 `INSERT … SELECT`，游标随每个批次提交持久化；
3. `verify`：比对源表与影子表的**行数**与**逐行内容摘要**（数值列统一 CAST 到 REAL 比对，避免类型亲和造成的伪差异），并做外键检查；
4. `switch`：`DROP` 旧表、影子表改名、重建索引在**单事务内原子完成**。

每版开始前做在线物理备份 `<db>.pre-vN.bak`（含 `.sha256` 旁路校验）。升级成功并通过全部校验后删除备份。

### 中断后的恢复判定

* **继续**：搬运阶段中断时游标已落盘，重启后只搬运 `id > last_copied_id` 的行，**不重复搬运**；若中断发生在切换事务提交之后，引擎识别主表已是目标结构，丢弃残留影子表并跳过该步；
* **回退**：复制校验或外键检查失败、且物理备份完好并通过摘要校验时，自动把文件回退到前置版本，写入 `<db>.rolled-back` 标记；
* **人工介入**：备份缺失/损坏、影子表行数与记账矛盾、源表缺失、结构无法解释时，状态冻结为 `needs_manual` 并停止写库，跨重启保持；存在 `.rolled-back` 标记时也拒绝再次启动，需人工排查后删除标记。

### 单迁移者协调

对文件型 SQLite，在同目录侧车文件 `<db>.migrate.lock` 上使用 `fcntl` 排他锁（随进程死亡自动释放）。同一数据库只有一个迁移者：

* `MIGRATION_MODE=wait`（默认）：其他进程阻塞等待，拿到锁后复核结构，结构就绪即接流量；
* `MIGRATION_MODE=reject`：锁被占用时立即以“迁移进行中”失败，业务请求返回 503。

### 未来版本保护

当数据库 `user_version` 高于本程序支持的最高版本时，**禁止降级启动**，直接返回错误并在健康接口中标记 `future_version`，不会对数据库做任何写操作。

### 相关环境变量

| 变量 | 默认 | 含义 |
| --- | --- | --- |
| `DATABASE_URL` | `sqlite:///./aquaculture.db` | SQLite 文件路径（仅文件库支持跨进程迁移协调） |
| `MIGRATION_MODE` | `wait` | 其他进程策略：`wait` 等待 / `reject` 立即拒绝 |
| `MIGRATION_WAIT_TIMEOUT` | `60` | 跟随者等待迁移者的秒数 |
| `MIGRATION_LOCK_TIMEOUT` | `30` | 迁移引擎获取锁的等待秒数 |
| `MIGRATION_COPY_BATCH_SIZE` | `500` | 复制搬运的批大小（越小中断粒度越细） |
| `MIGRATION_BACKUP_DIR` | 数据库同目录 | 物理备份存放目录 |

## 健康接口 `/health`

三项信号独立报告，便于区分“进程活着但结构未就绪”与“实例故障”：

* `checks.process`：进程存活（`alive`）；
* `checks.database`：数据库文件可连接（`available` / `unavailable`）；
* `checks.schema`：结构就绪状态（`ready` / `migration_in_progress` / `needs_manual` / `future_version` / `drift` / `pending`），含当前版本与期望版本。

总体 `status`：三项全部正常为 `healthy`（HTTP 200）；进程与数据库正常但结构未就绪为 `starting`（HTTP 503）；数据库不可用为 `unavailable`（HTTP 503）。

结构未就绪时，除 `/health`、`/`、`/docs`、`/openapi.json`、`/redoc` 外的所有业务接口返回 **503**（带 `Retry-After`），避免缺列等请求打到旧结构上。
