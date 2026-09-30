"""可恢复的结构迁移引擎。

职责
----

* **单迁移者协调**：对文件型 SQLite 使用同目录侧车文件 ``fcntl`` 排他锁。
  同一数据库只有一个进程执行迁移；其余进程按策略等待（拿到锁后复核）或立即以
  “迁移进行中”状态拒绝接流量。
* **断点续传**：复制型重建拆成 prepare/copy/verify/switch 四个阶段，搬运按主键
  游标分批，游标随批次提交。进程中断重启后依据影子表与记账状态决定“继续”，
  已搬运的行不会重复搬运。
* **回退 / 人工介入**：版本开始前做物理备份（SQLite online backup）。校验失败时
  备份完好则自动回退到前置版本并留下回退标记；备份不可信、结构漂移、状态无法
  解释时锁定为 ``needs_manual``，不再写数据库，等待人工介入。
* **禁止未来版本降级**：``PRAGMA user_version`` 高于本进程支持的版本时直接拒绝。

记账表（均以下划线开头，结构指纹忽略它们）：

* ``_schema_migrations`` 每个已提交版本一行（版本、校验摘要、时间、说明）；
* ``_migration_state`` 至多一行：中断恢复所需的版本、步骤、阶段与复制游标，
  以及 ``running`` / ``needs_manual`` 状态；
* ``_migration_row_counts`` 每版升级前后的行数快照，用于“不丢数据”不变量。
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import os
import shutil
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, List, Optional, Tuple

from .schema import (
    Column,
    SchemaSpec,
    TableSpec,
    live_digest,
    read_schema,
    schema_digest,
)
from .versions import CURRENT_VERSION, VERSIONS, MigrationVersion, RebuildStep, SqlStep

SHADOW_PREFIX = "__migrate_"
STATE_RUNNING = "running"
STATE_MANUAL = "needs_manual"

DEFAULT_LOCK_TIMEOUT = float(os.getenv("MIGRATION_LOCK_TIMEOUT", "30"))
DEFAULT_POLL_INTERVAL = float(os.getenv("MIGRATION_POLL_INTERVAL", "0.1"))
COPY_BATCH_SIZE = int(os.getenv("MIGRATION_COPY_BATCH_SIZE", "500"))


class MigrationError(Exception):
    """迁移失败基类（数据库保持在某个明确的、可检查的状态）。"""


class FutureVersionError(MigrationError):
    """数据库来自更新版本的程序；禁止降级启动。"""


class SchemaChecksumError(MigrationError):
    """实际结构摘要与版本登记的期望摘要不一致（结构漂移）。"""


class ManualInterventionRequired(MigrationError):
    """迁移无法自动继续或回退，已冻结状态等待人工介入。"""


class MigrationInProgress(MigrationError):
    """另一进程持有迁移锁；按等待策略超时或被要求立即拒绝。"""


class MigrationRolledBack(MigrationError):
    """迁移已自动回退到前置版本；当前程序不得对旧结构接流量。"""


@dataclass
class MigrationResult:
    state: str                       # created|migrated|continued|up_to_date
    from_version: int
    to_version: int
    digest: str
    messages: List[str] = field(default_factory=list)
    lock_wait_seconds: float = 0.0

    def as_dict(self) -> dict:
        return {
            "state": self.state,
            "from_version": self.from_version,
            "to_version": self.to_version,
            "digest": self.digest,
            "messages": self.messages,
            "lock_wait_seconds": self.lock_wait_seconds,
        }


# ---------------------------------------------------------------------------
# 路径与连接
# ---------------------------------------------------------------------------

def database_file_path(database_url: Optional[str] = None) -> Optional[str]:
    """从 SQLAlchemy 风格的 sqlite URL 解析出数据库文件绝对路径。

    ``:memory:`` 与非 SQLite URL 返回 None（内存库无需跨进程协调）。
    """
    url = database_url if database_url is not None else os.getenv(
        "DATABASE_URL", "sqlite:///./aquaculture.db"
    )
    if not url.startswith("sqlite:"):
        return None
    rest = url[len("sqlite:"):]
    if rest.startswith("//"):
        # sqlite:///相对路径 与 sqlite:////绝对路径（/// + /abs）都去掉前导 ///
        path = rest[3:]
    else:
        path = rest
    if path == ":memory:" or path == "":
        return None
    return os.path.abspath(path)


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


@contextlib.contextmanager
def _transaction(conn: sqlite3.Connection):
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# DDL 生成
# ---------------------------------------------------------------------------

def _column_ddl(col: Column) -> str:
    ddl = f'"{col.name}" {col.type}'
    if col.notnull:
        ddl += " NOT NULL"
    return ddl


def create_table_ddl(table: TableSpec, name: Optional[str] = None) -> str:
    table_name = name or table.name
    parts = [_column_ddl(c) for c in table.columns]
    pk_cols = [c.name for c in sorted(table.columns, key=lambda c: c.pk) if c.pk]
    if pk_cols:
        parts.append("PRIMARY KEY (" + ", ".join(f'"{c}"' for c in pk_cols) + ")")
    for fk in table.foreign_keys:
        ref = fk.ref_column or "id"
        parts.append(
            f'FOREIGN KEY("{fk.column}") REFERENCES "{fk.ref_table}" ("{ref}")'
        )
    return f'CREATE TABLE "{table_name}" (\n  ' + ",\n  ".join(parts) + "\n)"


def _index_ddl(index, table_name: str, index_name: Optional[str] = None) -> str:
    u = "UNIQUE " if index.unique else ""
    cols = ", ".join(f'"{c}"' for c in index.columns)
    return f'CREATE {u}INDEX "{index_name or index.name}" ON "{table_name}" ({cols})'


# ---------------------------------------------------------------------------
# 记账表
# ---------------------------------------------------------------------------

_DDL_MIGRATIONS = """
CREATE TABLE IF NOT EXISTS _schema_migrations (
    version INTEGER PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT ''
)
"""

_DDL_STATE = """
CREATE TABLE IF NOT EXISTS _migration_state (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    from_version INTEGER NOT NULL,
    to_version INTEGER NOT NULL,
    step_index INTEGER NOT NULL DEFAULT 0,
    phase TEXT,
    last_copied_id INTEGER NOT NULL DEFAULT 0,
    rows_copied INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'running',
    backup_path TEXT,
    message TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
)
"""

_DDL_COUNTS = """
CREATE TABLE IF NOT EXISTS _migration_row_counts (
    from_version INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (from_version, table_name)
)
"""

_STATE_FIELDS = (
    "from_version", "to_version", "step_index", "phase",
    "last_copied_id", "rows_copied", "status", "backup_path", "message",
)


def _ensure_bookkeeping(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_MIGRATIONS)
    conn.execute(_DDL_STATE)
    conn.execute(_DDL_COUNTS)


def _read_state(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    rows = conn.execute("SELECT * FROM _migration_state WHERE id = 1").fetchall()
    return rows[0] if rows else None


def _write_state(conn: sqlite3.Connection, row: sqlite3.Row, **overrides) -> None:
    values = {f: overrides.get(f, row[f]) for f in _STATE_FIELDS}
    values["updated_at"] = _now()
    conn.execute(
        "UPDATE _migration_state SET "
        "from_version=:from_version, to_version=:to_version, "
        "step_index=:step_index, phase=:phase, last_copied_id=:last_copied_id, "
        "rows_copied=:rows_copied, status=:status, backup_path=:backup_path, "
        "message=:message, updated_at=:updated_at WHERE id=1",
        values,
    )


def _init_state(conn: sqlite3.Connection, from_version: int, to_version: int,
                backup_path: Optional[str]) -> None:
    conn.execute("DELETE FROM _migration_state WHERE id=1")
    conn.execute(
        "INSERT INTO _migration_state (id, from_version, to_version, step_index, "
        "phase, last_copied_id, rows_copied, status, backup_path, message, updated_at) "
        "VALUES (1, ?, ?, 0, NULL, 0, 0, ?, ?, '', ?)",
        (from_version, to_version, STATE_RUNNING, backup_path, _now()),
    )


def _clear_state(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM _migration_state WHERE id=1")


# ---------------------------------------------------------------------------
# 文件锁
# ---------------------------------------------------------------------------

class _FileLock:
    """同目录侧车文件上的 fcntl 排他锁（进程级，随进程死亡自动释放）。"""

    def __init__(self, db_path: Optional[str]):
        self.path = None if db_path is None else db_path + ".migrate.lock"
        self.fd = None

    def acquire(self, blocking: bool, timeout: float) -> bool:
        if self.path is None:
            return True
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        start = time.monotonic()
        while True:
            try:
                fcntl.flock(
                    self.fd,
                    fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
                return True
            except BlockingIOError:
                if not blocking or time.monotonic() - start >= timeout:
                    os.close(self.fd)
                    self.fd = None
                    return False
                time.sleep(DEFAULT_POLL_INTERVAL)

    def release(self) -> None:
        if self.fd is not None:
            with contextlib.suppress(OSError):
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None


# ---------------------------------------------------------------------------
# 物理备份
# ---------------------------------------------------------------------------

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _make_backup(db_path: str, target_version: int,
                 predecessor_digest: str) -> Tuple[str, str]:
    """在线热备份到 ``<db>.pre-vN.bak``，并写旁路校验文件。"""
    backup_dir = os.getenv("MIGRATION_BACKUP_DIR") or os.path.dirname(db_path)
    os.makedirs(backup_dir, exist_ok=True)
    bak = os.path.join(
        backup_dir, os.path.basename(db_path) + f".pre-v{target_version}.bak"
    )
    tmp = bak + ".tmp"
    src = sqlite3.connect(db_path)
    try:
        dst = sqlite3.connect(tmp)
        try:
            src.backup(dst)
            dst.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            dst.close()
    finally:
        src.close()
    os.replace(tmp, bak)
    file_hash = _sha256_file(bak)
    with open(bak + ".sha256", "w", encoding="utf-8") as f:
        f.write(f"{file_hash}  pre-v{target_version} digest={predecessor_digest}\n")
    return bak, file_hash


def _backup_valid(bak: Optional[str], predecessor_digest: str) -> bool:
    if not bak or not os.path.exists(bak) or not os.path.exists(bak + ".sha256"):
        return False
    try:
        with open(bak + ".sha256", encoding="utf-8") as f:
            recorded = f.readline().split()[0]
        if _sha256_file(bak) != recorded:
            return False
        with sqlite3.connect(bak) as chk:
            _, digest = live_digest(chk)
        return digest == predecessor_digest
    except sqlite3.Error:
        return False


# ---------------------------------------------------------------------------
# 校验
# ---------------------------------------------------------------------------

def _verify_expected(conn: sqlite3.Connection, version: int) -> str:
    expected = VERSIONS[version].schema_digest()
    # 结构与 user_version 解耦比对：user_version 在提交时才翻转
    live_spec = read_schema(conn)
    actual = schema_digest(SchemaSpec(version, live_spec.tables))
    if actual != expected:
        raise SchemaChecksumError(
            f"结构摘要不匹配：版本 {version} 期望 {expected[:12]}，实际 {actual[:12]}"
        )
    return actual


def _business_tables(conn: sqlite3.Connection) -> List[str]:
    return [
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '\\_%' ESCAPE '\\'"
        ).fetchall()
    ]


def _snapshot_counts(conn: sqlite3.Connection, from_version: int) -> None:
    for table in _business_tables(conn):
        count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        conn.execute(
            "INSERT OR REPLACE INTO _migration_row_counts "
            "(from_version, table_name, row_count, recorded_at) VALUES (?,?,?,?)",
            (from_version, table, count, _now()),
        )


def _check_counts_unchanged(conn: sqlite3.Connection, from_version: int) -> None:
    for table in _business_tables(conn):
        row = conn.execute(
            "SELECT row_count FROM _migration_row_counts "
            "WHERE from_version=? AND table_name=?",
            (from_version, table),
        ).fetchone()
        if row is None:
            continue
        now_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
        if now_count != row[0]:
            raise MigrationError(
                f"行数不变量被破坏：{table} 升级前 {row[0]} 行，升级后 {now_count} 行"
            )


def _check_invariants(conn: sqlite3.Connection, mv: MigrationVersion) -> None:
    for description, sql in mv.invariants:
        violations = conn.execute(sql).fetchone()[0]
        if violations:
            raise MigrationError(f"升级后不变量不成立（{description}）：违规 {violations} 行")


# ---------------------------------------------------------------------------
# 复制型重建
# ---------------------------------------------------------------------------

def _shadow(table_name: str) -> str:
    return SHADOW_PREFIX + table_name


def _numeric_type(type_str: str) -> bool:
    return type_str.upper().startswith(
        ("INT", "FLOAT", "REAL", "NUMERIC", "DOUBLE", "DECIMAL")
    )


def _canonical_columns(source: TableSpec, target: TableSpec) -> List[Tuple[str, str]]:
    """源/目标同名列，按目标列序返回 (列名, 规范化表达式)。

    数值列统一 CAST 到 REAL 后比较，避免 FLOAT→NUMERIC 亲和性把 2.0 存成 2
    带来的伪差异；文本列原样比较。
    """
    source_names = {c.name for c in source.columns}
    pairs = []
    for c in target.columns:
        if c.name in source_names:
            expr = (f'CAST("{c.name}" AS REAL)' if _numeric_type(c.type)
                    else f'"{c.name}"')
            pairs.append((c.name, expr))
    return pairs


def _prepare_shadow(conn: sqlite3.Connection, step: RebuildStep) -> bool:
    """建影子表；已存在则直接返回 False（prepare 幂等）。"""
    shadow = _shadow(step.table.name)
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (shadow,)
    ).fetchone():
        return False
    conn.execute(create_table_ddl(step.table, shadow))
    return True


def _copy_shadow(conn: sqlite3.Connection, source_spec: TableSpec,
                 step: RebuildStep, inject: Callable) -> None:
    """按主键游标分批搬运；(0, last_copied_id] 区间绝不重复搬运。"""
    table = step.table.name
    shadow = _shadow(table)
    col_names = [c for c, _ in _canonical_columns(source_spec, step.table)]
    col_list = ", ".join(f'"{c}"' for c in col_names)
    placeholders = ", ".join("?" for _ in col_names)
    source_total = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    while True:
        state = _read_state(conn)
        last_id = state["last_copied_id"] or 0
        copied = state["rows_copied"] or 0
        inject(step.name, "copy",
               {"last_id": last_id, "rows_copied": copied, "total": source_total})
        rows = conn.execute(
            f'SELECT {col_list} FROM "{table}" WHERE id > ? ORDER BY id LIMIT ?',
            (last_id, COPY_BATCH_SIZE),
        ).fetchall()
        if not rows:
            return
        with _transaction(conn):
            conn.executemany(
                f'INSERT INTO "{shadow}" ({col_list}) VALUES ({placeholders})',
                [tuple(r) for r in rows],
            )
            _write_state(
                conn, state, phase="copy",
                last_copied_id=rows[-1][0], rows_copied=copied + len(rows),
            )
        if copied + len(rows) >= source_total:
            return


def _verify_shadow(conn: sqlite3.Connection, source_spec: TableSpec,
                   step: RebuildStep) -> None:
    table = step.table.name
    shadow = _shadow(table)
    pairs = _canonical_columns(source_spec, step.table)
    exprs = ", ".join(expr for _, expr in pairs)

    src_count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
    dst_count = conn.execute(f'SELECT COUNT(*) FROM "{shadow}"').fetchone()[0]
    if src_count != dst_count:
        raise MigrationError(
            f"复制校验失败：{table} 源 {src_count} 行 / 影子 {dst_count} 行（可能丢行或重行）"
        )

    def digest_of(tbl: str) -> str:
        h = hashlib.sha256()
        for r in conn.execute(f'SELECT {exprs} FROM "{tbl}" ORDER BY id'):
            h.update(repr(tuple(r)).encode("utf-8"))
            h.update(b"\n")
        return h.hexdigest()

    if digest_of(table) != digest_of(shadow):
        raise MigrationError(f"复制校验失败：{table} 逐行内容摘要不一致")


def _switch_shadow(conn: sqlite3.Connection, step: RebuildStep) -> None:
    """DROP 旧表、影子表改名、重建索引——调用方须包在单事务内。"""
    table = step.table.name
    conn.execute(f'DROP TABLE "{table}"')
    conn.execute(f'ALTER TABLE "{_shadow(table)}" RENAME TO "{table}"')
    for index in sorted(step.table.indexes, key=lambda i: i.name):
        conn.execute(_index_ddl(index, table))


# ---------------------------------------------------------------------------
# 失败处置
# ---------------------------------------------------------------------------

def _freeze_manual(conn: sqlite3.Connection, state: Optional[sqlite3.Row],
                   message: str) -> None:
    """把当前状态冻结为 needs_manual 并抛出人工介入异常。"""
    if state is not None:
        with contextlib.suppress(sqlite3.Error):
            with _transaction(conn):
                _write_state(conn, state, status=STATE_MANUAL, message=message)
    raise ManualInterventionRequired(message)


def _rollback_or_manual(db_path: Optional[str], conn: sqlite3.Connection,
                        state: Optional[sqlite3.Row], message: str,
                        predecessor_digest: str) -> None:
    """校验失败：备份可信则自动回退；否则冻结为人工介入。"""
    bak = state["backup_path"] if state is not None else None
    if db_path and _backup_valid(bak, predecessor_digest):
        marker = db_path + ".rolled-back"
        from_version = state["from_version"]
        conn.close()
        shutil.copyfile(bak, db_path)
        with open(marker, "w", encoding="utf-8") as f:
            f.write(f"{_now()} 自动回退到版本 v{from_version}\n原因: {message}\n")
        raise MigrationRolledBack(
            f"已自动回退数据库到 v{from_version}（备份 {bak}）。原因：{message}。"
            f"当前程序为更新版本，已留下标记 {marker}，请人工排查后删除标记再启动。"
        )
    _freeze_manual(conn, state, f"{message}；且回退备份缺失或已损坏，需要人工介入")


# ---------------------------------------------------------------------------
# 单版本执行与恢复
# ---------------------------------------------------------------------------

def _spec_of(conn: sqlite3.Connection, table: str) -> Optional[TableSpec]:
    for t in read_schema(conn).tables:
        if t.name == table:
            return t
    return None


def _table_matches_target(live: TableSpec, target: TableSpec) -> bool:
    """结构等价：列（名称/规整类型/NOT NULL/主键位）与索引（名/列/唯一）一致。"""
    from .schema import _normalize_type

    def col_key(c):
        return (c.name, _normalize_type(c.type), c.notnull, c.pk)

    def idx_key(i):
        return (i.name, i.columns, i.unique)

    live_cols = sorted((col_key(c) for c in live.columns), key=lambda k: k[0])
    target_cols = sorted((col_key(c) for c in target.columns), key=lambda k: k[0])
    if live_cols != target_cols:
        return False
    live_idx = sorted((idx_key(i) for i in live.indexes), key=lambda k: k[0])
    target_idx = sorted((idx_key(i) for i in target.indexes), key=lambda k: k[0])
    return live_idx == target_idx


def _commit_version(conn: sqlite3.Connection, mv: MigrationVersion, digest: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO _schema_migrations "
        "(version, checksum, applied_at, description) VALUES (?,?,?,?)",
        (mv.version, digest, _now(), mv.description),
    )
    conn.execute(f"PRAGMA user_version={mv.version}")
    _clear_state(conn)


def _execute_version(db_path: Optional[str], conn: sqlite3.Connection,
                     mv: MigrationVersion, inject: Callable,
                     resume: Optional[sqlite3.Row]) -> List[str]:
    """执行（或从中断处继续）单个版本升级。"""
    notes: List[str] = []
    pred = VERSIONS[mv.predecessor]
    pred_digest = pred.schema_digest()

    if resume is not None:
        if resume["to_version"] != mv.version:
            _freeze_manual(conn, resume,
                           f"中断记录目标 v{resume['to_version']} 与待执行 v{mv.version} 不符")
        start_index = resume["step_index"]
        start_phase = resume["phase"]
        notes.append(f"检测到 v{mv.version} 迁移中断（步骤 {start_index}/{phase_label(start_phase)}），按记账继续")
    else:
        # 前置版本必须严格匹配，且结构摘要必须与前置版本登记一致
        actual_uv = conn.execute("PRAGMA user_version").fetchone()[0]
        if actual_uv != mv.predecessor:
            raise MigrationError(
                f"v{mv.version} 的前置版本必须是 v{mv.predecessor}，当前为 v{actual_uv}"
            )
        _verify_expected(conn, mv.predecessor)
        with _transaction(conn):
            _snapshot_counts(conn, mv.predecessor)
        backup_path = None
        if any(isinstance(s, RebuildStep) for s in mv.steps):
            if db_path:
                backup_path, _ = _make_backup(db_path, mv.version, pred_digest)
                notes.append(f"已建立升级前物理备份 {backup_path}")
            else:
                notes.append("非文件数据库无法做物理备份，复制型变更失败时需人工恢复")
        with _transaction(conn):
            _init_state(conn, mv.predecessor, mv.version, backup_path)
        start_index, start_phase = 0, None

    def fail(exc: Exception) -> None:
        state = _read_state(conn)
        if state is not None and state["status"] == STATE_RUNNING and state["phase"] == "copy":
            # 搬运中断：游标已逐批落盘，重启后继续，已搬运行不会重搬
            raise MigrationError(
                f"v{mv.version} 数据搬运在已复制 {state['rows_copied']} 行处中断，"
                "游标已持久化，重启后继续且不重复搬运"
            ) from exc
        _freeze_manual(conn, state, f"升级 v{mv.version} 时失败：{exc}")

    try:
        for index, step in enumerate(mv.steps):
            if index < start_index:
                continue
            state = _read_state(conn)

            if isinstance(step, SqlStep):
                inject(step.name, "sql", {})
                with _transaction(conn):
                    for stmt in step.statements:
                        conn.execute(stmt)
                    _write_state(conn, state, step_index=index + 1, phase=None,
                                 last_copied_id=0, rows_copied=0)

            elif isinstance(step, RebuildStep):
                resumed = start_phase if index == start_index else None
                source_spec = _spec_of(conn, step.table.name)

                # 崩溃点可能在“切换事务提交后、记账推进前”：主表已换成目标结构。
                # 此时切换是原子完成的，只需丢弃残留影子表并把记账推进到下一步，
                # 绝不重新搬运。
                if source_spec is not None and _table_matches_target(source_spec, step.table):
                    with _transaction(conn):
                        conn.execute(
                            f'DROP TABLE IF EXISTS "{_shadow(step.table.name)}"'
                        )
                        _write_state(conn, _read_state(conn), step_index=index + 1,
                                     phase=None, last_copied_id=0, rows_copied=0)
                    start_phase = None
                    continue
                if source_spec is None:
                    _freeze_manual(conn, state,
                                   f"复制重建 {step.table.name} 时找不到源表，状态无法解释")

                # 主表仍是旧结构时，影子表必须存在（prepare 之后）；缺失且记账显示
                # 已进入 copy/verify，说明状态无法解释
                shadow = _shadow(step.table.name)
                shadow_exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (shadow,),
                ).fetchone()
                if not shadow_exists and resumed not in (None, "prepare"):
                    _freeze_manual(
                        conn, state,
                        f"{step.table.name} 记账停在 {phase_label(resumed)} 阶段，"
                        "但影子表不存在，状态无法解释"
                    )

                # 1) prepare（影子表已存在则跳过，幂等）
                if resumed in (None, "prepare"):
                    inject(step.name, "prepare", {})
                    with _transaction(conn):
                        _prepare_shadow(conn, step)
                        _write_state(conn, state, step_index=index, phase="prepare")

                # 2) copy（游标在 (0,last_id] 之外续跑；先核对影子表与记账一致）
                state = _read_state(conn)
                in_shadow = conn.execute(f'SELECT COUNT(*) FROM "{shadow}"').fetchone()[0]
                if in_shadow != (state["rows_copied"] or 0):
                    _freeze_manual(
                        conn, state,
                        f"{step.table.name} 影子表已有 {in_shadow} 行但记账为 "
                        f"{state['rows_copied']} 行，状态矛盾，拒绝猜测续跑点"
                    )
                _copy_shadow(conn, source_spec, step, inject)

                # 3) verify（行数 + 逐行摘要；失败则回退或人工介入）
                state = _read_state(conn)
                inject(step.name, "verify",
                       {"rows_copied": state["rows_copied"]})
                try:
                    _verify_shadow(conn, source_spec, step)
                except MigrationError as exc:
                    _rollback_or_manual(db_path, conn, _read_state(conn), str(exc), pred_digest)
                with _transaction(conn):
                    _write_state(conn, _read_state(conn), phase="verify")

                # 4) switch：DROP/改名/建索引在单事务内原子完成
                inject(step.name, "switch", {})
                conn.execute("PRAGMA foreign_keys=OFF")
                try:
                    with _transaction(conn):
                        _switch_shadow(conn, step)
                finally:
                    conn.execute("PRAGMA foreign_keys=ON")
                violations = conn.execute("PRAGMA foreign_key_check").fetchall()
                if violations:
                    _rollback_or_manual(
                        db_path, conn, _read_state(conn),
                        f"切换后外键检查发现 {len(violations)} 处违规", pred_digest
                    )
                # 切换已提交、记账尚未推进：真实的崩溃窗口。此处抛错后重启，
                # 引擎应识别主表已达目标结构并跳过该步，而非重新搬运。
                # 该阶段的异常不冻结为 needs_manual——切换事务已提交，属可续跑状态。
                try:
                    inject(step.name, "switched", {})
                except Exception as exc:
                    raise MigrationError(
                        f"v{mv.version} 的 {step.table.name} 已完成切换提交，"
                        "进程在推进记账前中断；重启后将识别新结构并继续，不会重新搬运"
                    ) from exc
                with _transaction(conn):
                    _write_state(conn, _read_state(conn), step_index=index + 1,
                                 phase=None, last_copied_id=0, rows_copied=0)
                start_phase = None
            else:  # pragma: no cover - 防御
                _freeze_manual(conn, state, f"未知步骤类型: {step!r}")

        # 整版收尾：外键、结构摘要、行数不变量、业务不变量
        conn.execute("PRAGMA foreign_keys=ON")
        fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if fk_violations:
            _rollback_or_manual(
                db_path, conn, _read_state(conn),
                f"整版外键检查发现 {len(fk_violations)} 处违规", pred_digest
            )
        digest = _verify_expected(conn, mv.version)
        _check_counts_unchanged(conn, mv.predecessor)
        _check_invariants(conn, mv)
        state_row = _read_state(conn)
        backup_path = state_row["backup_path"] if state_row else None
        with _transaction(conn):
            _commit_version(conn, mv, digest)

        if backup_path:
            for suffix in ("", ".sha256"):
                with contextlib.suppress(OSError):
                    os.remove(backup_path + suffix)
        return notes
    except (MigrationRolledBack, ManualInterventionRequired):
        raise
    except MigrationError:
        raise
    except Exception as exc:
        fail(exc)


def phase_label(phase: Optional[str]) -> str:
    return phase or "未开始"


# ---------------------------------------------------------------------------
# 全新库
# ---------------------------------------------------------------------------

def _create_fresh(conn: sqlite3.Connection) -> str:
    spec = VERSIONS[CURRENT_VERSION].schema
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        with _transaction(conn):
            for table in sorted(spec.tables, key=lambda t: t.name):
                conn.execute(create_table_ddl(table))
            for table in sorted(spec.tables, key=lambda t: t.name):
                for index in sorted(table.indexes, key=lambda i: i.name):
                    conn.execute(_index_ddl(index, table.name))
            _ensure_bookkeeping(conn)
            for version in range(1, CURRENT_VERSION + 1):
                mv = VERSIONS[version]
                conn.execute(
                    "INSERT INTO _schema_migrations "
                    "(version, checksum, applied_at, description) VALUES (?,?,?,?)",
                    (version, mv.schema_digest(), _now(), "全新库基线登记"),
                )
            conn.execute(f"PRAGMA user_version={CURRENT_VERSION}")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")
    return _verify_expected(conn, CURRENT_VERSION)


# ---------------------------------------------------------------------------
# 顶层入口
# ---------------------------------------------------------------------------

def run_migrations(
    db_path: Optional[str] = None,
    *,
    database_url: Optional[str] = None,
    mode: str = "wait",
    lock_timeout: float = DEFAULT_LOCK_TIMEOUT,
    failure_injection: Optional[Callable[[str, str, dict], None]] = None,
) -> MigrationResult:
    """确保数据库结构迁移到当前版本。

    :param db_path: SQLite 文件路径；None 时从 ``database_url`` / ``DATABASE_URL`` 解析。
    :param mode: ``wait``（等待持锁进程，拿到锁后复核）或 ``reject``（锁被占用即失败）。
    :param failure_injection: ``(step_name, phase, ctx) -> None`` 回调，
        抛异常模拟进程在某阶段中断，供故障注入测试使用。
    """
    if db_path is None:
        db_path = database_file_path(database_url)
    inject = failure_injection or (lambda step, phase, ctx: None)

    marker = None if db_path is None else db_path + ".rolled-back"
    if marker and os.path.exists(marker):
        raise ManualInterventionRequired(
            f"检测到自动回退标记 {marker}，为防止重复失败已拒绝启动；"
            "请人工确认数据后删除该标记文件再启动"
        )

    lock = _FileLock(db_path)
    wait_start = time.monotonic()
    if not lock.acquire(blocking=(mode == "wait"), timeout=lock_timeout):
        raise MigrationInProgress(
            "另一进程正在迁移该数据库"
            + ("（策略 reject：立即拒绝接流量）" if mode == "reject" else "（等待超时）")
        )
    waited = time.monotonic() - wait_start

    conn = _connect(db_path)
    result_state = "up_to_date"
    try:
        _ensure_bookkeeping(conn)

        uv = conn.execute("PRAGMA user_version").fetchone()[0]
        if uv > CURRENT_VERSION:
            raise FutureVersionError(
                f"数据库结构版本为 v{uv}，高于本程序支持的最高版本 v{CURRENT_VERSION}；"
                "禁止以旧版本程序降级启动，请升级程序或用备份恢复"
            )

        state = _read_state(conn)
        if state is not None and state["status"] == STATE_MANUAL:
            raise ManualInterventionRequired(
                f"迁移停在 v{state['to_version']} 等待人工介入：{state['message'] or '(无详情)'}"
            )

        business = _business_tables(conn)

        if uv == 0 and not business:
            digest = _create_fresh(conn)
            return MigrationResult("created", 0, CURRENT_VERSION, digest,
                                   lock_wait_seconds=waited)

        if uv == 0 and business:
            # 无版本记账的旧库：只有结构摘要与已知 v1 完全一致才补登记
            legacy_spec = read_schema(conn)
            legacy_digest = schema_digest(SchemaSpec(1, legacy_spec.tables))
            if legacy_digest != VERSIONS[1].schema_digest():
                raise SchemaChecksumError(
                    "未登记版本的旧数据库结构与已知 v1 摘要 "
                    f"{VERSIONS[1].schema_digest()[:12]} 不符（实际 {legacy_digest[:12]}），"
                    "拒绝猜测，请人工确认后补登记"
                )
            with _transaction(conn):
                conn.execute(
                    "INSERT INTO _schema_migrations "
                    "(version, checksum, applied_at, description) VALUES (?,?,?,?)",
                    (1, legacy_digest, _now(), "沿用旧 SQLite 文件补登记"),
                )
                conn.execute("PRAGMA user_version=1")
            uv = 1
            result_state = "migrated"

        messages: List[str] = []

        if state is not None:
            # 上次迁移中断：只续跑未完成的那一版
            target = state["to_version"]
            if target > CURRENT_VERSION:
                _freeze_manual(conn, state, f"中断记录指向未来版本 v{target}，拒绝处理")
            messages.extend(
                _execute_version(db_path, conn, VERSIONS[target], inject, state)
            )
            result_state = "continued"
        elif uv < CURRENT_VERSION:
            for version in range(uv + 1, CURRENT_VERSION + 1):
                mv = VERSIONS[version]
                if mv.predecessor != version - 1:
                    raise MigrationError(
                        f"版本链断裂：v{version} 的前置版本声明为 v{mv.predecessor}"
                    )
                messages.extend(_execute_version(db_path, conn, mv, inject, None))
            result_state = "migrated"

        digest = _verify_expected(conn, CURRENT_VERSION)
        return MigrationResult(result_state, uv if state is None else state["from_version"],
                               CURRENT_VERSION, digest, messages, waited)
    finally:
        conn.close()
        lock.release()


def wait_for_migrations(db_path: Optional[str], *, timeout: float) -> MigrationResult:
    """跟随者进程使用：等待持锁的迁移者完成并复核最终状态。"""
    return run_migrations(db_path, mode="wait", lock_timeout=timeout)


# ---------------------------------------------------------------------------
# 只读状态检查（健康接口用，不持锁、不写入）
# ---------------------------------------------------------------------------

def current_status(database_url: Optional[str] = None) -> dict:
    result = {
        "database": "unavailable",
        "schema_ready": False,
        "version": None,
        "expected_version": CURRENT_VERSION,
        "migration_state": None,
        "detail": "",
    }
    path = database_file_path(database_url)
    if path is None:
        result["detail"] = "非文件型/内存数据库"
        return result
    try:
        conn = sqlite3.connect(path, timeout=2, isolation_level=None)
    except sqlite3.Error as exc:
        result["detail"] = f"无法打开数据库: {exc}"
        return result
    try:
        try:
            conn.execute("SELECT 1").fetchone()
        except sqlite3.Error as exc:
            result["detail"] = f"数据库不可用: {exc}"
            return result
        result["database"] = "available"

        uv = conn.execute("PRAGMA user_version").fetchone()[0]
        result["version"] = uv
        if uv > CURRENT_VERSION:
            result["migration_state"] = "future_version"
            result["detail"] = f"数据库来自更新版本 v{uv}，本进程拒绝降级服务"
            return result

        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='_migration_state'"
        ).fetchone():
            row = conn.execute(
                "SELECT status, to_version, message FROM _migration_state WHERE id=1"
            ).fetchone()
            if row:
                result["migration_state"] = row[0]
                if row[0] == STATE_MANUAL:
                    result["detail"] = f"迁移等待人工介入（目标 v{row[1]}）: {row[2]}"
                else:
                    result["detail"] = f"结构迁移进行中（目标 v{row[1]}）"
                return result

        if os.path.exists(path + ".rolled-back"):
            result["migration_state"] = "rolled_back"
            result["detail"] = "上次迁移已自动回退，等待人工处理"
            return result

        if uv == CURRENT_VERSION:
            _, digest = live_digest(conn)
            if digest == VERSIONS[CURRENT_VERSION].schema_digest():
                result["schema_ready"] = True
                result["migration_state"] = "up_to_date"
                result["detail"] = "结构为当前版本"
            else:
                result["migration_state"] = "drift"
                result["detail"] = "版本号正确但结构摘要不匹配（结构漂移）"
        else:
            result["migration_state"] = "pending"
            result["detail"] = f"结构版本 v{uv}，需要迁移到 v{CURRENT_VERSION}"
    finally:
        conn.close()
    return result
