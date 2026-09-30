"""迁移执行引擎：跨进程互斥、心跳、备份、可恢复复制与校验。

状态模型
========

``schema_meta``（键值表）记录当前结构版本与一次迁移的断点：

    version, migration_target, phase, last_copied_id, rows_copied,
    backup_path, owner, attempts, blocked_reason, updated_at

``schema_migration_lock`` 是单行锁（``id=1``）。迁移者持有它并周期性刷新
心跳；心跳超时说明迁移进程已死，等待者可以接管锁并按断点恢复。

恢复决策（v3 复制型迁移）
------------------------

* 源表完好、影子表与断点一致        -> **继续**（从 ``last_copied_id`` 之后复制）
* 影子表损坏但源表仍匹配 v2 摘要    -> **回退本次尝试**（删影子表，从头复制，
  不触碰线上表；文件备份保留）
* 换名已提交、结构为 v3 但不变量失败 -> **人工介入**（已提交的版本绝不自动降级）
* 结构摘要既不匹配源版本也不匹配目标 -> **人工介入**
"""

from __future__ import annotations

import hashlib
import os
import shutil
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from sqlalchemy.engine import make_url

from . import schema

# ---------------------------------------------------------------------------
# 可调参数（环境变量覆盖，便于测试故障注入）
# ---------------------------------------------------------------------------

CURRENT_VERSION = 3

LOCK_STALE_SECONDS = float(os.getenv("MIGRATION_LOCK_STALE_SECONDS", "30"))
WAIT_TIMEOUT_SECONDS = float(os.getenv("MIGRATION_WAIT_TIMEOUT_SECONDS", "60"))
POLL_INTERVAL_SECONDS = float(os.getenv("MIGRATION_POLL_INTERVAL_SECONDS", "0.2"))
COPY_BATCH_SIZE = int(os.getenv("MIGRATION_COPY_BATCH_SIZE", "500"))
STEP_SLEEP_SECONDS = float(os.getenv("MIGRATION_STEP_SLEEP", "0"))

# 故障注入：{"after_batches": int, "at_phase": "verify|swap|invariants",
#           "exit": bool}。exit=True 时模拟进程崩溃（os._exit），
# 已提交的断点保留；否则抛出 MigrationCrash（同一进程内的单测使用）。
_FAULTS: Dict[str, Any] = {}
_FAULTS_LOCK = threading.Lock()


def set_faults(faults: Optional[Dict[str, Any]]) -> None:
    global _FAULTS
    with _FAULTS_LOCK:
        _FAULTS = dict(faults or {})


def _fault(where: str, ctx: Dict[str, Any]) -> None:
    with _FAULTS_LOCK:
        spec = dict(_FAULTS)
    hit = False
    if where == "after_batch" and spec.get("after_batches") is not None:
        hit = ctx["batch_index"] >= int(spec["after_batches"])
    if where == "phase" and spec.get("at_phase") == ctx["phase"]:
        hit = True
    if not hit:
        return
    if spec.get("exit"):
        os._exit(9)
    raise MigrationCrash(f"故障注入：{where} {ctx}")


# 环境变量形式的故障注入（供子进程使用）
if os.getenv("MIGRATION_FAULT_AFTER_BATCHES"):
    _FAULTS["after_batches"] = int(os.environ["MIGRATION_FAULT_AFTER_BATCHES"])
    _FAULTS["exit"] = True
if os.getenv("MIGRATION_FAULT_AT_PHASE"):
    _FAULTS["at_phase"] = os.environ["MIGRATION_FAULT_AT_PHASE"]
    _FAULTS["exit"] = True


class MigrationError(RuntimeError):
    """迁移失败的基类。"""


class FutureVersionError(MigrationError):
    """数据库来自更新版本的程序：禁止降级启动。"""


class MigrationBlocked(MigrationError):
    """状态无法自动判定，必须人工介入。"""


class MigrationCrash(MigrationError):
    """测试故障注入抛出（生产环境等价于进程被杀掉）。"""


class MigrationWaitTimeout(MigrationError):
    """等待其他进程迁移超时。"""


@dataclass
class StartupResult:
    state: str                      # ready | waiting | blocked | future_version
    version: Optional[int] = None
    target: int = CURRENT_VERSION
    migrator: bool = False
    detail: str = ""
    lock_owner: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def ready(self) -> bool:
        return self.state == "ready"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sqlite_path_from_engine(engine: Any) -> Optional[Path]:
    url = make_url(str(engine.url))
    if url.get_backend_name() != "sqlite" or not url.database or url.database == ":memory:":
        return None
    return Path(url.database)


def _connect(path: Optional[Path]) -> sqlite3.Connection:
    if path is None:
        conn = sqlite3.connect(":memory:")
    else:
        conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.isolation_level = None  # 显式管理事务
    conn.execute(f"PRAGMA busy_timeout = 10000")
    # 迁移连接关闭外键约束；换表时禁止 RENAME 改写其他表的外键引用，
    # 保证 batches.pond_id 在 ponds->ponds__old/ponds__new->ponds 后仍引用 ponds
    conn.execute("PRAGMA foreign_keys = OFF")
    conn.execute("PRAGMA legacy_alter_table = ON")
    return conn


# ---------------------------------------------------------------------------
# 元数据表 / 锁
# ---------------------------------------------------------------------------

_DDL_META = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

_DDL_LOCK = """
CREATE TABLE IF NOT EXISTS schema_migration_lock (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    owner TEXT NOT NULL,
    state TEXT NOT NULL,
    heartbeat TEXT NOT NULL,
    target_version INTEGER NOT NULL,
    note TEXT,
    acquired_at TEXT NOT NULL
)
"""


def _ensure_meta_tables(conn: sqlite3.Connection) -> None:
    conn.execute(_DDL_META)
    conn.execute(_DDL_LOCK)


def _meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM schema_meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def _meta_put(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT INTO schema_meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, "" if value is None else str(value)),
    )


def _meta_delete(conn: sqlite3.Connection, *keys: str) -> None:
    conn.execute(
        f"DELETE FROM schema_meta WHERE key IN ({','.join('?' for _ in keys)})",
        keys,
    )


def _business_tables(conn: sqlite3.Connection) -> list:
    return [
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        if r[0] not in schema.META_TABLES
    ]


def _owner_id() -> str:
    return f"{uuid.uuid4().hex[:12]}@pid{os.getpid()}@{socket_hostname()}"


def socket_hostname() -> str:
    import socket

    return socket.gethostname()


class LockBusy(Exception):
    pass


def _acquire_lock(
    path: Optional[Path], target: int, wait: float
) -> Tuple[sqlite3.Connection, str]:
    """尝试成为唯一迁移者。

    返回 ``(持锁连接, owner)``。锁被活跃心跳持有时轮询等待最多 *wait* 秒；
    心跳过期则接管（前任进程已死，后续断点恢复逻辑保证安全）。
    """
    owner = _owner_id()
    deadline = time.monotonic() + wait
    conn = _connect(path)
    conn.execute("PRAGMA foreign_keys = OFF")
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError:
            conn.rollback()
            if time.monotonic() >= deadline:
                conn.close()
                raise MigrationWaitTimeout("数据库写锁繁忙，无法获取迁移锁")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        _ensure_meta_tables(conn)
        row = conn.execute("SELECT * FROM schema_migration_lock WHERE id=1").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO schema_migration_lock"
                "(id, owner, state, heartbeat, target_version, note, acquired_at) "
                "VALUES(1, ?, 'running', ?, ?, '', ?)",
                (owner, _now(), target, _now()),
            )
            conn.commit()
            return conn, owner

        # 已有锁
        if row["state"] == "blocked":
            # 若库实际已健康（典型：运维就地修复了问题并使结构达标），
            # 陈旧的 blocked 锁不再有意义——清除并继续正常启动。
            raw_version = _meta_get(conn, "version")
            digest, _ = schema.schema_fingerprint(conn)
            healthy = (
                raw_version is not None
                and int(raw_version) == CURRENT_VERSION
                and digest == schema.EXPECTED_FINGERPRINTS[CURRENT_VERSION]
            )
            if healthy:
                conn.execute("DELETE FROM schema_migration_lock WHERE id=1")
                conn.execute(
                    "INSERT INTO schema_migration_lock"
                    "(id, owner, state, heartbeat, target_version, note, acquired_at) "
                    "VALUES(1, ?, 'running', ?, ?, '', ?)",
                    (owner, _now(), CURRENT_VERSION, _now()),
                )
                _meta_delete(conn, "blocked_reason")
                conn.commit()
                return conn, owner
            conn.commit()
            conn.close()
            raise MigrationBlocked(
                f"迁移已被标记为需人工介入（owner={row['owner']}）：{row['note']}"
            )

        age = (
            datetime.now(timezone.utc)
            - datetime.fromisoformat(row["heartbeat"])
        ).total_seconds()
        if age <= LOCK_STALE_SECONDS:
            conn.commit()
            if time.monotonic() >= deadline:
                note = f"owner={row['owner']} 正在迁移到 v{row['target_version']}"
                conn.close()
                raise MigrationWaitTimeout(f"等待迁移者完成超时：{note}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        # 心跳过期：接管
        conn.execute(
            "UPDATE schema_migration_lock SET owner=?, state='running', "
            "heartbeat=?, target_version=?, note=?, acquired_at=? WHERE id=1",
            (owner, _now(), target, f"接管过期锁(前任={row['owner']})", _now()),
        )
        conn.commit()
        return conn, owner


def _release_lock(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM schema_migration_lock WHERE id=1")
    conn.commit()


def _mark_blocked(conn: sqlite3.Connection, reason: str) -> None:
    conn.execute(
        "UPDATE schema_migration_lock SET state='blocked', note=?, heartbeat=? WHERE id=1",
        (reason[:500], _now()),
    )
    _meta_put(conn, "blocked_reason", reason[:500])
    conn.commit()


def _heartbeat(conn: sqlite3.Connection, extra: Optional[dict] = None) -> None:
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "UPDATE schema_migration_lock SET heartbeat=? WHERE id=1", (_now(),)
    )
    if extra:
        for k, v in extra.items():
            _meta_put(conn, k, v)
    _meta_put(conn, "updated_at", _now())
    conn.commit()


# ---------------------------------------------------------------------------
# 备份与校验
# ---------------------------------------------------------------------------

def _backup_database_file(path: Path, from_version: int) -> str:
    backup = path.with_name(f"{path.name}.v{from_version}.bak")
    source = sqlite3.connect(str(path))
    dest = sqlite3.connect(str(backup))
    try:
        source.backup(dest)
    finally:
        dest.close()
        source.close()
    return str(backup)


def _expect_fingerprint(conn: sqlite3.Connection, version: int, where: str) -> str:
    digest, per_table = schema.schema_fingerprint(conn)
    expected = schema.EXPECTED_FINGERPRINTS[version]
    if digest != expected:
        detail = ", ".join(sorted(per_table))
        raise MigrationBlocked(
            f"{where}：实际结构摘要不匹配 v{version} 预期。"
            f"expected={expected[:12]} actual={digest[:12]} tables=[{detail}]"
        )
    return digest


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]


def _streaming_digest(conn: sqlite3.Connection, sql: str) -> str:
    """对查询结果按行做稳定哈希（统一投影，用于源表/影子表逐行比对）。"""
    digest = hashlib.sha256()
    for row in conn.execute(sql):
        digest.update(repr(tuple(row)).encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


# 升级后的逻辑投影（源表用 COALESCE/补码，影子表直接取列）
_PONDS_PROJECTION_SOURCE = """
SELECT id, name, area, water_depth,
       COALESCE(species, '未指定品种'),
       status, created_at, updated_at,
       COALESCE(location_code, ''),
       'P' || printf('%06d', id)
FROM ponds ORDER BY id
"""

_PONDS_PROJECTION_SHADOW = """
SELECT id, name, area, water_depth, species, status, created_at, updated_at,
       location_code, pond_code
FROM ponds__new ORDER BY id
"""


# ---------------------------------------------------------------------------
# 版本探测
# ---------------------------------------------------------------------------

def _detect_version(conn: sqlite3.Connection) -> int:
    """返回当前库版本；全新库返回 0，无法判定抛 MigrationBlocked。

    无版本记录的库有两种合法来源：最初的 v1 历史库，以及极窄的崩溃窗口
    ——v1->v2 的加列事务已提交、版本号尚未落盘。两者都能靠结构摘要精确识别。
    """
    _ensure_meta_tables(conn)
    raw = _meta_get(conn, "version")
    tables = _business_tables(conn)
    if raw is not None:
        return int(raw)
    if not tables:
        return 0
    digest, _ = schema.schema_fingerprint(conn)
    for version, expected in sorted(schema.EXPECTED_FINGERPRINTS.items()):
        if digest == expected:
            if version > 1:
                # 崩溃窗口：补齐版本记录后继续后续迁移
                _meta_put(conn, "version", version)
                conn.commit()
            return version
    raise MigrationBlocked(
        "无版本记录且结构不匹配任何已知历史版本，拒绝猜测式迁移"
    )


# ---------------------------------------------------------------------------
# 迁移步骤
# ---------------------------------------------------------------------------

def _init_fresh(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN IMMEDIATE")
    _ensure_meta_tables(conn)
    schema.build_schema_at(conn, CURRENT_VERSION)
    _meta_put(conn, "version", CURRENT_VERSION)
    _meta_put(conn, "initialized_at", _now())
    conn.commit()


def _migrate_v1_to_v2(conn: sqlite3.Connection, path: Optional[Path]) -> None:
    _expect_fingerprint(conn, 1, "v1->v2 前置校验")
    backup = ""
    if path is not None:
        conn.commit()  # backup API 需要源连接上无写事务
        backup = _backup_database_file(path, 1)
    conn.execute("BEGIN IMMEDIATE")
    _meta_put(conn, "migration_target", 2)
    _meta_put(conn, "phase", "alter_columns")
    _meta_put(conn, "backup_path", backup)
    for table, sql in schema.V2_ALTER_COLUMNS:
        existing = {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}
        target_col = sql.split("ADD COLUMN", 1)[1].strip().split()[0]
        if target_col not in existing:
            conn.execute(sql)
    conn.commit()

    conn.execute("BEGIN IMMEDIATE")
    _expect_fingerprint(conn, 2, "v1->v2 升级后校验")
    _meta_put(conn, "version", 2)
    _meta_delete(conn, "migration_target", "phase", "backup_path", "blocked_reason")
    conn.commit()


def _reset_copy_attempt(conn: sqlite3.Connection, reason: str) -> None:
    """回退一次失败的复制尝试：删影子表与断点，源表从未被触碰。"""
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("DROP TABLE IF EXISTS ponds__new")
    _meta_put(conn, "phase", "copy")
    _meta_put(conn, "last_copied_id", 0)
    _meta_put(conn, "rows_copied", 0)
    attempts = int(_meta_get(conn, "attempts") or 0) + 1
    _meta_put(conn, "attempts", attempts)
    _meta_put(conn, "last_rollback_reason", reason[:300])
    conn.commit()


def _migrate_v2_to_v3(conn: sqlite3.Connection, path: Optional[Path]) -> None:
    _expect_fingerprint(conn, 2, "v2->v3 前置校验")

    # ---- 断点分类：继续 / 回退本次尝试 / 推进到换名 / 人工 --------------
    phase = _meta_get(conn, "phase")
    shadow_exists = bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ponds__new'"
        ).fetchone()
    )

    if phase == "copy" and shadow_exists and not _shadow_matches_breakpoint(conn):
        # 影子表与断点对不上（典型：复制过程中页损坏/被外力破坏）。
        # 源表仍然完好 -> 回退本次复制，从头再来；源表也坏了 -> 人工介入。
        if schema.schema_fingerprint(conn)[0] != schema.EXPECTED_FINGERPRINTS[2]:
            raise MigrationBlocked(
                "影子表断点损坏且源表 v2 摘要不符，无法安全继续或回退"
            )
        _reset_copy_attempt(conn, "影子表与 last_copied_id 不一致")
        shadow_exists = False

    if phase is None or (phase == "copy" and not shadow_exists):
        backup = _meta_get(conn, "backup_path") or ""
        if path is not None and not backup:
            conn.commit()
            backup = _backup_database_file(path, 2)
        conn.execute("BEGIN IMMEDIATE")
        _meta_put(conn, "migration_target", 3)
        if backup:
            _meta_put(conn, "backup_path", backup)
        _meta_put(conn, "phase", "copy")
        _meta_put(conn, "last_copied_id", 0)
        _meta_put(conn, "rows_copied", 0)
        if _meta_get(conn, "attempts") is None:
            _meta_put(conn, "attempts", 0)
        conn.execute(schema.V3_PONDS_SQL)
        conn.commit()

    elif phase in ("verify", "swap"):
        digest_now = schema.schema_fingerprint(conn)[0]
        if digest_now == schema.EXPECTED_FINGERPRINTS[3]:
            _finalize_v3(conn)
            return
        if not shadow_exists:
            raise MigrationBlocked(
                f"迁移停在 {phase} 阶段但影子表缺失，源表非 v3，需人工恢复备份"
            )
        if schema.schema_fingerprint(conn)[0] != schema.EXPECTED_FINGERPRINTS[2]:
            raise MigrationBlocked(
                f"迁移停在 {phase} 阶段且源表 v2 摘要不符，需人工介入"
            )
        # 影子表在但没换名：落到下方重新校验，通过后继续换名
    elif phase == "copy":
        # 断点与影子表一致：直接进入分批复制循环（从 last_copied_id 之后续搬）
        pass
    else:
        raise MigrationBlocked(f"未知迁移阶段标记: {phase!r}")

    # ---- 分批复制（断点之后，绝不重复搬运） -----------------------------
    if _meta_get(conn, "phase") == "copy":
        _copy_ponds_in_batches(conn)

    # ---- 全量校验：行数 + 逐行稳定哈希 ----------------------------------
    _heartbeat(conn)
    src_count = _table_count(conn, "ponds")
    dst_count = _table_count(conn, "ponds__new")
    if src_count != dst_count:
        raise MigrationBlocked(
            f"复制后行数不一致 source={src_count} shadow={dst_count}"
        )
    if conn.execute("SELECT COUNT(*) - COUNT(DISTINCT id) FROM ponds__new").fetchone()[0]:
        raise MigrationBlocked("影子表存在重复 id")
    if _streaming_digest(conn, _PONDS_PROJECTION_SOURCE) != _streaming_digest(
        conn, _PONDS_PROJECTION_SHADOW
    ):
        raise MigrationBlocked("逐行校验摘要不一致：源表与影子表内容不同")

    conn.execute("BEGIN IMMEDIATE")
    _meta_put(conn, "phase", "verify")
    conn.commit()
    _fault("phase", {"phase": "verify"})

    # ---- 事务内换名（SQLite 不支持的表变更） -----------------------------
    _fault("phase", {"phase": "swap"})
    conn.execute("BEGIN IMMEDIATE")
    # 本连接 foreign_keys=OFF：RENAME 不会改写 batches 的外键引用
    conn.execute("DROP INDEX IF EXISTS ix_ponds_id")
    conn.execute("DROP INDEX IF EXISTS ix_ponds_name")
    conn.execute("ALTER TABLE ponds RENAME TO ponds__old")
    conn.execute("ALTER TABLE ponds__new RENAME TO ponds")
    conn.execute(schema.V3_POND_CODE_INDEX[1])
    conn.execute("CREATE INDEX ix_ponds_id ON ponds (id)")
    conn.execute("CREATE UNIQUE INDEX ix_ponds_name ON ponds (name)")
    violations = conn.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        conn.rollback()
        raise MigrationBlocked(f"换表后外键校验失败: {violations[:5]}")
    _meta_put(conn, "phase", "swap")
    conn.commit()

    # 删除旧表（独立事务；失败也不影响已是 v3 的库，下次启动清理）
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("DROP TABLE IF EXISTS ponds__old")
    conn.commit()

    _finalize_v3(conn)


def _shadow_matches_breakpoint(conn: sqlite3.Connection) -> bool:
    last_id = int(_meta_get(conn, "last_copied_id") or 0)
    copied = int(_meta_get(conn, "rows_copied") or 0)
    row = conn.execute(
        "SELECT COUNT(*) AS c, COALESCE(MAX(id), 0) AS m, "
        "COUNT(DISTINCT id) AS d FROM ponds__new"
    ).fetchone()
    if row["c"] != row["d"]:
        return False
    if row["m"] != last_id or row["c"] != copied:
        return False
    expected = conn.execute(
        "SELECT COUNT(*) FROM ponds WHERE id <= ?", (last_id,)
    ).fetchone()[0]
    return row["c"] == expected


def _copy_ponds_in_batches(conn: sqlite3.Connection) -> None:
    batch_index = 0
    while True:
        last_id = int(_meta_get(conn, "last_copied_id") or 0)
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            schema.V3_COPY_SQL,
            {"default_species": "未指定品种", "last_id": last_id,
             "batch_size": COPY_BATCH_SIZE},
        )
        moved = cur.rowcount
        new_last = conn.execute("SELECT COALESCE(MAX(id), 0) FROM ponds__new").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM ponds__new").fetchone()[0]
        _meta_put(conn, "last_copied_id", new_last)
        _meta_put(conn, "rows_copied", total)
        conn.commit()
        _heartbeat(conn)
        on_copy_batch(moved=moved, last_id=new_last, total=total, batch_index=batch_index)
        if moved == 0:
            break
        batch_index += 1
        if STEP_SLEEP_SECONDS:
            time.sleep(STEP_SLEEP_SECONDS)
        _fault("after_batch", {"batch_index": batch_index, "moved": moved})


def on_copy_batch(moved: int, last_id: int, total: int, batch_index: int) -> None:
    """每搬完一批的回调钩子（默认空实现，测试可替换以观测搬运量）。"""
    return None


def _finalize_v3(conn: sqlite3.Connection) -> None:
    """结构已是 v3：清旧表、跑不变量、写版本号。不变量失败转人工。"""
    digest, _ = schema.schema_fingerprint(conn)
    if digest != schema.EXPECTED_FINGERPRINTS[3]:
        raise MigrationBlocked("最终化阶段：结构摘要不匹配 v3")
    conn.execute("BEGIN IMMEDIATE")
    conn.execute("DROP TABLE IF EXISTS ponds__old")
    for name, sql in schema.V3_INVARIANTS:
        bad = conn.execute(sql).fetchall()
        if bad:
            conn.rollback()
            raise MigrationBlocked(
                f"v3 升级后不变量被违反（{name}），示例: {[tuple(r) for r in bad[:5]]}"
            )
    _meta_put(conn, "version", 3)
    _meta_delete(
        conn, "migration_target", "phase", "last_copied_id", "rows_copied",
        "blocked_reason",
    )
    conn.commit()
    _fault("phase", {"phase": "invariants"})


_STEPS = {
    2: _migrate_v1_to_v2,
    3: _migrate_v2_to_v3,
}


# ---------------------------------------------------------------------------
# 对外入口
# ---------------------------------------------------------------------------

def startup_migrate(engine: Any, wait: Optional[float] = None) -> StartupResult:
    """启动时迁移。同一数据库只有一个进程真正执行，其余进程等待。

    返回：
      * ``ready``   —— 结构已在 CURRENT_VERSION，可接流量；
      * ``waiting`` —— 等待超时，对端仍在迁移（调用方应拒绝流量并后台继续等）；
      * ``blocked`` —— 需人工介入；
      * ``future_version`` —— 数据库版本比程序新，禁止降级。
    """
    path = sqlite_path_from_engine(engine)
    wait = WAIT_TIMEOUT_SECONDS if wait is None else wait

    try:
        conn, owner = _acquire_lock(path, CURRENT_VERSION, wait=wait)
    except MigrationBlocked as exc:
        return StartupResult(state="blocked", detail=str(exc))
    except MigrationWaitTimeout as exc:
        status = inspect_status(engine)
        if status["schema"] == "future_version":
            return StartupResult(state="future_version", detail=status["detail"])
        return StartupResult(
            state="waiting",
            version=status.get("version"),
            lock_owner=status.get("lock_owner", ""),
            detail=str(exc),
        )

    try:
        # 未来版本优先判定，任何情况下都不得降级启动
        version = _detect_version(conn)
        if version > CURRENT_VERSION:
            reason = (
                f"数据库结构版本 v{version} 新于本程序支持的最高版本 "
                f"v{CURRENT_VERSION}，禁止降级启动"
            )
            _mark_blocked(conn, reason)
            return StartupResult(
                state="future_version", version=version, detail=reason
            )

        if version == 0:
            _init_fresh(conn)
            return StartupResult(state="ready", version=CURRENT_VERSION, migrator=True,
                                 detail="全新库直接初始化为当前版本")

        # 断点可能停在任意中间阶段：先处理“结构已是 v3 但版本号未落盘”
        digest = schema.schema_fingerprint(conn)[0]
        if (
            version < CURRENT_VERSION
            and digest == schema.EXPECTED_FINGERPRINTS[3]
        ):
            _finalize_v3(conn)
            version = 3

        applied = False
        while version < CURRENT_VERSION:
            expected_predecessor = schema.VERSION_INFO[version + 1]["predecessor"]
            if expected_predecessor is not None and expected_predecessor != version:
                raise MigrationBlocked(
                    f"无法从 v{version} 跳到 v{version + 1}"
                    f"（该版本前置版本为 v{expected_predecessor}）"
                )
            _STEPS[version + 1](conn, path)
            applied = True
            version = int(_meta_get(conn, "version"))

        # 启动即校验当前版本结构摘要
        _expect_fingerprint(conn, CURRENT_VERSION, "启动结构校验")
        return StartupResult(
            state="ready",
            version=CURRENT_VERSION,
            migrator=applied,
            detail="完成迁移" if applied else "结构已是最新",
        )
    except FutureVersionError as exc:
        return StartupResult(state="future_version", detail=str(exc))
    except MigrationCrash as exc:
        # 进程内故障注入（生产上等价于进程被杀）：已提交的断点保留，
        # 下次启动从断点继续；不标记 blocked。
        return StartupResult(state="interrupted", detail=str(exc))
    except MigrationBlocked as exc:
        try:
            _mark_blocked(conn, str(exc))
        except sqlite3.Error:
            pass
        return StartupResult(state="blocked", detail=str(exc))
    finally:
        try:
            _release_lock(conn)
            conn.close()
        except sqlite3.Error:
            pass


def inspect_status(engine: Any) -> Dict[str, Any]:
    """供健康检查使用的只读状态。"""
    path = sqlite_path_from_engine(engine)
    result: Dict[str, Any] = {
        "database": "down",
        "schema": "unknown",
        "version": None,
        "target": CURRENT_VERSION,
        "detail": "",
        "lock_owner": "",
    }
    conn = None
    try:
        conn = _connect(path)
        result["database"] = "up"
        conn.execute("SELECT 1")
        _ensure_meta_tables(conn)
        raw = _meta_get(conn, "version")
        lock = conn.execute(
            "SELECT * FROM schema_migration_lock WHERE id=1"
        ).fetchone()

        tables = _business_tables(conn)
        if raw is None and not tables:
            result.update(schema="uninitialized", detail="数据库为空，尚未初始化")
            return result
        if raw is None:
            version = _detect_version(conn)  # 历史库或抛 blocked
            result["version"] = version
        else:
            version = int(raw)
            result["version"] = version

        if version > CURRENT_VERSION:
            result.update(
                schema="future_version",
                detail=(
                    f"数据库结构版本 v{version} 新于本程序支持的最高版本 "
                    f"v{CURRENT_VERSION}，禁止降级启动"
                ),
            )
            return result

        if lock is not None and lock["state"] == "blocked":
            digest, _ = schema.schema_fingerprint(conn)
            healthy = (
                version == CURRENT_VERSION
                and digest == schema.EXPECTED_FINGERPRINTS[CURRENT_VERSION]
            )
            if healthy:
                # 与启动路径一致：问题已被就地修复，陈旧 blocked 锁不再代表故障
                result.update(schema="ready", detail="结构就绪（blocked 锁将在启动时清除）")
            else:
                result.update(
                    schema="blocked",
                    detail=_meta_get(conn, "blocked_reason") or lock["note"],
                    lock_owner=lock["owner"],
                )
            return result

        digest, _ = schema.schema_fingerprint(conn)
        phase = _meta_get(conn, "phase")
        if lock is not None and lock["state"] == "running":
            age = (
                datetime.now(timezone.utc)
                - datetime.fromisoformat(lock["heartbeat"])
            ).total_seconds()
            result["lock_owner"] = lock["owner"]
            if age > LOCK_STALE_SECONDS:
                result.update(
                    schema="blocked",
                    detail=f"迁移者心跳已过期 {age:.0f}s，等待接管",
                )
            else:
                result.update(
                    schema="migrating",
                    detail=f"正在迁移到 v{lock['target_version']}"
                    f"（阶段 {phase or '-'}）",
                )
            return result

        if version == CURRENT_VERSION and digest == schema.EXPECTED_FINGERPRINTS[3]:
            result.update(schema="ready", detail="结构就绪")
        else:
            reason = _meta_get(conn, "blocked_reason")
            result.update(
                schema="blocked",
                detail=reason or "结构摘要与已登记版本不一致，需人工介入",
            )
    except MigrationBlocked as exc:
        result.update(schema="blocked", detail=str(exc))
    except sqlite3.Error as exc:
        result.update(database="down", detail=f"数据库不可用: {exc}")
    finally:
        if conn is not None:
            conn.close()
    return result
