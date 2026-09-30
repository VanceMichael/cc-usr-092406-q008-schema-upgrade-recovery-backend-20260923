"""结构版本定义。

每一个 :class:`MigrationVersion` 明确四件事：

1. ``predecessor`` —— 唯一允许的前置版本（升级链固定为 1→2→3，不允许跳级执行）；
2. ``schema`` —— 升级后期望的完整 :class:`SchemaSpec`，其 SHA-256 即该校验摘要；
3. ``steps`` —— 有序、可断点续传的升级步骤（普通 DDL 或复制型重建）；
4. ``invariants`` —— 升级后必须成立的数据不变量（SQL 形式，返回违规行数必须为 0）。

版本历史
--------

* v1：初版结构（SQLAlchemy ``create_all`` 产物，无版本记账表）。
* v2：纯增量结构。
    - ``ponds.location_code`` 新增并按既有主键回填 ``P`` + 8 位补零编码；
    - ``feeding_records.feed_batch_number`` 新增；
    - ``feeding_records(batch_id, feeding_date)``、
      ``cost_records(batch_id, cost_date)`` 新增复合索引。
* v3：SQLite 无法直接完成的表变更，全部走“建影子表 → 搬运 → 校验 → 切换”：
    - ``ponds.name`` 收紧为 ``VARCHAR(50) NOT NULL``，
      ``ponds.location_code`` 收紧为 ``NOT NULL``；
    - ``feeding_records.feed_quantity`` 由 ``FLOAT`` 改为 ``NUMERIC(10,2)``；
    - ``cost_records.cost_type`` 收紧为 ``VARCHAR(30) NOT NULL``。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from .schema import Column, ForeignKey, Index, SchemaSpec, TableSpec

# ---------------------------------------------------------------------------
# 构造辅助
# ---------------------------------------------------------------------------


def C(name, type, notnull=0, default=None, pk=0) -> Column:
    return Column(name, type, notnull, default, pk)


def I(name, columns, unique=0) -> Index:
    return Index(name, tuple(columns), unique, "c")


def FK(column, ref_table, ref_column="id") -> ForeignKey:
    return ForeignKey(column, ref_table, ref_column, "NO ACTION", "NO ACTION")


def _replace_columns(table: TableSpec, changes: Dict[str, Optional[Column]],
                     append: Tuple[Column, ...] = ()) -> TableSpec:
    """按名称替换/删除列（changes[name]=None 表示删除），并可追加新列。"""
    cols: List[Column] = []
    for col in table.columns:
        if col.name in changes:
            new_col = changes[col.name]
            if new_col is not None:
                cols.append(new_col)
        else:
            cols.append(col)
    cols.extend(append)
    return TableSpec(table.name, tuple(cols), table.indexes, table.foreign_keys)


# ---------------------------------------------------------------------------
# v1 —— 初版（与旧版 SQLAlchemy create_all 产物逐项一致）
# ---------------------------------------------------------------------------

_PONDS_V1 = TableSpec(
    "ponds",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("name", "VARCHAR(100)", notnull=1),
        C("area", "FLOAT", notnull=1),
        C("water_depth", "FLOAT", notnull=1),
        C("species", "VARCHAR(100)"),
        C("status", "VARCHAR(20)"),
        C("created_at", "DATETIME"),
        C("updated_at", "DATETIME"),
    ),
    (I("ix_ponds_id", ("id",)), I("ix_ponds_name", ("name",), unique=1)),
    (),
)

_BATCHES_V1 = TableSpec(
    "batches",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("batch_number", "VARCHAR(50)", notnull=1),
        C("pond_id", "INTEGER", notnull=1),
        C("species", "VARCHAR(100)", notnull=1),
        C("stocking_date", "DATE", notnull=1),
        C("estimated_harvest_date", "DATE"),
        C("actual_harvest_date", "DATE"),
        C("status", "VARCHAR(20)"),
        C("created_at", "DATETIME"),
        C("updated_at", "DATETIME"),
    ),
    (I("ix_batches_id", ("id",)), I("ix_batches_batch_number", ("batch_number",), unique=1)),
    (FK("pond_id", "ponds"),),
)

_STOCKING_V1 = TableSpec(
    "stocking_records",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("batch_id", "INTEGER", notnull=1),
        C("species", "VARCHAR(100)", notnull=1),
        C("quantity", "INTEGER", notnull=1),
        C("source", "VARCHAR(200)"),
        C("batch_number", "VARCHAR(50)"),
        C("weight_per_unit", "FLOAT"),
        C("total_weight", "FLOAT"),
        C("notes", "TEXT"),
        C("created_at", "DATETIME"),
    ),
    (I("ix_stocking_records_id", ("id",)),),
    (FK("batch_id", "batches"),),
)

_FEEDING_V1 = TableSpec(
    "feeding_records",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("batch_id", "INTEGER", notnull=1),
        C("feeding_date", "DATE", notnull=1),
        C("feed_type", "VARCHAR(100)", notnull=1),
        C("feed_quantity", "FLOAT", notnull=1),
        C("feeding_time", "VARCHAR(20)"),
        C("weather", "VARCHAR(50)"),
        C("water_temperature", "FLOAT"),
        C("notes", "TEXT"),
        C("created_at", "DATETIME"),
    ),
    (I("ix_feeding_records_id", ("id",)),),
    (FK("batch_id", "batches"),),
)

_WATER_QUALITY_V1 = TableSpec(
    "water_quality_records",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("batch_id", "INTEGER", notnull=1),
        C("record_date", "DATE", notnull=1),
        C("record_time", "VARCHAR(20)"),
        C("water_temperature", "FLOAT"),
        C("ph_value", "FLOAT"),
        C("dissolved_oxygen", "FLOAT"),
        C("ammonia_nitrogen", "FLOAT"),
        C("nitrite", "FLOAT"),
        C("transparency", "FLOAT"),
        C("notes", "TEXT"),
        C("created_at", "DATETIME"),
    ),
    (I("ix_water_quality_records_id", ("id",)),),
    (FK("batch_id", "batches"),),
)

_MEDICATION_V1 = TableSpec(
    "medication_records",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("batch_id", "INTEGER", notnull=1),
        C("medication_date", "DATE", notnull=1),
        C("drug_name", "VARCHAR(200)", notnull=1),
        C("drug_type", "VARCHAR(50)"),
        C("dosage", "FLOAT"),
        C("dosage_unit", "VARCHAR(20)"),
        C("administration_method", "VARCHAR(100)"),
        C("purpose", "VARCHAR(200)"),
        C("manufacturer", "VARCHAR(200)"),
        C("batch_number", "VARCHAR(50)"),
        C("notes", "TEXT"),
        C("created_at", "DATETIME"),
    ),
    (I("ix_medication_records_id", ("id",)),),
    (FK("batch_id", "batches"),),
)

_COST_V1 = TableSpec(
    "cost_records",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("batch_id", "INTEGER", notnull=1),
        C("cost_date", "DATE", notnull=1),
        C("cost_type", "VARCHAR(50)", notnull=1),
        C("amount", "FLOAT", notnull=1),
        C("description", "VARCHAR(500)"),
        C("quantity", "FLOAT"),
        C("unit", "VARCHAR(20)"),
        C("unit_price", "FLOAT"),
        C("notes", "TEXT"),
        C("created_at", "DATETIME"),
    ),
    (I("ix_cost_records_id", ("id",)),),
    (FK("batch_id", "batches"),),
)

_HARVEST_V1 = TableSpec(
    "harvest_sales",
    (
        C("id", "INTEGER", notnull=1, pk=1),
        C("batch_id", "INTEGER", notnull=1),
        C("sale_date", "DATE", notnull=1),
        C("weight", "FLOAT", notnull=1),
        C("unit_price", "FLOAT", notnull=1),
        C("total_amount", "FLOAT"),
        C("buyer", "VARCHAR(200)"),
        C("batch_number", "VARCHAR(50)"),
        C("quality_grade", "VARCHAR(50)"),
        C("notes", "TEXT"),
        C("created_at", "DATETIME"),
    ),
    (I("ix_harvest_sales_id", ("id",)),),
    (FK("batch_id", "batches"),),
)

_ALL_V1 = (
    _BATCHES_V1, _COST_V1, _FEEDING_V1, _HARVEST_V1, _MEDICATION_V1,
    _PONDS_V1, _STOCKING_V1, _WATER_QUALITY_V1,
)

SPEC_V1 = SchemaSpec(1, _ALL_V1)

# ---------------------------------------------------------------------------
# v2 —— 纯增量（ADD COLUMN / CREATE INDEX 均为 SQLite 原生支持的可回滚 DDL）
# ---------------------------------------------------------------------------

_PONDS_V2 = _replace_columns(_PONDS_V1, {}, append=(C("location_code", "VARCHAR(20)"),))
_PONDS_V2 = TableSpec(
    _PONDS_V2.name,
    _PONDS_V2.columns,
    tuple(sorted(
        _PONDS_V2.indexes + (I("ix_ponds_location_code", ("location_code",), unique=1),),
        key=lambda i: i.name,
    )),
    _PONDS_V2.foreign_keys,
)

_FEEDING_V2 = _replace_columns(
    _FEEDING_V1, {}, append=(C("feed_batch_number", "VARCHAR(50)"),)
)
_FEEDING_V2 = TableSpec(
    _FEEDING_V2.name,
    _FEEDING_V2.columns,
    tuple(sorted(
        _FEEDING_V2.indexes + (I("ix_feeding_records_batch_date", ("batch_id", "feeding_date")),),
        key=lambda i: i.name,
    )),
    _FEEDING_V2.foreign_keys,
)

_COST_V2 = TableSpec(
    _COST_V1.name,
    _COST_V1.columns,
    tuple(sorted(
        _COST_V1.indexes + (I("ix_cost_records_batch_date", ("batch_id", "cost_date")),),
        key=lambda i: i.name,
    )),
    _COST_V1.foreign_keys,
)

SPEC_V2 = SchemaSpec(
    2,
    tuple(sorted(
        tuple(t for t in SPEC_V1.tables
              if t.name not in ("ponds", "feeding_records", "cost_records"))
        + (_COST_V2, _FEEDING_V2, _PONDS_V2),
        key=lambda t: t.name,
    )),
)

# ---------------------------------------------------------------------------
# v3 —— 重建型变更（SQLite 不支持改列类型/约束，复制到影子表后切换）
# ---------------------------------------------------------------------------

_PONDS_V3 = _replace_columns(
    _PONDS_V2,
    {
        "name": C("name", "VARCHAR(50)", notnull=1),
        "location_code": C("location_code", "VARCHAR(20)", notnull=1),
    },
)

_FEEDING_V3 = _replace_columns(
    _FEEDING_V2,
    {"feed_quantity": C("feed_quantity", "NUMERIC(10,2)", notnull=1)},
)

_COST_V3 = _replace_columns(
    _COST_V2,
    {"cost_type": C("cost_type", "VARCHAR(30)", notnull=1)},
)

SPEC_V3 = SchemaSpec(
    3,
    tuple(sorted(
        tuple(t for t in SPEC_V2.tables
              if t.name not in ("ponds", "feeding_records", "cost_records"))
        + (_COST_V3, _FEEDING_V3, _PONDS_V3),
        key=lambda t: t.name,
    )),
)

# ---------------------------------------------------------------------------
# 升级步骤定义
# ---------------------------------------------------------------------------


@dataclass
class SqlStep:
    """单条或多条顺序执行的原生 DDL/DML（包在一个事务里，失败整体回滚）。"""

    name: str
    statements: Tuple[str, ...]


@dataclass
class RebuildStep:
    """复制型表重建。

    引擎按四阶段执行并逐阶段记账，中断后按影子表实况决定续跑：
    ``prepare``（建影子表与索引）→ ``copy``（INSERT … SELECT 搬运）→
    ``verify``（行数与逐行摘要比对）→ ``switch``（DROP 旧表 + 改名，单事务）。

    搬运列取源表与目标表同名列的交集，并按目标表列序排列——v3 只做类型/约束
    收紧，不新增列，因此所有目标列都能在源表中找到。
    """

    name: str
    table: TableSpec


def _v2_steps() -> Tuple[object, ...]:
    return (
        SqlStep(
            "add_ponds_location_code",
            ('ALTER TABLE ponds ADD COLUMN location_code VARCHAR(20)',),
        ),
        SqlStep(
            "backfill_ponds_location_code",
            # 既有行按主键生成 P + 8 位补零编码（8 位可覆盖 1 亿以内主键且不撞号），
            # 仅回填仍为 NULL 的行（可重复执行）。应用侧新建塘口使用同一编码规则。
            (("UPDATE ponds SET location_code = 'P' || substr('00000000' || id, -8, 8) "
              "WHERE location_code IS NULL"),),
        ),
        SqlStep(
            "create_ponds_location_code_index",
            ("CREATE UNIQUE INDEX ix_ponds_location_code ON ponds (location_code)",),
        ),
        SqlStep(
            "add_feeding_feed_batch_number",
            ('ALTER TABLE feeding_records ADD COLUMN feed_batch_number VARCHAR(50)',),
        ),
        SqlStep(
            "create_feeding_batch_date_index",
            ('CREATE INDEX ix_feeding_records_batch_date '
             'ON feeding_records (batch_id, feeding_date)',),
        ),
        SqlStep(
            "create_cost_batch_date_index",
            ('CREATE INDEX ix_cost_records_batch_date '
             'ON cost_records (batch_id, cost_date)',),
        ),
    )


def _v3_steps() -> Tuple[object, ...]:
    return (
        RebuildStep("rebuild_ponds_v3", _PONDS_V3),
        RebuildStep("rebuild_feeding_records_v3", _FEEDING_V3),
        RebuildStep("rebuild_cost_records_v3", _COST_V3),
    )


# ---------------------------------------------------------------------------
# 升级后不变量：每项返回 (说明, SQL)；SQL 必须返回违规行数，且结果为 0
# ---------------------------------------------------------------------------

_INVARIANT_COUNTS = "__row_counts_snapshot__"

_INVARIANTS_V2: Tuple[Tuple[str, str], ...] = (
    (
        "ponds.location_code 必须全部按 P + 8 位数字回填",
        "SELECT COUNT(*) FROM ponds WHERE location_code IS NULL "
        "OR location_code NOT GLOB 'P[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'",
    ),
    (
        "ponds.location_code 不允许重复",
        "SELECT COUNT(*) FROM (SELECT location_code FROM ponds "
        "GROUP BY location_code HAVING COUNT(*) > 1)",
    ),
)

_INVARIANTS_V3: Tuple[Tuple[str, str], ...] = (
    (
        "ponds.name 收紧后长度不得超过 50",
        "SELECT COUNT(*) FROM ponds WHERE name IS NULL OR length(name) > 50",
    ),
    (
        "ponds.location_code 必须保持 P + 8 位数字且非空",
        "SELECT COUNT(*) FROM ponds WHERE location_code IS NULL "
        "OR location_code NOT GLOB 'P[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'",
    ),
    (
        "feeding_records.feed_quantity 不允许为空",
        "SELECT COUNT(*) FROM feeding_records WHERE feed_quantity IS NULL",
    ),
    (
        "cost_records.cost_type 长度不得超过 30 且非空",
        "SELECT COUNT(*) FROM cost_records "
        "WHERE cost_type IS NULL OR length(cost_type) > 30",
    ),
)


@dataclass
class MigrationVersion:
    version: int
    predecessor: int
    schema: SchemaSpec
    steps: Tuple[object, ...]
    invariants: Tuple[Tuple[str, str], ...] = ()
    description: str = ""

    @property
    def checksum(self) -> str:
        return self.schema_digest()

    def schema_digest(self) -> str:
        from .schema import schema_digest
        return schema_digest(self.schema)


VERSIONS: Dict[int, MigrationVersion] = {
    1: MigrationVersion(1, 0, SPEC_V1, (), (), "初版结构（无版本记账）"),
    2: MigrationVersion(2, 1, SPEC_V2, _v2_steps(), _INVARIANTS_V2,
                        "塘口编码与投喂/成本索引"),
    3: MigrationVersion(3, 2, SPEC_V3, _v3_steps(), _INVARIANTS_V3,
                        "列类型与约束收紧（复制重建）"),
}

CURRENT_VERSION = 3


def version_chain(from_version: int, to: int = CURRENT_VERSION) -> List[MigrationVersion]:
    """返回严格按 predecessor 链接的升级序列；任何跳级都会报错。"""
    chain: List[MigrationVersion] = []
    cur = to
    while cur > from_version:
        mv = VERSIONS[cur]
        if mv.predecessor != cur - 1:
            raise ValueError(f"版本 {cur} 的前置版本不是 {cur - 1}")
        chain.append(mv)
        cur = mv.predecessor
    chain.reverse()
    return chain
