"""
Shared test utilities for running main/summary_inc.py in local docker.
"""

import calendar
import json
import os
import sys
import types
from datetime import date, datetime
from typing import Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Import fallbacks for main/summary_inc.py dependencies that are unused in tests
# ---------------------------------------------------------------------------

try:
    import boto3  # noqa: F401
except Exception:
    boto3_stub = types.ModuleType("boto3")

    def _unsupported_client(*args, **kwargs):
        raise RuntimeError("boto3 client should not be used in local docker tests")

    boto3_stub.client = _unsupported_client
    sys.modules["boto3"] = boto3_stub


try:
    from dateutil.relativedelta import relativedelta  # noqa: F401
except Exception:
    dateutil_mod = types.ModuleType("dateutil")
    relativedelta_mod = types.ModuleType("dateutil.relativedelta")

    class relativedelta:  # type: ignore
        def __init__(self, months: int = 0):
            self.months = months

        def __radd__(self, other):
            total_month = (other.year * 12 + (other.month - 1)) + self.months
            year = total_month // 12
            month = (total_month % 12) + 1
            day = min(other.day, calendar.monthrange(year, month)[1])
            return other.replace(year=year, month=month, day=day)

    relativedelta_mod.relativedelta = relativedelta
    dateutil_mod.relativedelta = relativedelta_mod
    sys.modules["dateutil"] = dateutil_mod
    sys.modules["dateutil.relativedelta"] = relativedelta_mod


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
MAIN_DIR = os.path.join(ROOT_DIR, "main")

if MAIN_DIR not in sys.path:
    sys.path.insert(0, MAIN_DIR)

import summary_inc as main_pipeline  # noqa: E402


HISTORY_LENGTH = 36

SOURCE_SCHEMA_SPEC = [
    ("cons_acct_key", "BIGINT"),
    ("bureau_mbr_id", "STRING"),
    ("port_type_cd", "STRING"),
    ("acct_type_dtl_cd", "STRING"),
    ("acct_open_dt", "DATE"),
    ("acct_closed_dt", "DATE"),
    ("pymt_terms_cd", "STRING"),
    ("pymt_terms_dtl_cd", "STRING"),
    ("acct_dt", "DATE"),
    ("acct_stat_cd", "STRING"),
    ("acct_pymt_stat_cd", "STRING"),
    ("acct_pymt_stat_dtl_cd", "STRING"),
    ("acct_credit_ext_am", "INT"),
    ("acct_bal_am", "INT"),
    ("past_due_am", "INT"),
    ("actual_pymt_am", "INT"),
    ("last_pymt_dt", "DATE"),
    ("schd_pymt_dt", "DATE"),
    ("next_schd_pymt_am", "INT"),
    ("collateral_cd", "STRING"),
    ("orig_pymt_due_dt", "DATE"),
    ("write_off_dt", "DATE"),
    ("write_off_am", "INT"),
    ("asset_class_cd_4in", "STRING"),
    ("days_past_due_ct_4in", "INT"),
    ("high_credit_am_4in", "INT"),
    ("cash_limit_am_4in", "INT"),
    ("collateral_am_4in", "INT"),
    ("total_write_off_am_4in", "INT"),
    ("principal_write_off_am_4in", "INT"),
    ("settled_am_4in", "INT"),
    ("interest_rate_4in", "INT"),
    ("suit_filed_wilful_def_stat_cd_4in", "STRING"),
    ("wo_settled_stat_cd_4in", "STRING"),
    ("soft_del_cd", "STRING"),
    ("base_ts", "TIMESTAMP"),
    ("rpt_as_of_mo", "STRING"),
    ("insert_ts", "TIMESTAMP"),
    ("update_ts", "TIMESTAMP"),
]

SUMMARY_SCHEMA_SPEC = [
    ("cons_acct_key", "BIGINT"),
    ("bureau_member_id", "STRING"),
    ("portfolio_rating_type_cd", "STRING"),
    ("acct_type_dtl_cd", "STRING"),
    ("open_dt", "DATE"),
    ("closed_dt", "DATE"),
    ("pymt_terms_cd", "STRING"),
    ("pymt_terms_dtl_cd", "STRING"),
    ("acct_dt", "DATE"),
    ("acct_stat_cd", "STRING"),
    ("acct_pymt_stat_cd", "STRING"),
    ("acct_pymt_stat_dtl_cd", "STRING"),
    ("credit_limit_am", "INT"),
    ("balance_am", "INT"),
    ("past_due_am", "INT"),
    ("actual_payment_am", "INT"),
    ("last_payment_dt", "DATE"),
    ("schd_pymt_dt", "DATE"),
    ("emi_amt", "INT"),
    ("collateral_cd", "STRING"),
    ("orig_pymt_due_dt", "DATE"),
    ("dflt_status_dt", "DATE"),
    ("write_off_am", "INT"),
    ("asset_class_cd", "STRING"),
    ("days_past_due", "INT"),
    ("hi_credit_am", "INT"),
    ("cash_limit_am", "INT"),
    ("collateral_am", "INT"),
    ("charge_off_am", "INT"),
    ("principal_write_off_am", "INT"),
    ("settled_am", "INT"),
    ("interest_rate", "INT"),
    ("suit_filed_willful_dflt", "STRING"),
    ("written_off_and_settled_status", "STRING"),
    ("soft_del_cd", "STRING"),
    ("base_ts", "TIMESTAMP"),
    ("rpt_as_of_mo", "STRING"),
    ("orig_loan_am", "INT"),
    ("payment_rating_cd", "STRING"),
    ("actual_payment_am_history", "ARRAY<INT>"),
    ("balance_am_history", "ARRAY<INT>"),
    ("credit_limit_am_history", "ARRAY<INT>"),
    ("past_due_am_history", "ARRAY<INT>"),
    ("payment_rating_cd_history", "ARRAY<STRING>"),
    ("days_past_due_history", "ARRAY<INT>"),
    ("asset_class_cd_4in_history", "ARRAY<STRING>"),
    ("payment_history_grid", "STRING"),
]

LATEST_SUMMARY_SCHEMA_SPEC = list(SUMMARY_SCHEMA_SPEC)


def _spark_type(sql_type: str):
    upper = sql_type.upper()
    if upper == "STRING":
        return StringType()
    if upper == "INT":
        return IntegerType()
    if upper == "BIGINT":
        return LongType()
    if upper == "DATE":
        return DateType()
    if upper == "TIMESTAMP":
        return TimestampType()
    if upper == "ARRAY<INT>":
        return ArrayType(IntegerType())
    if upper == "ARRAY<STRING>":
        return ArrayType(StringType())
    raise ValueError(f"Unsupported SQL type: {sql_type}")


def _build_schema(spec: List[tuple]) -> StructType:
    return StructType([StructField(name, _spark_type(sql_type), True) for name, sql_type in spec])


SOURCE_SCHEMA = _build_schema(SOURCE_SCHEMA_SPEC)
SUMMARY_SCHEMA = _build_schema(SUMMARY_SCHEMA_SPEC)
LATEST_SUMMARY_SCHEMA = _build_schema(LATEST_SUMMARY_SCHEMA_SPEC)


def create_spark_session(app_name: str) -> SparkSession:
    execution_catalog_warehouse = os.environ.get("TEST_execution_catalog_WAREHOUSE", "").strip()
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.primary_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.primary_catalog.type", "rest")
        .config("spark.sql.catalog.primary_catalog.uri", "http://rest:8181")
        .config("spark.sql.catalog.primary_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.primary_catalog.warehouse", "s3://warehouse/")
        .config("spark.sql.catalog.primary_catalog.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.execution_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.execution_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.execution_catalog.s3.endpoint", "http://minio:9000")
        .config("spark.sql.defaultCatalog", "primary_catalog")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.iceberg.planning.preserve-data-grouping", "true")
        .config("spark.sql.sources.v2.bucketing.enabled", "true")
        .config("spark.sql.sources.v2.bucketing.pushPartValues.enabled", "true")
        .config("spark.sql.requireAllClusterKeysForDistribution", "false")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
    )
    if execution_catalog_warehouse:
        builder = (
            builder
            .config("spark.sql.catalog.execution_catalog.type", "hadoop")
            .config("spark.sql.catalog.execution_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
            .config("spark.sql.catalog.execution_catalog.warehouse", execution_catalog_warehouse)
        )
    else:
        builder = (
            builder
            .config("spark.sql.catalog.execution_catalog.type", "rest")
            .config("spark.sql.catalog.execution_catalog.uri", "http://rest:8181")
            .config("spark.sql.catalog.execution_catalog.warehouse", "s3://warehouse/")
        )

    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def load_main_test_config(namespace: str) -> Dict:
    namespace = os.environ.get("TEST_NAMESPACE_OVERRIDE", namespace).strip() or namespace
    test_catalog = os.environ.get("TEST_PRIMARY_CATALOG", "primary_catalog").strip() or "primary_catalog"
    config_path = os.path.join(MAIN_DIR, "config.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    config["source_table"] = f"{test_catalog}.{namespace}.accounts_all"
    config["destination_table"] = f"{test_catalog}.{namespace}.summary"
    config["latest_history_table"] = f"{test_catalog}.{namespace}.latest_summary"
    config["hist_rpt_dt_table"] = f"{test_catalog}.{namespace}.consumer_account_hist_rpt"

    config["spark"] = {
        "app_name": f"SummaryMainDocker_{namespace}",
        "spark.sql.shuffle.partitions": "32",
        "spark.default.parallelism": "32",
        "spark.sql.iceberg.planning.preserve-data-grouping": "true",
        "spark.sql.sources.v2.bucketing.enabled": "true",
        "spark.sql.sources.v2.bucketing.pushPartValues.enabled": "true",
        "spark.sql.requireAllClusterKeysForDistribution": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
    }

    config.setdefault("performance", {})
    config["performance"].setdefault("target_records_per_partition", 50000)
    config["performance"].setdefault("min_partitions", 8)
    config["performance"].setdefault("max_partitions", 256)

    return config


def _schema_sql(spec: List[tuple]) -> str:
    return ",\n            ".join([f"{name} {sql_type}" for name, sql_type in spec])


def _get_reset_targets(config: Dict):
    source_table = config["source_table"]
    summary_table = config["destination_table"]
    latest_table = config["latest_history_table"]
    hist_rpt_dt_table = config["hist_rpt_dt_table"]
    tracker_table = (
        config.get("watermark_tracker_table")
        or main_pipeline.get_watermark_tracker_table(config)
    )
    return source_table, summary_table, latest_table, hist_rpt_dt_table, tracker_table


def _temp_case_tables() -> List[str]:
    return ["case_1", "case_2", "case_3a", "case_3b", "case_3d_month", "case_3d_future", "case_4"]


def _ensure_base_tables(spark: SparkSession, config: Dict):
    source_table, summary_table, latest_table, hist_rpt_dt_table, tracker_table = _get_reset_targets(config)

    namespace = source_table.rsplit(".", 1)[0]

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS execution_catalog.checkpointdb")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {source_table} (
            {_schema_sql(SOURCE_SCHEMA_SPEC)}
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo)
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {summary_table} (
            {_schema_sql(SUMMARY_SCHEMA_SPEC)}
        )
        USING iceberg
        PARTITIONED BY (rpt_as_of_mo, bucket(64, cons_acct_key))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {latest_table} (
            {_schema_sql(LATEST_SUMMARY_SCHEMA_SPEC)}
        )
        USING iceberg
        PARTITIONED BY (bucket(64, cons_acct_key))
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {hist_rpt_dt_table} (
            cons_acct_key BIGINT,
            soft_del_cd STRING,
            acct_dt DATE,
            base_ts TIMESTAMP,
            insert_dt TIMESTAMP,
            update_dt TIMESTAMP,
            insert_time STRING,
            update_time STRING
        )
        USING iceberg
        """
    )

    if hasattr(main_pipeline, "_ensure_watermark_tracker_table"):
        main_pipeline._ensure_watermark_tracker_table(spark, tracker_table)
    else:
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {tracker_table} (
                source_name STRING,
                source_table STRING,
                max_base_ts TIMESTAMP,
                max_rpt_as_of_mo STRING,
                updated_at TIMESTAMP
            )
            USING iceberg
            """
        )


def precreate_tables(spark: SparkSession, config: Dict):
    _ensure_base_tables(spark, config)


def reset_tables(spark: SparkSession, config: Dict):
    source_table, summary_table, latest_table, hist_rpt_dt_table, tracker_table = _get_reset_targets(config)
    use_precreated = os.environ.get("TEST_USE_PRECREATED_TABLES", "").strip() == "1"
    assume_precreated_empty = os.environ.get("TEST_PRECREATED_ASSUME_EMPTY", "").strip() == "1"

    if not use_precreated:
        for table in [source_table, summary_table, latest_table, hist_rpt_dt_table, tracker_table]:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
        for temp_case in _temp_case_tables():
            spark.sql(f"DROP TABLE IF EXISTS execution_catalog.checkpointdb.{temp_case}")
        _ensure_base_tables(spark, config)
    else:
        _ensure_base_tables(spark, config)
        for temp_case in _temp_case_tables():
            spark.sql(f"DROP TABLE IF EXISTS execution_catalog.checkpointdb.{temp_case}")
        if not assume_precreated_empty:
            for table in [source_table, summary_table, latest_table, hist_rpt_dt_table, tracker_table]:
                spark.sql(f"DELETE FROM {table} WHERE 1 = 1")

    if not (use_precreated and assume_precreated_empty):
        spark.sql(f"DELETE FROM {tracker_table} WHERE 1 = 1")


def month_start(month: str) -> date:
    year, mon = month.split("-")
    return date(int(year), int(mon), 1)


def history(positions: Dict[int, object], length: int = HISTORY_LENGTH) -> List[object]:
    arr: List[object] = [None] * length
    for idx, value in positions.items():
        arr[idx] = value
    return arr


def grid_from_history(values: List[Optional[object]], placeholder: str = "?") -> str:
    return "".join([placeholder if v is None else str(v) for v in values])


def build_source_row(
    cons_acct_key: int,
    rpt_as_of_mo: str,
    base_ts: datetime,
    balance: int,
    actual_payment: int,
    soft_del_cd: str = "",
    credit_limit: int = 10000,
    days_past_due: int = 0,
    past_due: int = 0,
    asset_class: str = "A",
    acct_type_dtl_cd: str = "5",
) -> Dict:
    record = {name: None for name, _ in SOURCE_SCHEMA_SPEC}
    as_of_dt = month_start(rpt_as_of_mo)

    record.update(
        {
            "cons_acct_key": cons_acct_key,
            "bureau_mbr_id": f"MBR{cons_acct_key}",
            "port_type_cd": "PT",
            "acct_type_dtl_cd": acct_type_dtl_cd,
            "acct_open_dt": date(as_of_dt.year - 1, as_of_dt.month, 1),
            "acct_closed_dt": None,
            "pymt_terms_cd": "01",
            "pymt_terms_dtl_cd": "M",
            "acct_dt": as_of_dt,
            "acct_stat_cd": "1",
            "acct_pymt_stat_cd": "0",
            "acct_pymt_stat_dtl_cd": "0",
            "acct_credit_ext_am": credit_limit,
            "acct_bal_am": balance,
            "past_due_am": past_due,
            "actual_pymt_am": actual_payment,
            "last_pymt_dt": None,
            "schd_pymt_dt": as_of_dt,
            "next_schd_pymt_am": actual_payment,
            "collateral_cd": "N",
            "orig_pymt_due_dt": as_of_dt,
            "write_off_dt": None,
            "write_off_am": 0,
            "asset_class_cd_4in": asset_class,
            "days_past_due_ct_4in": days_past_due,
            "high_credit_am_4in": credit_limit,
            "cash_limit_am_4in": 0,
            "collateral_am_4in": 0,
            "total_write_off_am_4in": 0,
            "principal_write_off_am_4in": 0,
            "settled_am_4in": 0,
            "interest_rate_4in": 0,
            "suit_filed_wilful_def_stat_cd_4in": "N",
            "wo_settled_stat_cd_4in": "N",
            "soft_del_cd": str(soft_del_cd),
            "base_ts": base_ts,
            "rpt_as_of_mo": rpt_as_of_mo,
            "insert_ts": base_ts,
            "update_ts": base_ts,
        }
    )

    return record


def build_summary_row(
    cons_acct_key: int,
    rpt_as_of_mo: str,
    base_ts: datetime,
    balance: int,
    actual_payment: int,
    credit_limit: int = 10000,
    days_past_due: int = 0,
    past_due: int = 0,
    asset_class: str = "A",
    payment_rating: str = "0",
    soft_del_cd: str = "",
    balance_history: Optional[List[Optional[int]]] = None,
    payment_history: Optional[List[Optional[int]]] = None,
    credit_history: Optional[List[Optional[int]]] = None,
    past_due_history: Optional[List[Optional[int]]] = None,
    rating_history: Optional[List[Optional[str]]] = None,
    dpd_history: Optional[List[Optional[int]]] = None,
    asset_history: Optional[List[Optional[str]]] = None,
) -> Dict:
    record = {name: None for name, _ in SUMMARY_SCHEMA_SPEC}
    as_of_dt = month_start(rpt_as_of_mo)

    actual_payment_history = payment_history or history({0: actual_payment})
    balance_am_history = balance_history or history({0: balance})
    credit_limit_history = credit_history or history({0: credit_limit})
    past_due_am_history = past_due_history or history({0: past_due})
    payment_rating_history = rating_history or history({0: payment_rating})
    days_past_due_history = dpd_history or history({0: days_past_due})
    asset_class_history = asset_history or history({0: asset_class})

    record.update(
        {
            "cons_acct_key": cons_acct_key,
            "bureau_member_id": f"MBR{cons_acct_key}",
            "portfolio_rating_type_cd": "PT",
            "acct_type_dtl_cd": "5",
            "open_dt": date(as_of_dt.year - 1, as_of_dt.month, 1),
            "closed_dt": None,
            "pymt_terms_cd": "01",
            "pymt_terms_dtl_cd": "M",
            "acct_dt": as_of_dt,
            "acct_stat_cd": "1",
            "acct_pymt_stat_cd": "0",
            "acct_pymt_stat_dtl_cd": "0",
            "credit_limit_am": credit_limit,
            "balance_am": balance,
            "past_due_am": past_due,
            "actual_payment_am": actual_payment,
            "last_payment_dt": None,
            "schd_pymt_dt": as_of_dt,
            "emi_amt": actual_payment,
            "collateral_cd": "N",
            "orig_pymt_due_dt": as_of_dt,
            "dflt_status_dt": None,
            "write_off_am": 0,
            "asset_class_cd": asset_class,
            "days_past_due": days_past_due,
            "hi_credit_am": credit_limit,
            "cash_limit_am": 0,
            "collateral_am": 0,
            "charge_off_am": 0,
            "principal_write_off_am": 0,
            "settled_am": 0,
            "interest_rate": 0,
            "suit_filed_willful_dflt": "N",
            "written_off_and_settled_status": "N",
            "soft_del_cd": str(soft_del_cd),
            "base_ts": base_ts,
            "rpt_as_of_mo": rpt_as_of_mo,
            "orig_loan_am": credit_limit,
            "payment_rating_cd": payment_rating,
            "actual_payment_am_history": actual_payment_history,
            "balance_am_history": balance_am_history,
            "credit_limit_am_history": credit_limit_history,
            "past_due_am_history": past_due_am_history,
            "payment_rating_cd_history": payment_rating_history,
            "days_past_due_history": days_past_due_history,
            "asset_class_cd_4in_history": asset_class_history,
            "payment_history_grid": grid_from_history(payment_rating_history),
        }
    )

    return record


def write_source_rows(spark: SparkSession, table: str, rows: List[Dict]):
    if not rows:
        return
    df = spark.createDataFrame(rows, SOURCE_SCHEMA)
    df.writeTo(table).append()


def write_summary_rows(spark: SparkSession, table: str, rows: List[Dict]):
    if not rows:
        return
    df = spark.createDataFrame(rows, SUMMARY_SCHEMA)
    target_cols = spark.table(table).columns
    existing_cols = [c for c in target_cols if c in df.columns]
    df = df.select(*existing_cols)
    df.writeTo(table).append()


def fetch_single_row(spark: SparkSession, table: str, cons_acct_key: int, rpt_as_of_mo: str):
    rows = (
        spark.table(table)
        .filter(F.col("cons_acct_key") == cons_acct_key)
        .filter(F.col("rpt_as_of_mo") == rpt_as_of_mo)
        .collect()
    )
    if len(rows) != 1:
        raise AssertionError(
            f"Expected exactly 1 row in {table} for account={cons_acct_key}, month={rpt_as_of_mo}, got {len(rows)}"
        )
    return rows[0]


def _safe_min_datetime(a: Optional[datetime], b: Optional[datetime]) -> Optional[datetime]:
    if a is None:
        return b
    if b is None:
        return a
    return a if a <= b else b


def _safe_min_month(a: Optional[str], b: Optional[str]) -> Optional[str]:
    if a is None:
        return b
    if b is None:
        return a
    return a if a <= b else b


def assert_watermark_tracker_consistent(spark: SparkSession, config: Dict):
    tracker_table = (
        config.get("watermark_tracker_table")
        or main_pipeline.get_watermark_tracker_table(config)
    )
    if not spark.catalog.tableExists(tracker_table):
        raise AssertionError(f"Watermark tracker table not found: {tracker_table}")

    summary_table = config["destination_table"]
    latest_table = config["latest_history_table"]
    ts = config["max_identifier_column"]
    prt = config["partition_column"]

    summary_stats = (
        spark.table(summary_table)
        .agg(F.max(F.col(ts)).alias("max_base_ts"), F.max(F.col(prt)).alias("max_rpt_as_of_mo"))
        .first()
    )
    latest_stats = (
        spark.table(latest_table)
        .agg(F.max(F.col(ts)).alias("max_base_ts"), F.max(F.col(prt)).alias("max_rpt_as_of_mo"))
        .first()
    )

    tracker_rows = (
        spark.table(tracker_table)
        .select("source_name", "max_base_ts", "max_rpt_as_of_mo")
        .collect()
    )
    tracker_map = {
        row["source_name"]: (row["max_base_ts"], row["max_rpt_as_of_mo"])
        for row in tracker_rows
    }

    expected_summary = (summary_stats["max_base_ts"], summary_stats["max_rpt_as_of_mo"])
    expected_latest = (latest_stats["max_base_ts"], latest_stats["max_rpt_as_of_mo"])
    expected_committed = (
        _safe_min_datetime(summary_stats["max_base_ts"], latest_stats["max_base_ts"]),
        _safe_min_month(summary_stats["max_rpt_as_of_mo"], latest_stats["max_rpt_as_of_mo"]),
    )

    actual_summary = tracker_map.get(main_pipeline.TRACKER_SOURCE_SUMMARY)
    actual_latest = tracker_map.get(main_pipeline.TRACKER_SOURCE_LATEST_SUMMARY)
    actual_committed = tracker_map.get(main_pipeline.TRACKER_SOURCE_COMMITTED)

    if actual_summary != expected_summary:
        raise AssertionError(
            f"Summary tracker mismatch: expected={expected_summary}, actual={actual_summary}"
        )
    if actual_latest != expected_latest:
        raise AssertionError(
            f"Latest tracker mismatch: expected={expected_latest}, actual={actual_latest}"
        )
    if actual_committed != expected_committed:
        raise AssertionError(
            f"Committed tracker mismatch: expected={expected_committed}, actual={actual_committed}"
        )


def assert_deletion_aware_invariants(spark: SparkSession, config: Dict):
    summary_table = config["destination_table"]
    latest_table = config["latest_history_table"]
    pk = config["primary_column"]
    prt = config["partition_column"]

    if not spark.catalog.tableExists(summary_table):
        raise AssertionError(f"Summary table not found: {summary_table}")
    if not spark.catalog.tableExists(latest_table):
        raise AssertionError(f"Latest summary table not found: {latest_table}")

    summary_cols = set(spark.table(summary_table).columns)
    latest_cols = set(spark.table(latest_table).columns)
    if "soft_del_cd" not in summary_cols:
        raise AssertionError(f"'soft_del_cd' missing in summary table: {summary_table}")
    if "soft_del_cd" not in latest_cols:
        raise AssertionError(f"'soft_del_cd' missing in latest summary table: {latest_table}")

    dup_latest = (
        spark.table(latest_table)
        .groupBy(pk)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    if dup_latest != 0:
        raise AssertionError(f"latest_summary has duplicate account keys: {dup_latest}")

    # Deletion-aware invariant:
    # If latest_summary row is soft-deleted, that account must not still have
    # any active (non-deleted) row in summary.
    deleted_latest_with_active_summary = spark.sql(
        f"""
        SELECT COUNT(1) AS cnt
        FROM {latest_table} l
        INNER JOIN (
            SELECT DISTINCT {pk}
            FROM {summary_table}
            WHERE COALESCE(soft_del_cd, '') NOT IN ('1', '4')
        ) a
          ON l.{pk} = a.{pk}
        WHERE COALESCE(l.soft_del_cd, '') IN ('1', '4')
        """
    ).first()["cnt"]
    if deleted_latest_with_active_summary != 0:
        raise AssertionError(
            "latest_summary has deleted rows for accounts that still have active rows in summary "
            f"(count={deleted_latest_with_active_summary})"
        )

    # For non-deleted latest rows, month should match max active month in summary.
    latest_active_month_mismatch = spark.sql(
        f"""
        SELECT COUNT(1) AS cnt
        FROM (
            SELECT {pk}, MAX({prt}) AS max_active_month
            FROM {summary_table}
            WHERE COALESCE(soft_del_cd, '') NOT IN ('1', '4')
            GROUP BY {pk}
        ) s
        INNER JOIN {latest_table} l
          ON s.{pk} = l.{pk}
        WHERE COALESCE(l.soft_del_cd, '') NOT IN ('1', '4')
          AND l.{prt} <> s.max_active_month
        """
    ).first()["cnt"]
    if latest_active_month_mismatch != 0:
        raise AssertionError(
            "latest_summary active month mismatch vs summary max active month "
            f"(count={latest_active_month_mismatch})"
        )


_ORIG_RUN_PIPELINE = main_pipeline.run_pipeline


def _run_pipeline_with_deletion_assertions(spark: SparkSession, config: Dict, *args, **kwargs):
    result = _ORIG_RUN_PIPELINE(spark, config, *args, **kwargs)
    assert_deletion_aware_invariants(spark, config)
    return result


main_pipeline.run_pipeline = _run_pipeline_with_deletion_assertions


__all__ = [
    "create_spark_session",
    "load_main_test_config",
    "reset_tables",
    "history",
    "build_source_row",
    "build_summary_row",
    "write_source_rows",
    "write_summary_rows",
    "fetch_single_row",
    "assert_watermark_tracker_consistent",
    "assert_deletion_aware_invariants",
    "main_pipeline",
]
