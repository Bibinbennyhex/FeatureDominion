from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark import StorageLevel
import logging
import json
import argparse
import sys
import time
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple, Any, Optional
from functools import reduce
import math

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.propagate = False
start_time = time.time()

TARGET_RECORDS_PER_PARTITION = 500_000_000
MIN_PARTITIONS = 16
MAX_PARTITIONS = 8192
AVG_RECORD_SIZE_BYTES = 200
SNAPSHOT_INTERVAL = 12
MAX_FILE_SIZE = 256
HISTORY_LENGTH = 36
CASE_TEMP_BUCKET_COUNT = 64
WORKSET_LATEST_SUMMARY_TABLE = "temp_catalog.checkpointdb.workset_latest_summary"
WORKSET_SUMMARY_CASE3_TABLE = "temp_catalog.checkpointdb.workset_summary_case3"
CASE3_LATEST_MONTH_PATCH_TABLE = "temp_catalog.checkpointdb.case_3_latest_month_patch"
CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE = "temp_catalog.checkpointdb.case_3_unified_latest_month_patch"
CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE = "temp_catalog.checkpointdb.case_3d_latest_history_context_patch"
CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE = "temp_catalog.checkpointdb.case_3d_unified_latest_history_patch"
TRACKER_SOURCE_SUMMARY = "summary"
TRACKER_SOURCE_LATEST_SUMMARY = "latest_summary"
TRACKER_SOURCE_COMMITTED = "committed_ingestion_watermark"
TRACKER_STATUS_RUNNING = "RUNNING"
TRACKER_STATUS_SUCCESS = "SUCCESS"
TRACKER_STATUS_FAILURE = "FAILURE"
SOFT_DELETE_COLUMN = "soft_del_cd"
SOFT_DELETE_CODES = ["1", "4"]
LATEST_HISTORY_MIN_LEN_V4 = 72


def load_config(bucket, key):
    s3 = boto3.client("s3")
    logger.info(f"Loading configuration from: s3://{bucket}/{key}")

    obj = s3.get_object(Bucket=bucket, Key=key)
    config = json.loads(obj["Body"].read().decode("utf-8"))
    
    return config


def get_watermark_tracker_table(config: Dict[str, Any]) -> str:
    """Resolve tracker table name from config or derive from destination namespace."""
    explicit_table = config.get("watermark_tracker_table")
    if explicit_table:
        return explicit_table

    destination_table = config["destination_table"]
    if "." not in destination_table:
        return f"{destination_table}_watermark_tracker"

    namespace, _ = destination_table.rsplit(".", 1)
    return f"{namespace}.summary_watermark_tracker"


def _ensure_watermark_tracker_table(spark: SparkSession, tracker_table: str) -> None:
    spark.sql(
        f"""
            CREATE TABLE IF NOT EXISTS {tracker_table} (
                source_name STRING,
                source_table STRING,
                max_base_ts TIMESTAMP,
                max_rpt_as_of_mo STRING,
                updated_at TIMESTAMP,
                previous_successful_snapshot_id BIGINT,
                current_snapshot_id BIGINT,
                status STRING,
                run_id STRING,
                error_message STRING
            )
            USING iceberg
        """
    )
    existing_cols = set(spark.table(tracker_table).columns)
    required_cols = {
        "previous_successful_snapshot_id": "BIGINT",
        "current_snapshot_id": "BIGINT",
        "status": "STRING",
        "run_id": "STRING",
        "error_message": "STRING",
    }
    for col_name, data_type in required_cols.items():
        if col_name not in existing_cols:
            spark.sql(f"ALTER TABLE {tracker_table} ADD COLUMN {col_name} {data_type}")


def _capture_table_state(
    spark: SparkSession,
    table_name: str,
    partition_col: str,
    ts_col: str,
) -> Dict[str, Optional[Any]]:
    state = {
        "max_base_ts": None,
        "max_rpt_as_of_mo": None,
        "snapshot_id": None,
    }
    if not spark.catalog.tableExists(table_name):
        return state

    stats = (
        spark.table(table_name)
        .agg(
            F.max(F.col(ts_col)).alias("max_base_ts"),
            F.max(F.col(partition_col)).alias("max_rpt_as_of_mo"),
        )
        .first()
    )
    state["max_base_ts"] = stats["max_base_ts"]
    state["max_rpt_as_of_mo"] = stats["max_rpt_as_of_mo"]

    try:
        snap = spark.sql(
            f"""
                SELECT snapshot_id
                FROM {table_name}.snapshots
                ORDER BY committed_at DESC
                LIMIT 1
            """
        ).collect()
        if snap:
            state["snapshot_id"] = snap[0]["snapshot_id"]
    except Exception:
        # Table may exist without snapshots yet in some early lifecycle states.
        pass

    return state


def _read_tracker_rows(spark: SparkSession, tracker_table: str) -> Dict[str, Dict[str, Any]]:
    if not spark.catalog.tableExists(tracker_table):
        return {}
    rows = spark.table(tracker_table).collect()
    return {row["source_name"]: row.asDict() for row in rows}


def _upsert_tracker_rows(
    spark: SparkSession,
    tracker_table: str,
    rows: List[Tuple[Any, ...]],
) -> None:
    update_df = spark.createDataFrame(
        rows,
        schema=(
            "source_name STRING, source_table STRING, max_base_ts TIMESTAMP, "
            "max_rpt_as_of_mo STRING, updated_at TIMESTAMP, "
            "previous_successful_snapshot_id BIGINT, current_snapshot_id BIGINT, "
            "status STRING, run_id STRING, error_message STRING"
        ),
    )
    update_df.createOrReplaceTempView("watermark_tracker_updates")
    spark.sql(
        f"""
            MERGE INTO {tracker_table} t
            USING watermark_tracker_updates u
            ON t.source_name = u.source_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    )


def _capture_run_snapshot_states(spark: SparkSession, config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    prt = config["partition_column"]
    ts = config["max_identifier_column"]
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    return {
        TRACKER_SOURCE_SUMMARY: _capture_table_state(spark, summary_table, prt, ts),
        TRACKER_SOURCE_LATEST_SUMMARY: _capture_table_state(spark, latest_summary_table, prt, ts),
    }


def log_current_snapshot_state(spark: SparkSession, config: Dict[str, Any], label: str) -> Dict[str, Dict[str, Any]]:
    states = _capture_run_snapshot_states(spark, config)
    logger.info(f"{label} snapshot state:")
    logger.info(
        f"  summary: snapshot_id={states[TRACKER_SOURCE_SUMMARY]['snapshot_id']}, "
        f"max_base_ts={states[TRACKER_SOURCE_SUMMARY]['max_base_ts']}, "
        f"max_month={states[TRACKER_SOURCE_SUMMARY]['max_rpt_as_of_mo']}"
    )
    logger.info(
        f"  latest_summary: snapshot_id={states[TRACKER_SOURCE_LATEST_SUMMARY]['snapshot_id']}, "
        f"max_base_ts={states[TRACKER_SOURCE_LATEST_SUMMARY]['max_base_ts']}, "
        f"max_month={states[TRACKER_SOURCE_LATEST_SUMMARY]['max_rpt_as_of_mo']}"
    )
    return states


def _parse_catalog_and_identifier(table_name: str) -> Tuple[str, str]:
    parts = table_name.split(".")
    if len(parts) < 2:
        raise ValueError(f"Invalid table name for rollback: {table_name}")
    catalog = parts[0]
    identifier = ".".join(parts[1:])
    return catalog, identifier


def _rollback_table_to_snapshot(
    spark: SparkSession,
    table_name: str,
    snapshot_id: Optional[Any],
) -> bool:
    """
    Rollback a single Iceberg table to a given snapshot id.
    Returns True when rollback statement executed successfully.
    """
    if snapshot_id is None:
        logger.warning(f"Rollback skipped for {table_name}: start snapshot is NULL")
        return False
    if not spark.catalog.tableExists(table_name):
        logger.warning(f"Rollback skipped for {table_name}: table does not exist")
        return False

    catalog, identifier = _parse_catalog_and_identifier(table_name)
    snapshot_id_int = int(snapshot_id)

    # Try named-argument call first, then positional fallback.
    try:
        spark.sql(
            f"""
                CALL {catalog}.system.rollback_to_snapshot(
                    table => '{identifier}',
                    snapshot_id => {snapshot_id_int}
                )
            """
        )
        return True
    except Exception:
        spark.sql(
            f"CALL {catalog}.system.rollback_to_snapshot('{identifier}', {snapshot_id_int})"
        )
        return True


def rollback_tables_to_run_start(
    spark: SparkSession,
    config: Dict[str, Any],
    start_states: Dict[str, Dict[str, Any]],
) -> Dict[str, str]:
    """
    Best-effort rollback for summary and latest_summary to their pre-run snapshots.
    Returns per-table status messages.
    """
    statuses: Dict[str, str] = {}
    table_map = {
        TRACKER_SOURCE_SUMMARY: config["destination_table"],
        TRACKER_SOURCE_LATEST_SUMMARY: config["latest_history_table"],
    }

    for source_name, table_name in table_map.items():
        start_snapshot = start_states.get(source_name, {}).get("snapshot_id")
        try:
            executed = _rollback_table_to_snapshot(spark, table_name, start_snapshot)
            if executed:
                statuses[source_name] = f"ROLLED_BACK_TO_{start_snapshot}"
                logger.info(
                    f"Rollback complete for {source_name} ({table_name}) "
                    f"to snapshot_id={start_snapshot}"
                )
            else:
                statuses[source_name] = f"SKIPPED_START_SNAPSHOT_{start_snapshot}"
        except Exception as rollback_error:
            statuses[source_name] = f"ROLLBACK_FAILED_{rollback_error}"
            logger.error(
                f"Rollback failed for {source_name} ({table_name}) "
                f"to snapshot_id={start_snapshot}: {rollback_error}"
            )

    return statuses


def mark_run_started(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str,
    start_states: Dict[str, Dict[str, Any]],
) -> None:
    tracker_table = get_watermark_tracker_table(config)
    _ensure_watermark_tracker_table(spark, tracker_table)
    existing = _read_tracker_rows(spark, tracker_table)

    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    now = datetime.utcnow()

    update_rows = []
    for source_name, source_table in [
        (TRACKER_SOURCE_SUMMARY, summary_table),
        (TRACKER_SOURCE_LATEST_SUMMARY, latest_summary_table),
    ]:
        existing_row = existing.get(source_name, {})
        prev_success_id = existing_row.get("previous_successful_snapshot_id")
        if prev_success_id is None and existing_row.get("status") == TRACKER_STATUS_SUCCESS:
            prev_success_id = existing_row.get("current_snapshot_id")
        state = start_states[source_name]
        update_rows.append(
            (
                source_name,
                source_table,
                state["max_base_ts"],
                state["max_rpt_as_of_mo"],
                now,
                prev_success_id,
                state["snapshot_id"],
                TRACKER_STATUS_RUNNING,
                run_id,
                None,
            )
        )

    committed_existing = existing.get(TRACKER_SOURCE_COMMITTED, {})
    committed_ts = committed_existing.get("max_base_ts")
    committed_month = committed_existing.get("max_rpt_as_of_mo")
    if committed_ts is None:
        committed_ts = _safe_min_datetime(
            start_states[TRACKER_SOURCE_SUMMARY]["max_base_ts"],
            start_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_base_ts"],
        )
    if committed_month is None:
        committed_month = _safe_min_month(
            start_states[TRACKER_SOURCE_SUMMARY]["max_rpt_as_of_mo"],
            start_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_rpt_as_of_mo"],
        )
    update_rows.append(
        (
            TRACKER_SOURCE_COMMITTED,
            config["source_table"],
            committed_ts,
            committed_month,
            now,
            committed_existing.get("previous_successful_snapshot_id"),
            committed_existing.get("current_snapshot_id"),
            TRACKER_STATUS_RUNNING,
            run_id,
            None,
        )
    )

    _upsert_tracker_rows(spark, tracker_table, update_rows)
    logger.info(f"Marked tracker RUNNING for run_id={run_id}")


def finalize_run_tracking(
    spark: SparkSession,
    config: Dict[str, Any],
    run_id: str,
    start_states: Dict[str, Dict[str, Any]],
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    tracker_table = get_watermark_tracker_table(config)
    _ensure_watermark_tracker_table(spark, tracker_table)
    existing = _read_tracker_rows(spark, tracker_table)
    end_states = _capture_run_snapshot_states(spark, config)
    now = datetime.utcnow()

    status = TRACKER_STATUS_SUCCESS if success else TRACKER_STATUS_FAILURE
    update_rows = []

    for source_name, source_table in [
        (TRACKER_SOURCE_SUMMARY, config["destination_table"]),
        (TRACKER_SOURCE_LATEST_SUMMARY, config["latest_history_table"]),
    ]:
        existing_row = existing.get(source_name, {})
        if success:
            prev_success_id = start_states[source_name]["snapshot_id"]
        else:
            prev_success_id = existing_row.get("previous_successful_snapshot_id")
            if prev_success_id is None and existing_row.get("status") == TRACKER_STATUS_SUCCESS:
                prev_success_id = existing_row.get("current_snapshot_id")

        update_rows.append(
            (
                source_name,
                source_table,
                end_states[source_name]["max_base_ts"],
                end_states[source_name]["max_rpt_as_of_mo"],
                now,
                prev_success_id,
                end_states[source_name]["snapshot_id"],
                status,
                run_id,
                None if success else (error_message[:500] if error_message else "pipeline_failed"),
            )
        )

    committed_existing = existing.get(TRACKER_SOURCE_COMMITTED, {})
    if success:
        committed_ts = _safe_min_datetime(
            end_states[TRACKER_SOURCE_SUMMARY]["max_base_ts"],
            end_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_base_ts"],
        )
        committed_month = _safe_min_month(
            end_states[TRACKER_SOURCE_SUMMARY]["max_rpt_as_of_mo"],
            end_states[TRACKER_SOURCE_LATEST_SUMMARY]["max_rpt_as_of_mo"],
        )
    else:
        committed_ts = committed_existing.get("max_base_ts")
        committed_month = committed_existing.get("max_rpt_as_of_mo")

    update_rows.append(
        (
            TRACKER_SOURCE_COMMITTED,
            config["source_table"],
            committed_ts,
            committed_month,
            now,
            committed_existing.get("previous_successful_snapshot_id"),
            committed_existing.get("current_snapshot_id"),
            status,
            run_id,
            None if success else (error_message[:500] if error_message else "pipeline_failed"),
        )
    )

    _upsert_tracker_rows(spark, tracker_table, update_rows)
    logger.info(
        f"Finalized tracker for run_id={run_id}, status={status}, "
        f"tracker_table={tracker_table}"
    )


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


def get_committed_ingestion_watermark(
    spark: SparkSession,
    config: Dict[str, Any],
    fallback_base_ts: Optional[datetime],
    fallback_rpt_as_of_mo: Optional[str],
) -> Tuple[Optional[datetime], Optional[str], str]:
    """
    Return committed ingestion watermark from tracker if available, else fallback.

    This watermark is used to filter source increments and is advanced only after
    successful end-to-end run completion.
    """
    tracker_table = get_watermark_tracker_table(config)

    if spark.catalog.tableExists(tracker_table):
        row = (
            spark.table(tracker_table)
            .filter(F.col("source_name") == TRACKER_SOURCE_COMMITTED)
            .select("max_base_ts", "max_rpt_as_of_mo")
            .limit(1)
            .collect()
        )
        if row:
            committed_ts = row[0]["max_base_ts"]
            committed_month = row[0]["max_rpt_as_of_mo"]
            if committed_ts is not None:
                return committed_ts, committed_month, "tracker"

    return fallback_base_ts, fallback_rpt_as_of_mo, "fallback_summary"


def refresh_watermark_tracker(
    spark: SparkSession,
    config: Dict[str, Any],
    mark_committed: bool = True,
) -> None:
    """
    Persist latest watermark snapshot for summary and latest_summary tables.

    Tracker schema:
      source_name: 'summary' | 'latest_summary' | 'committed_ingestion_watermark'
      source_table: fully qualified table name
      max_base_ts: latest base_ts in source table
      max_rpt_as_of_mo: latest rpt_as_of_mo in source table
      updated_at: tracker refresh timestamp (UTC)
    """
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]
    prt = config["partition_column"]
    ts = config["max_identifier_column"]
    tracker_table = get_watermark_tracker_table(config)

    _ensure_watermark_tracker_table(spark, tracker_table)

    summary_stats = (
        spark.table(summary_table)
        .agg(
            F.max(F.col(ts)).alias("max_base_ts"),
            F.max(F.col(prt)).alias("max_rpt_as_of_mo"),
        )
        .first()
    )

    latest_summary_stats = (
        spark.table(latest_summary_table)
        .agg(
            F.max(F.col(ts)).alias("max_base_ts"),
            F.max(F.col(prt)).alias("max_rpt_as_of_mo"),
        )
        .first()
    )

    update_rows = [
        (
            TRACKER_SOURCE_SUMMARY,
            summary_table,
            summary_stats["max_base_ts"],
            summary_stats["max_rpt_as_of_mo"],
            datetime.utcnow(),
        ),
        (
            TRACKER_SOURCE_LATEST_SUMMARY,
            latest_summary_table,
            latest_summary_stats["max_base_ts"],
            latest_summary_stats["max_rpt_as_of_mo"],
            datetime.utcnow(),
        ),
    ]

    if mark_committed:
        committed_base_ts = _safe_min_datetime(
            summary_stats["max_base_ts"],
            latest_summary_stats["max_base_ts"],
        )
        committed_rpt_as_of_mo = _safe_min_month(
            summary_stats["max_rpt_as_of_mo"],
            latest_summary_stats["max_rpt_as_of_mo"],
        )
        update_rows.append(
            (
                TRACKER_SOURCE_COMMITTED,
                config["source_table"],
                committed_base_ts,
                committed_rpt_as_of_mo,
                datetime.utcnow(),
            )
        )

    update_df = spark.createDataFrame(
        update_rows,
        schema="source_name STRING, source_table STRING, max_base_ts TIMESTAMP, max_rpt_as_of_mo STRING, updated_at TIMESTAMP",
    )
    update_df.createOrReplaceTempView("watermark_tracker_updates")

    spark.sql(
        f"""
            MERGE INTO {tracker_table} t
            USING watermark_tracker_updates u
            ON t.source_name = u.source_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
    )

    logger.info(
        f"Refreshed watermark tracker table: {tracker_table} "
        f"(mark_committed={mark_committed})"
    )


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration"""
    required_fields = ["source_table", "partition_column", "destination_table", "latest_history_table", "primary_column", "primary_date_column", "max_identifier_column", "history_length", "columns", "column_transformations", "inferred_columns", "coalesce_exclusion_cols", "date_col_list", "latest_history_addon_cols", "rolling_columns", "grid_columns", "spark"]
    
    for field in required_fields:
        if field not in config or not config[field]:
            logger.error(f"Required config field '{field}' is missing or empty")
            return False
    
    return True


def ensure_soft_delete_columns(spark: SparkSession, config: Dict[str, Any]) -> None:
    """
    Ensure summary/latest_summary tables both expose soft_del_cd.
    """
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]

    if spark.catalog.tableExists(summary_table):
        summary_cols = _read_table_columns(spark, summary_table)
        if SOFT_DELETE_COLUMN not in summary_cols:
            logger.info(f"Adding {SOFT_DELETE_COLUMN} to {summary_table}")
            spark.sql(f"ALTER TABLE {summary_table} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")

    if spark.catalog.tableExists(latest_summary_table):
        latest_cols = _read_table_columns(spark, latest_summary_table)
        if SOFT_DELETE_COLUMN not in latest_cols:
            logger.info(f"Adding {SOFT_DELETE_COLUMN} to {latest_summary_table}")
            spark.sql(f"ALTER TABLE {latest_summary_table} ADD COLUMN {SOFT_DELETE_COLUMN} STRING")


def _read_table_columns(spark: SparkSession, table_name: str) -> List[str]:
    return spark.table(table_name).columns


def preload_run_table_columns(spark: SparkSession, config: Dict[str, Any]) -> None:
    runtime_cache = config.setdefault("_runtime_cache", {})
    summary_table = config["destination_table"]
    latest_summary_table = config["latest_history_table"]

    runtime_cache["summary_cols"] = (
        _read_table_columns(spark, summary_table)
        if spark.catalog.tableExists(summary_table)
        else []
    )
    runtime_cache["latest_summary_cols"] = (
        _read_table_columns(spark, latest_summary_table)
        if spark.catalog.tableExists(latest_summary_table)
        else []
    )

    logger.info(
        f"Preloaded run schema cache | summary_cols={len(runtime_cache['summary_cols'])}, "
        f"latest_summary_cols={len(runtime_cache['latest_summary_cols'])}"
    )


def get_summary_cols(config: Dict[str, Any]) -> List[str]:
    runtime_cache = config.get("_runtime_cache", {})
    if "summary_cols" not in runtime_cache:
        raise ValueError("summary_cols missing from runtime cache; call preload_run_table_columns first")
    return runtime_cache["summary_cols"]


def get_latest_cols(config: Dict[str, Any]) -> List[str]:
    runtime_cache = config.get("_runtime_cache", {})
    if "latest_summary_cols" not in runtime_cache:
        raise ValueError("latest_summary_cols missing from runtime cache; call preload_run_table_columns first")
    return runtime_cache["latest_summary_cols"]


def _use_working_set_latest_context(config: Dict[str, Any]) -> bool:
    return bool(config.get("use_working_set_latest_context", True))


def _use_working_set_case3_summary_context(config: Dict[str, Any]) -> bool:
    return bool(config.get("use_working_set_case3_summary_context", True))


def get_latest_context_table(spark: SparkSession, config: Dict[str, Any]) -> str:
    if _use_working_set_latest_context(config) and spark.catalog.tableExists(WORKSET_LATEST_SUMMARY_TABLE):
        return WORKSET_LATEST_SUMMARY_TABLE
    return config["latest_history_table"]


def get_case3_summary_context_table(spark: SparkSession, config: Dict[str, Any]) -> str:
    if _use_working_set_case3_summary_context(config) and spark.catalog.tableExists(WORKSET_SUMMARY_CASE3_TABLE):
        return WORKSET_SUMMARY_CASE3_TABLE
    return config["destination_table"]


def get_case3_latest_month_patch_table(config: Dict[str, Any]) -> str:
    return config.get("_case3_latest_month_patch_table", CASE3_LATEST_MONTH_PATCH_TABLE)


def get_case3d_latest_history_patch_table(config: Dict[str, Any]) -> str:
    return config.get("_case3d_latest_history_patch_table", CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE)


def materialize_working_set_context_tables(
    spark: SparkSession,
    classified_df,
    config: Dict[str, Any],
) -> None:
    pk = config["primary_column"]
    prt = config["partition_column"]
    ts = config["max_identifier_column"]
    latest_summary_table = config["latest_history_table"]
    summary_table = config["destination_table"]

    classified_keys = classified_df.select(pk).distinct()
    classified_keys.createOrReplaceTempView("classified_accounts_all")
    logger.info("Working-set key scope built: classified_accounts_all")

    case3_keys = (
        classified_df
        .filter(F.col("case_type") == "CASE_III")
        .select(pk)
        .distinct()
    )
    case3_keys.createOrReplaceTempView("case3_accounts_all")
    logger.info("Working-set key scope built: case3_accounts_all")

    if _use_working_set_latest_context(config):
        latest_latest_df = (
            spark.table(latest_summary_table)
            .withColumn(
                "_rn",
                F.row_number().over(
                    Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())
                ),
            )
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        latest_context_df = classified_keys.join(
            latest_latest_df,
            on=pk,
            how="left",
        )
        write_case_table_bucketed(
            spark=spark,
            df=latest_context_df,
            table_name=WORKSET_LATEST_SUMMARY_TABLE,
            config=config,
            stage="workset_latest_summary_temp",
            expected_rows=None,
        )

    if _use_working_set_case3_summary_context(config):
        if case3_keys.limit(1).isEmpty():
            summary_context_df = spark.table(summary_table).limit(0)
        else:
            summary_context_df = (
                spark.table(summary_table)
                .join(F.broadcast(case3_keys), on=pk, how="inner")
            )
        write_case_table_bucketed(
            spark=spark,
            df=summary_context_df,
            table_name=WORKSET_SUMMARY_CASE3_TABLE,
            config=config,
            stage="workset_summary_case3_temp",
            expected_rows=None,
        )

    logger.info("Working-set table creation completed")


def get_summary_history_len(config: Dict[str, Any]) -> int:
    return _to_int(config.get("history_length", HISTORY_LENGTH), HISTORY_LENGTH)


def get_latest_history_len(config: Dict[str, Any]) -> int:
    configured_latest_len = _to_int(
        config.get("latest_history_window_months", LATEST_HISTORY_MIN_LEN_V4),
        LATEST_HISTORY_MIN_LEN_V4,
    )
    return max(configured_latest_len, get_summary_history_len(config))


def align_history_arrays_to_length(df, rolling_columns: List[Dict[str, Any]], target_len: int):
    if target_len <= 0:
        return df
    for rc in rolling_columns:
        history_col = f"{rc['name']}_history"
        if history_col in df.columns:
            df = df.withColumn(
                history_col,
                F.transform(
                    F.sequence(F.lit(0), F.lit(target_len - 1)),
                    lambda i: F.element_at(F.col(history_col), i + F.lit(1)),
                ),
            )
    return df


def latest_history_preserve_tail_expr(
    array_name: str,
    summary_history_len: int,
    latest_history_len: int,
) -> str:
    return (
        f"transform(sequence(0, {latest_history_len - 1}), i -> "
        f"CASE WHEN i < {summary_history_len} "
        f"THEN element_at(c.{array_name}, i + 1) "
        f"ELSE element_at(s.{array_name}, i + 1) END)"
    )


def summary_history_trim_expr(array_name: str, summary_history_len: int) -> str:
    return f"slice(c.{array_name}, 1, {summary_history_len})"


def create_spark_session(app_name: str, spark_config: Dict[str, Any]):
    spark_builder = SparkSession.builder.appName(app_name)
    for key,value in spark_config.items():
        if key != 'app_name':
            spark_builder = spark_builder.config(key,str(value))

    spark = spark_builder.enableHiveSupport().getOrCreate()

    return spark


def month_to_int_expr(col_name: str) -> str:
    """SQL expression to convert YYYY-MM to integer"""
    return f"(CAST(SUBSTRING({col_name}, 1, 4) AS INT) * 12 + CAST(SUBSTRING({col_name}, 6, 2) AS INT))"


def _to_int(value: Any, default: int) -> int:
    """Best-effort int conversion with default fallback."""
    try:
        return int(value)
    except Exception:
        return default


def get_write_partitions(
    spark: SparkSession,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
    scale_factor: float = 1.0,
    stage: str = ""
) -> int:
    """
    Compute write partitions efficiently and effectively.

    Strategy:
      1. Respect explicit override `write_partitions` if provided.
      2. Cache runtime defaults (shuffle/defaultParallelism) once per run.
      3. If expected_rows is known, blend row-based sizing with a baseline
         parallelism floor to avoid under-utilization.
    """
    configured = config.get("write_partitions")
    if configured is not None:
        parts = _to_int(configured, MIN_PARTITIONS)
        parts = max(MIN_PARTITIONS, min(MAX_PARTITIONS, parts))
        return parts

    runtime_cache = config.setdefault("_runtime_cache", {})
    write_cache = runtime_cache.get("write_partitions")
    if write_cache is None:
        shuffle_parts = _to_int(spark.conf.get("spark.sql.shuffle.partitions", "200"), 200)
        default_parallelism = _to_int(spark.sparkContext.defaultParallelism, MIN_PARTITIONS)
        base_parallelism = max(shuffle_parts, default_parallelism)

        perf_cfg = config.get("performance", {})
        min_parts = _to_int(
            config.get("min_write_partitions", perf_cfg.get("min_partitions", MIN_PARTITIONS)),
            MIN_PARTITIONS
        )
        max_parts = _to_int(
            config.get("max_write_partitions", perf_cfg.get("max_partitions", MAX_PARTITIONS)),
            MAX_PARTITIONS
        )
        target_rows_per_partition = _to_int(
            config.get("target_rows_per_partition", perf_cfg.get("target_records_per_partition", 2_000_000)),
            2_000_000
        )
        min_parallelism_factor = float(
            config.get("min_parallelism_factor", perf_cfg.get("min_parallelism_factor", 0.25))
        )

        write_cache = {
            "base_parallelism": base_parallelism,
            "min_parts": max(1, min_parts),
            "max_parts": max(1, max_parts),
            "target_rows_per_partition": max(1, target_rows_per_partition),
            "min_parallelism_factor": max(0.0, min_parallelism_factor),
        }
        runtime_cache["write_partitions"] = write_cache

    base_parallelism = write_cache["base_parallelism"]
    min_parts = write_cache["min_parts"]
    max_parts = write_cache["max_parts"]
    target_rows_per_partition = write_cache["target_rows_per_partition"]
    min_parallelism_factor = write_cache["min_parallelism_factor"]

    scaled_base = max(1, int(round(base_parallelism * scale_factor)))

    if expected_rows is not None and expected_rows > 0:
        row_based = int(math.ceil(float(expected_rows) / float(target_rows_per_partition)))
        floor_parallel = int(max(min_parts, round(scaled_base * min_parallelism_factor)))
        desired = max(row_based, floor_parallel)
    else:
        desired = scaled_base

    parts = max(min_parts, min(max_parts, desired))

    if stage:
        logger.info(
            f"Write partitions [{stage}]: {parts} "
            f"(base={base_parallelism}, expected_rows={expected_rows}, scale={scale_factor})"
        )

    return parts


def align_for_summary_merge(
    spark: SparkSession,
    df,
    config: Dict[str, Any],
    stage: str,
    expected_rows: Optional[int] = None,
):
    """
    Align merge source layout with summary table distribution:
    repartition on (partition_column, primary_column) and sort within partition.
    """
    pk = config["primary_column"]
    prt = config["partition_column"]
    write_parts = get_write_partitions(spark, config, expected_rows=expected_rows, stage=stage)
    return df.repartition(write_parts, F.col(prt), F.col(pk)).sortWithinPartitions(prt, pk)


def write_case_table_bucketed(
    spark: SparkSession,
    df,
    table_name: str,
    config: Dict[str, Any],
    stage: str,
    expected_rows: Optional[int] = None,
) -> None:
    """
    Materialize case_* table as Iceberg table partitioned by month and bucketed by account key.
    """
    pk = config["primary_column"]
    prt = config["partition_column"]
    bucket_count = int(config.get("case_temp_bucket_count", CASE_TEMP_BUCKET_COUNT))

    if pk not in df.columns or prt not in df.columns:
        raise ValueError(f"{table_name} write requires columns '{pk}' and '{prt}'")

    aligned_df = align_for_summary_merge(
        spark=spark,
        df=df,
        config=config,
        stage=stage,
        expected_rows=expected_rows,
    )
    temp_view = f"_tmp_{table_name.replace('.', '_')}_{int(time.time() * 1000)}"
    namespace = table_name.rsplit(".", 1)[0]

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    aligned_df.createOrReplaceTempView(temp_view)
    create_sql = f"""
        CREATE TABLE {table_name}
        USING iceberg
        PARTITIONED BY ({prt}, bucket({bucket_count}, {pk}))
        TBLPROPERTIES (
            'write.distribution-mode'='hash',
            'write.merge.distribution-mode'='hash'
        )
        AS SELECT * FROM {temp_view}
    """
    max_retries = 3
    try:
        for attempt in range(1, max_retries + 1):
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            try:
                spark.sql(create_sql)
                break
            except Exception as e:
                msg = str(e).lower()
                if "already exists" in msg and attempt < max_retries:
                    time.sleep(1.0 * attempt)
                    continue
                raise
    finally:
        spark.catalog.dropTempView(temp_view)


def drop_case_tables(
    spark: SparkSession,
    table_stage_map: Dict[str, str],
) -> None:
    for table_name in table_stage_map.keys():
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def snapshot_case_tables(
    spark: SparkSession,
    table_stage_map: Dict[str, str],
    lane_tag: str,
) -> Dict[str, str]:
    snapshot_tables: Dict[str, str] = {}
    for table_name in table_stage_map.keys():
        if spark.catalog.tableExists(table_name):
            snapshot_name = f"{table_name}_{lane_tag}"
            spark.sql(f"DROP TABLE IF EXISTS {snapshot_name}")
            spark.sql(f"CREATE TABLE {snapshot_name} USING iceberg AS SELECT * FROM {table_name}")
            snapshot_tables[table_name] = snapshot_name
    return snapshot_tables


def cleanup_snapshot_tables(
    spark: SparkSession,
    snapshot_groups: List[Dict[str, str]],
) -> None:
    for snapshot_map in snapshot_groups:
        for snapshot_name in snapshot_map.values():
            spark.sql(f"DROP TABLE IF EXISTS {snapshot_name}")


def combine_case_table_snapshots(
    spark: SparkSession,
    config: Dict[str, Any],
    table_stage_map: Dict[str, str],
    snapshot_groups: List[Dict[str, str]],
    expected_rows: Optional[int] = None,
) -> None:
    for table_name, stage_name in table_stage_map.items():
        parts = []
        for snapshot_map in snapshot_groups:
            snapshot_name = snapshot_map.get(table_name)
            if snapshot_name and spark.catalog.tableExists(snapshot_name):
                parts.append(spark.read.table(snapshot_name))

        if not parts:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            continue

        combined_df = parts[0]
        for extra_df in parts[1:]:
            combined_df = combined_df.unionByName(extra_df, allowMissingColumns=True)
        combined_df = combined_df.dropDuplicates()

        write_case_table_bucketed(
            spark=spark,
            df=combined_df,
            table_name=table_name,
            config=config,
            stage=stage_name,
            expected_rows=expected_rows,
        )


def build_balanced_month_chunks(
    month_weights: List[Tuple[str, float]],
    overflow_ratio: float = 0.10,
) -> List[List[str]]:
    """
    Build balanced month chunks using a greedy bin-packing strategy.

    Args:
        month_weights: list of (rpt_as_of_mo, weighted_load)
        overflow_ratio: allowed overflow over the target chunk load.

    Returns:
        List of month lists, each list representing one merge chunk.
    """
    if not month_weights:
        return []

    # Largest month is used as baseline target load.
    sorted_weights = sorted(
        [(m, float(w)) for m, w in month_weights if m is not None and w is not None and w > 0],
        key=lambda x: x[1],
        reverse=True,
    )
    if not sorted_weights:
        return []

    target = sorted_weights[0][1]
    allowed = target * (1.0 + max(0.0, overflow_ratio))

    bins: List[Dict[str, Any]] = []
    for month, weight in sorted_weights:
        if not bins:
            bins.append({"load": weight, "months": [month]})
            continue

        # Place next month into the lightest chunk when possible.
        lightest_idx = min(range(len(bins)), key=lambda i: bins[i]["load"])
        if bins[lightest_idx]["load"] + weight <= allowed:
            bins[lightest_idx]["load"] += weight
            bins[lightest_idx]["months"].append(month)
        else:
            bins.append({"load": weight, "months": [month]})

    chunks = [sorted(b["months"]) for b in bins if b["months"]]
    return chunks


def build_month_chunks_from_df(
    df,
    prt: str,
    overflow_ratio: float = 0.10,
) -> List[List[str]]:
    """
    Build balanced month chunks from a dataframe using per-month row counts as weights.
    """
    if df is None:
        return []

    month_weights = [
        (r[prt], float(r["count"]))
        for r in df.groupBy(prt).count().select(prt, "count").collect()
        if r[prt] is not None and r["count"] is not None and r["count"] > 0
    ]

    month_chunks = build_balanced_month_chunks(month_weights, overflow_ratio=overflow_ratio)
    if month_chunks:
        return month_chunks

    return [[r[prt]] for r in df.select(prt).distinct().orderBy(prt).collect() if r[prt] is not None]


def prepare_source_data(df, date_df, config: Dict[str, Any]):
    """
    Prepare source data with:
    1. Column mappings (rename source -> destination)
    2. Column transformations (sentinel value handling)
    3. Inferred/derived columns
    4. Date column validation
    5. Deduplication
    """
    logger.info("Preparing source data...")
    
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    pdt = config['primary_date_column']
    
    result = df
    deduplication_trackers = ['insert_ts','update_ts']
    
    # Step 1: Apply column mappings
    column_mappings = config.get('columns', {})
    if column_mappings:
        select_exprs = []
        for src_col, dst_col in column_mappings.items():
            if src_col in result.columns:
                select_exprs.append(F.col(src_col).alias(dst_col))
            else:
                logger.warning(f"Source column '{src_col}' not found, skipping")
        
        if select_exprs:
            result = result.select(*select_exprs, *deduplication_trackers)
            logger.info(f"Applied {len(column_mappings)} column mappings")
    
    # Step 2: Apply column transformations
    column_transformations = config.get('column_transformations', [])
    for transform in column_transformations:
        col_name = transform['name']
        # Use 'mapper_expr' to match original format
        expr = transform.get('mapper_expr', transform.get('expr', col_name))
        if col_name in result.columns or any(c in expr for c in result.columns):
            result = result.withColumn(col_name, F.expr(expr))
    
    if column_transformations:
        logger.info(f"Applied {len(column_transformations)} column transformations")
    
    # Step 3: Apply inferred/derived columns
    inferred_columns = config.get('inferred_columns', [])
    for inferred in inferred_columns:
        col_name = inferred['name']
        # Use 'mapper_expr' to match original format
        expr = inferred.get('mapper_expr', inferred.get('expr', ''))
        if expr:
            result = result.withColumn(col_name, F.expr(expr))
    
    if inferred_columns:
        logger.info(f"Created {len(inferred_columns)} inferred columns")
    
    # Step 4: Apply rolling column mappers (prepare values for history arrays)
    rolling_columns = config.get('rolling_columns', [])
    for rc in rolling_columns:
        result = result.withColumn(f"{rc['mapper_column']}", F.expr(rc['mapper_expr']))
    
    # Step 5: Validate date columns (replace invalid years with NULL)
    date_columns = config.get('date_col_list', config.get('date_columns', []))
    for date_col in date_columns:
        if date_col in result.columns:
            result = result.withColumn(
                date_col,
                F.when(F.year(F.col(date_col)) < 1000, None)
                 .otherwise(F.col(date_col))
            )
    
    if date_columns:
        logger.info(f"Validated {len(date_columns)} date columns")
    
    # Step 6: Deduplicate by primary key + partition, keeping latest timestamp
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc(), *[F.col(c).desc() for c in deduplication_trackers])
    
    result = (
        result
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn", *deduplication_trackers)
    )
    
    date_df = date_df.withColumn(prt, F.date_format(F.col(pdt), "yyyy-MM"))
    hist_dedup_order_cols = [ts, "insert_dt", "update_dt", "insert_time", "update_time"]
    hist_dedup_order_exprs = [
        F.col(col_name).desc()
        for col_name in hist_dedup_order_cols
        if col_name in date_df.columns
    ]
    if not hist_dedup_order_exprs:
        hist_dedup_order_exprs = [F.col(ts).desc()]

    date_df_window_spec = Window.partitionBy(pk, prt).orderBy(*hist_dedup_order_exprs)

    date_df = (
        date_df
        .withColumn("_rn", F.row_number().over(date_df_window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    r = result.alias("r")
    d = F.broadcast(date_df.alias("d"))
    delete_codes_sql = ",".join([f"'{code}'" for code in SOFT_DELETE_CODES])

    result_final = r.join(
        d,
        (
            (F.col(f"r.{pk}") == F.col(f"d.{pk}")) &
            (F.col(f"r.{prt}") == F.col(f"d.{prt}"))
        ),
        "left"
    ).select(
        *[F.col(f"r.{c}") for c in result.columns if c not in [pdt,"soft_del_cd"]],
        F.expr(
            f"""
                CASE
                    WHEN d.{pk} IS NULL
                    THEN r.{pdt}
                    WHEN COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql})
                        AND COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql})
                    THEN NULL
                    WHEN COALESCE(r.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql}) OR r.{pdt} > d.{pdt})
                    THEN r.{pdt}
                    WHEN d.{pk} IS NOT NULL
                        AND COALESCE(d.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql}) OR d.{pdt} > r.{pdt})
                    THEN d.{pdt}
                    ELSE r.{pdt}
                END
            """
        ).alias(pdt),
        F.expr(
            f"""
                CASE
                    WHEN d.{pk} IS NULL
                    THEN r.soft_del_cd
                    WHEN COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql})
                        AND COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql})
                    THEN r.soft_del_cd
                    WHEN COALESCE(r.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(d.soft_del_cd, '') IN ({delete_codes_sql}) OR r.{pdt} > d.{pdt})
                    THEN r.soft_del_cd
                    WHEN d.{pk} IS NOT NULL
                        AND COALESCE(d.soft_del_cd, '') NOT IN ({delete_codes_sql})
                        AND (COALESCE(r.soft_del_cd, '') IN ({delete_codes_sql}) OR d.{pdt} > r.{pdt})
                    THEN d.soft_del_cd
                    ELSE r.soft_del_cd
                END
            """
        ).alias("soft_del_cd")
    )

    logger.info("Source data preparation complete")
    return result_final


def load_and_classify_accounts(spark: SparkSession, config: Dict[str, Any]):
    """
    Load accounts and classify into Case I/II/III
    
    Case I:   New accounts (not in summary)
    Case II:  Forward entries (month > max existing)
    Case III: Backfill (month <= max existing)
    Case IV: New account with MULTIPLE months - subsequent months

    Returns: DataFrame with case_type column
    """
    logger.info("=" * 80)
    logger.info("STEP 1: Load and Classify Accounts")
    logger.info("=" * 80)

    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    source_table = config['source_table']
    destination_table = config['destination_table']
    latest_summary_table = config['latest_history_table']
    hist_rpt_dt_table = config['hist_rpt_dt_table']
    hist_rpt_dt_cols = config['hist_rpt_dt_cols']

    if spark.catalog.tableExists(destination_table):
        fallback_max_base_ts = spark.table(destination_table) \
            .agg(F.max(ts).alias("max_base_ts")) \
            .first()["max_base_ts"]

        max_month_destination = spark.table(destination_table) \
            .agg(F.max(prt).alias("max_month")) \
            .first()["max_month"]

        min_month_destination = spark.table(destination_table) \
            .agg(F.min(prt).alias("min_month")) \
            .first()["min_month"]

        max_month_latest_history = spark.table(latest_summary_table) \
            .agg(F.max(prt).alias("max_month")) \
            .first()["max_month"]

        if max_month_destination != max_month_latest_history:
            raise ValueError(f"max_month_destination : {max_month_destination} and max_month_latest_history : {max_month_latest_history} does not match")                
    else:
        raise ValueError(f"{destination_table} does not exist")

    effective_max_base_ts, _, watermark_source = get_committed_ingestion_watermark(
        spark,
        config,
        fallback_max_base_ts,
        max_month_destination,
    )
    if effective_max_base_ts is None:
        effective_max_base_ts = datetime(1900, 1, 1)
    logger.info(
        f"Using ingestion watermark ({watermark_source}) "
        f"for source filter: {ts} > {effective_max_base_ts}"
    )

    # Load accounts
    logger.info(f"Loading accounts from {source_table}")
    if max_month_destination is None or min_month_destination is None:
        next_month = None
        accounts_df = spark.read.table(source_table).filter(F.col(ts) > F.lit(effective_max_base_ts))
        logging.info(f"Reading from {source_table} - {ts} > {effective_max_base_ts}")
    else:
        next_month = (datetime.strptime(max_month_destination, "%Y-%m") + relativedelta(months=1)).strftime("%Y-%m")
        accounts_df = spark.read.table(source_table)\
            .filter(
                    (F.col(prt) >= f"{min_month_destination}")
                    & (F.col(prt) < f"{next_month}")
                    & (F.col(ts) > F.lit(effective_max_base_ts))
                )
        
        logging.info(f"Reading from {source_table} - ({prt} < {next_month}) & ({ts} > {effective_max_base_ts})")

    # Load account_hist_rpt_dt
    logger.info(f"Loading account_hist_rpt_dt from {hist_rpt_dt_table}")
    hist_rpt_dt_df = spark.read.table(hist_rpt_dt_table).select(*hist_rpt_dt_cols).filter(F.col(ts) > F.lit(effective_max_base_ts))
    
    logging.info(f"Reading from {hist_rpt_dt_table} - {ts} > {effective_max_base_ts}")

    # Prepare source data (mappings, transformations, deduplication)
    accounts_prepared = prepare_source_data(accounts_df, hist_rpt_dt_df, config)

    # Add month integer for comparison
    accounts_prepared = accounts_prepared.withColumn(
        "month_int",
        F.expr(month_to_int_expr(prt))
    )
    accounts_prepared = accounts_prepared.withColumn(
        "_is_soft_delete",
        F.coalesce(F.col(SOFT_DELETE_COLUMN), F.lit("")).isin(*SOFT_DELETE_CODES)
    )

    # Load summary metadata (small - can broadcast)
    logger.info(f"Loading summary metadata from {latest_summary_table}")
    try:
        summary_meta = spark.sql(f"""
            SELECT 
                {pk},
                {prt} as max_existing_month,
                {ts} as max_existing_ts,
                {month_to_int_expr(prt)} as max_month_int
            FROM {latest_summary_table}
        """)

        logger.info(f"Loaded metadata for existing accounts")
    except Exception as e:
        logger.warning(f"Could not load summary metadata: {e}")
        # No existing summary - all are Case I
        return accounts_prepared.withColumn("case_type", F.lit("CASE_I"))

    # Classify records
    logger.info("Classifying accounts into Case I/II/III/IV")

    # Initial classification (before detecting bulk historical)
    initial_classified = (
        accounts_prepared.alias("n")
        .join(
            summary_meta.alias("s"),
            F.col(f"n.{pk}") == F.col(f"s.{pk}"),
            "left"
        )
        .select(
            F.col("n.*"),
            F.col("s.max_existing_month"),
            F.col("s.max_existing_ts"),
            F.col("s.max_month_int")
        )
        .withColumn(
            "initial_case_type",
            F.when(F.col("max_existing_month").isNull(), F.lit("CASE_I"))
            .when(F.col("month_int") > F.col("max_month_int"), F.lit("CASE_II"))
            .otherwise(F.lit("CASE_III"))
        )
    )

    # Detect bulk historical: new accounts with multiple months in batch
    # For Case I accounts, find the earliest month per account
    window_spec = Window.partitionBy(pk)
    classified = (
        initial_classified
        .withColumn(
            "min_month_for_new_account",
            F.when(
                F.col("initial_case_type") == "CASE_I",
                F.min("month_int").over(window_spec)
            )
        )
        .withColumn(
            "count_months_for_new_account",
            F.when(
                F.col("initial_case_type") == "CASE_I",
                F.count("*").over(window_spec)
            ).otherwise(F.lit(1))
        )
        .withColumn(
            "case_type",
            F.when(
                # Case I: New account with ONLY single month in batch
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") == 1),
                F.lit("CASE_I")
            ).when(
                # Case IV: New account with MULTIPLE months - this is the first/earliest month
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") > 1) &
                (F.col("month_int") == F.col("min_month_for_new_account")),
                F.lit("CASE_I")  # First month treated as Case I
            ).when(
                # Case IV: New account with MULTIPLE months - subsequent months
                (F.col("initial_case_type") == "CASE_I") & 
                (F.col("count_months_for_new_account") > 1) &
                (F.col("month_int") > F.col("min_month_for_new_account")),
                F.lit("CASE_IV")  # Bulk historical - will build on earlier months in batch
            ).otherwise(
                # Keep existing classification for Case II and III
                F.col("initial_case_type")
            )
        )
        .withColumn(
            "MONTH_DIFF",
            F.when(
                F.col("case_type") == "CASE_II",
                F.col("month_int") - F.col("max_month_int")
            ).when(
                # For Case IV, calculate diff from earliest month in batch
                F.col("case_type") == "CASE_IV",
                F.col("month_int") - F.col("min_month_for_new_account")
            ).otherwise(F.lit(1))
        )
        .drop("initial_case_type")
    )

    return classified


def process_case_i(case_i_df, config: Dict[str, Any]):
    """
    Process Case I - create initial arrays for new accounts
    
    Arrays are initialized as: [current_value, NULL, NULL, ..., NULL] (36 elements)
    """
    logger.info("=" * 80)
    logger.info("STEP 2a: Process Case I (New Accounts)")
    logger.info("=" * 80)

    logger.info(f"Processing new accounts")

    result = case_i_df
    history_len = get_summary_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])

    # Create initial arrays for each rolling column
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']

        # Array: [current_value, NULL, NULL, ..., NULL]
        null_array = ", ".join(["NULL" for _ in range(history_len - 1)])
        result = result.withColumn(
            array_name,
            F.expr(f"array({mapper_column}, {null_array})")
        )

    # Generate grid columns
    for gc in grid_columns:
        # Use 'mapper_rolling_column' to match original format
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))

        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )

    # Drop classification and temporary columns
    drop_cols = [
        "month_int",
        "max_existing_month",
        "max_existing_ts",
        "max_month_int",
        "_is_soft_delete",
        "case_type",
        "MONTH_DIFF",
        "min_month_for_new_account",
        "count_months_for_new_account",
    ]
    # Also drop prepared columns
    drop_cols.extend([c for c in result.columns if c.startswith("_prepared_")])
    result = result.drop(*[c for c in drop_cols if c in result.columns])

    return result


def process_case_ii(spark: SparkSession, case_ii_df, config: Dict[str, Any], expected_rows: Optional[int] = None):
    """
    Process Case II - shift existing arrays for forward entries
    
    Logic:
    - MONTH_DIFF == 1: [new_value, prev[0:35]]
    - MONTH_DIFF > 1:  [new_value, NULLs_for_gap, prev[0:35-gap]]
    """
    process_start_time = time.time()
    logger.info("=" * 80)
    logger.info("STEP 2b: Process Case II (Forward Entries)")
    logger.info("=" * 80)

    logger.info(f"Processing forward entries")

    pk = config['primary_column']
    prt = config['partition_column']
    latest_summary_table = config["latest_history_table"]
    history_len = get_summary_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])

    # Get affected accounts
    affected_keys = case_ii_df.select(pk).distinct()

    # Load latest summary for affected accounts
    logger.info(f"Loading latest summary for Affected Accounts")

    # Select needed columns from latest summary
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    latest_summary_cols = set(get_latest_cols(config))
    if not all(col in latest_summary_cols for col in history_cols):
        logger.warning("Case II requires *_history columns in latest_summary; skipping Case II processing")
        return

    # Create temp view for affected keys
    affected_keys.createOrReplaceTempView("case_ii_affected_keys")

    # Use latest_summary only (one row per account) and exclude deleted contexts.
    latest_base_df = (
        spark.table(latest_summary_table)
        .join(affected_keys, on=pk, how="inner")
        .filter(~F.coalesce(F.col(SOFT_DELETE_COLUMN), F.lit("")).isin(*SOFT_DELETE_CODES))
    )
    latest_select_exprs = [F.col(pk), F.col(prt)] + [F.col(c) for c in history_cols]
    latest_for_affected = latest_base_df.select(*latest_select_exprs).dropDuplicates([pk, prt]).withColumn(
        "prev_month_int",
        F.expr(month_to_int_expr(prt))
    )

    # Collect peer forward entries into MAP for gap filling (Chaining)
    # Map Key: month_int, Map Value: Struct(val_col1, val_col2...)
    val_struct_fields = []
    for rc in rolling_columns:
        src_col = rc.get('mapper_column', rc.get('source_column', rc['name']))
        prepared_col = f"_prepared_{rc['name']}"
        value_col = prepared_col if prepared_col in case_ii_df.columns else src_col
        val_struct_fields.append(F.col(value_col).alias(f"val_{rc['name']}"))

    peer_window = Window.partitionBy(pk)
    case_ii_df = case_ii_df.withColumn(
        "peer_map", 
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("month_int"), 
                    F.struct(*val_struct_fields)
                )
            ).over(peer_window)
        )
    )

    # Build SQL for array shifting (resolved accounts with non-deleted latest context)
    shift_exprs = []
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']
        prepared_col = f"_prepared_{rc['name']}"
        current_value_col = prepared_col if prepared_col in case_ii_df.columns else mapper_column
        val_col_name = f"val_{rc['name']}"
        month_diff_expr = "(c.month_int - p.prev_month_int)"

        shift_expr = f"""
            CASE
                WHEN {month_diff_expr} = 1 THEN
                    slice(concat(array(c.{current_value_col}), p.{array_name}), 1, {history_len})
                WHEN {month_diff_expr} > 1 THEN
                    slice(
                        concat(
                            array(c.{current_value_col}),
                            transform(
                                sequence(1, {month_diff_expr} - 1),
                                i -> c.peer_map[CAST(c.month_int - i AS INT)].{val_col_name}
                            ),
                            p.{array_name}
                        ),
                        1, {history_len}
                    )
                ELSE
                    concat(array(c.{current_value_col}), array_repeat(NULL, {history_len - 1}))
            END as {array_name}
        """
        shift_exprs.append(shift_expr)

    # Get non-array columns from current record
    exclude_cols = {
        "month_int",
        "max_existing_month",
        "max_existing_ts",
        "max_month_int",
        "_is_soft_delete",
        "case_type",
        "MONTH_DIFF",
        "min_month_for_new_account",
        "count_months_for_new_account",
        "peer_map"
    }
    exclude_cols.update(history_cols)
    exclude_cols.update([c for c in case_ii_df.columns if c.startswith("_prepared_")])
    current_cols = [c for c in case_ii_df.columns if c not in exclude_cols]
    current_select = ", ".join([f"c.{col}" for col in current_cols])

    latest_context_keys = latest_for_affected.select(pk).distinct().withColumn("_has_latest_context", F.lit(1))
    case_ii_with_context = case_ii_df.join(latest_context_keys, on=pk, how="left")
    case_ii_resolved = case_ii_with_context.filter(F.col("_has_latest_context").isNotNull()).drop("_has_latest_context")
    case_ii_unresolved = case_ii_with_context.filter(F.col("_has_latest_context").isNull()).drop("_has_latest_context")

    result_parts = []

    if not case_ii_resolved.isEmpty():
        case_ii_resolved.createOrReplaceTempView("case_ii_records")
        latest_for_affected.createOrReplaceTempView("latest_summary_affected")
        sql = f"""
            SELECT 
                {current_select},
                {', '.join(shift_exprs)}
            FROM case_ii_records c
            JOIN latest_summary_affected p ON c.{pk} = p.{pk}
        """
        result_parts.append(spark.sql(sql))

    # Accounts with no non-deleted latest context are treated as Case I style initialization.
    if not case_ii_unresolved.isEmpty():
        unresolved_select_exprs = [F.col(c) for c in current_cols]
        for rc in rolling_columns:
            array_name = f"{rc['name']}_history"
            mapper_column = rc['mapper_column']
            prepared_col = f"_prepared_{rc['name']}"
            value_col = prepared_col if prepared_col in case_ii_unresolved.columns else mapper_column
            value_type = case_ii_unresolved.schema[value_col].dataType
            init_history_expr = F.concat(
                F.array(F.col(value_col)),
                F.array_repeat(F.lit(None).cast(value_type), history_len - 1)
            )
            unresolved_select_exprs.append(
                init_history_expr.alias(array_name)
            )
        result_parts.append(case_ii_unresolved.select(*unresolved_select_exprs))

    if not result_parts:
        logger.warning("No Case II rows generated after context split")
        return

    if len(result_parts) == 1:
        result = result_parts[0]
    else:
        result = result_parts[0].unionByName(result_parts[1])

    # Generate grid columns
    for gc in grid_columns:
        # Use 'mapper_rolling_column' to match original format
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))

        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )

    write_case_table_bucketed(
        spark=spark,
        df=result,
        table_name="temp_catalog.checkpointdb.case_2",
        config=config,
        stage="case_2_temp",
        expected_rows=expected_rows,
    )

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case II Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
    logger.info("-" * 60)

    return


def process_case_iii_using_latest_history_context(
    spark: SparkSession,
    case_iii_df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> bool:
    """
    V4 path: build Case III rolling arrays from latest_summary history arrays.
    Avoids reading summary history arrays for Case III array generation.
    Returns True when handled; False to fallback to legacy path.
    """
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = get_case3_summary_context_table(spark, config)
    latest_summary_table = get_latest_context_table(spark, config)
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    case3_latest_patch_table = get_case3_latest_month_patch_table(config)

    if case_iii_df is None or case_iii_df.isEmpty():
        return True

    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    latest_cols = set(get_latest_cols(config))
    has_physical_history = all(c in latest_cols for c in history_cols)
    if not has_physical_history:
        logger.warning("Case III latest-history path requires *_history columns in latest_summary; falling back to legacy Case III")
        return False

    global_latest_month = (
        case_iii_df
        .agg(F.max("max_existing_month").alias("global_latest_month"))
        .first()["global_latest_month"]
    )
    if global_latest_month is None:
        return True

    case_iii_latest_df = case_iii_df.filter(F.col(prt) == F.lit(global_latest_month))
    split_counts = (
        case_iii_df
        .agg(
            F.sum(F.when(F.col(prt) == F.lit(global_latest_month), F.lit(1)).otherwise(F.lit(0))).alias("latest_count"),
            F.sum(F.when(F.col(prt) < F.lit(global_latest_month), F.lit(1)).otherwise(F.lit(0))).alias("older_count"),
        )
        .first()
    )
    latest_count = int(split_counts["latest_count"] or 0)
    older_count = int(split_counts["older_count"] or 0)
    logger.info(
        f"Case III latest-history path selected for latest_month={global_latest_month}"
    )

    # Keep latest-month patch behavior for summary table (index-0 update on current latest month rows).
    if latest_count > 0:
        latest_dedupe_window = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
        case_iii_latest_dedup = (
            case_iii_latest_df
            .withColumn("_rn", F.row_number().over(latest_dedupe_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        temp_exclusions = {
            "month_int", "max_existing_month", "max_existing_ts", "max_month_int",
            "_is_soft_delete", "case_type", "MONTH_DIFF", "min_month_for_new_account",
            "count_months_for_new_account",
        }
        latest_source_cols = [
            c for c in case_iii_latest_dedup.columns
            if c not in temp_exclusions and not c.startswith("_prepared_")
        ]
        latest_value_cols = [
            F.col(rc['mapper_column']).alias(f"latest_val_{rc['name']}")
            for rc in rolling_columns
        ]
        latest_patch_df = case_iii_latest_dedup.select(
            *[F.col(c) for c in latest_source_cols],
            *latest_value_cols,
        )
        write_case_table_bucketed(
            spark=spark,
            df=latest_patch_df,
            table_name=case3_latest_patch_table,
            config=config,
            stage=f"{case3_latest_patch_table.split('.')[-1]}_temp",
            expected_rows=latest_count,
        )
        logger.info(f"Case III latest-month patch table generated: {case3_latest_patch_table}")

    # Build peer map for all Case III rows (latest + older) for overwrite precedence.
    backfill_all = case_iii_df
    if "month_int" not in backfill_all.columns:
        backfill_all = backfill_all.withColumn("month_int", F.expr(month_to_int_expr(prt)))
    val_struct_fields = []
    for rc in rolling_columns:
        mapper_column = rc['mapper_column']
        val_struct_fields.append(F.col(mapper_column).alias(f"val_{rc['name']}"))
    peer_window = Window.partitionBy(pk)
    backfill_all = backfill_all.withColumn(
        "peer_map",
        F.map_from_entries(
            F.collect_list(
                F.struct(F.col("month_int"), F.struct(*val_struct_fields))
            ).over(peer_window)
        )
    )
    peer_per_account = backfill_all.select(pk, "peer_map").dropDuplicates([pk])

    account_stats = (
        backfill_all.groupBy(pk)
        .agg(
            F.min("month_int").alias("min_backfill_int"),
            F.max("month_int").alias("max_backfill_int"),
            F.max(F.col(ts)).alias("max_backfill_ts"),
        )
    )

    affected_accounts = case_iii_df.select(pk).distinct()
    latest_select_exprs = [F.col(pk), F.col(prt), F.col(ts)] + [F.col(c) for c in history_cols]
    latest_ctx = (
        spark.table(latest_summary_table)
        .select(*latest_select_exprs, F.col(SOFT_DELETE_COLUMN))
        .join(affected_accounts, on=pk, how="inner")
        .filter(~F.coalesce(F.col(SOFT_DELETE_COLUMN), F.lit("")).isin(*SOFT_DELETE_CODES))
        .drop(SOFT_DELETE_COLUMN)
        .withColumn("latest_month_int", F.expr(month_to_int_expr(prt)))
    )
    if latest_ctx.isEmpty():
        logger.warning("No non-deleted latest_summary context for Case III; falling back to legacy Case III")
        return False

    latest_patch_base = (
        latest_ctx.join(peer_per_account, on=pk, how="inner")
        .join(account_stats, on=pk, how="inner")
        .withColumn(
            "required_size",
            F.greatest(
                F.size(F.col(history_cols[0])),
                F.col("latest_month_int") - F.col("min_backfill_int") + F.lit(1),
                F.lit(latest_history_len),
            ),
        )
    )
    latest_patch_base.createOrReplaceTempView("case3_latest_patch_base")

    history_patch_exprs = []
    for rc in rolling_columns:
        history_col = f"{rc['name']}_history"
        val_col = f"val_{rc['name']}"
        dtype = rc.get('type', rc.get('data_type', 'String')).upper()
        ub_expr = f"""
            transform(
                sequence(0, required_size - 1),
                i -> CASE
                    WHEN peer_map[CAST(latest_month_int - i AS INT)] IS NOT NULL
                    THEN CAST(peer_map[CAST(latest_month_int - i AS INT)].{val_col} AS {dtype})
                    ELSE element_at(
                        concat(
                            {history_col},
                            array_repeat(CAST(NULL AS {dtype}), greatest(required_size - size({history_col}), 0))
                        ),
                        i + 1
                    )
                END
            ) AS {history_col}
        """
        history_patch_exprs.append(ub_expr)

    latest_patch_sql = f"""
        SELECT
            {pk},
            {prt},
            GREATEST({ts}, max_backfill_ts) AS {ts},
            {', '.join(history_patch_exprs)}
        FROM case3_latest_patch_base
    """
    latest_history_patch_df = spark.sql(latest_patch_sql)
    for rc in rolling_columns:
        latest_history_patch_df = latest_history_patch_df.withColumn(
            f"{rc['name']}_history",
            F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
        )
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        latest_history_patch_df = latest_history_patch_df.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.slice(F.col(source_history), 1, history_len),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )
    write_case_table_bucketed(
        spark=spark,
        df=latest_history_patch_df,
        table_name="temp_catalog.checkpointdb.case_3_latest_history_context_patch",
        config=config,
        stage="case_3_latest_history_context_patch_temp",
        expected_rows=expected_rows,
    )

    if older_count == 0:
        logger.info("Case III latest-history processing completed (latest-month only)")
        return True

    older_with_peer = backfill_all.filter(F.col(prt) < F.lit(global_latest_month))
    older_ctx = (
        older_with_peer.alias("c")
        .join(
            latest_ctx.select(pk, "latest_month_int", *history_cols).alias("l"),
            on=pk,
            how="inner",
        )
    )
    older_ctx.createOrReplaceTempView("case3_older_ctx")

    exclude_backfill = {
        "month_int", "max_existing_month", "max_existing_ts", "max_month_int",
        "_is_soft_delete", "case_type", "MONTH_DIFF", "min_month_for_new_account",
        "count_months_for_new_account",
    }
    exclude_backfill.update([c for c in case_iii_df.columns if c.startswith("_prepared_")])
    base_cols = [f"c.{c}" for c in case_iii_df.columns if c not in exclude_backfill]

    case3a_exprs = []
    for rc in rolling_columns:
        bounded_col = f"{rc['name']}_history"
        history_col = f"{rc['name']}_history"
        val_col = f"val_{rc['name']}"
        dtype = rc.get('type', rc.get('data_type', 'String')).upper()
        case3a_exprs.append(
            f"""
                transform(
                    sequence(0, {history_len - 1}),
                    pos -> CASE
                        WHEN c.peer_map[CAST(c.month_int - pos AS INT)] IS NOT NULL
                        THEN CAST(c.peer_map[CAST(c.month_int - pos AS INT)].{val_col} AS {dtype})
                        ELSE element_at(c.{history_col}, (c.latest_month_int - c.month_int + pos + 1))
                    END
                ) AS {bounded_col}
            """
        )
    case3a_sql = f"""
        SELECT
            {', '.join(base_cols)},
            {', '.join(case3a_exprs)}
        FROM case3_older_ctx c
    """
    case3a_df = spark.sql(case3a_sql)
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        case3a_df = case3a_df.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )
    case3a_df = (
        case3a_df
        .withColumn("_rn", F.row_number().over(Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    write_case_table_bucketed(
        spark=spark,
        df=case3a_df,
        table_name="temp_catalog.checkpointdb.case_3a",
        config=config,
        stage="case_3a_temp",
        expected_rows=expected_rows,
    )

    summary_targets = (
        spark.table(summary_table)
        .select(pk, prt, ts)
        .join(account_stats, on=pk, how="inner")
        .withColumn("summary_month_int", F.expr(month_to_int_expr(prt)))
        .filter(
            (F.col("summary_month_int") >= F.col("min_backfill_int"))
            & (F.col("summary_month_int") <= F.col("max_backfill_int") + F.lit(history_len - 1))
        )
    )
    future_ctx = (
        summary_targets.alias("s")
        .join(latest_ctx.select(pk, "latest_month_int", *history_cols).alias("l"), on=pk, how="inner")
        .join(peer_per_account.alias("p"), on=pk, how="inner")
        .filter(F.col("summary_month_int") <= F.col("latest_month_int"))
    )
    future_ctx.createOrReplaceTempView("case3_future_ctx")
    case3b_exprs = []
    for rc in rolling_columns:
        bounded_col = f"{rc['name']}_history"
        history_col = f"{rc['name']}_history"
        val_col = f"val_{rc['name']}"
        dtype = rc.get('type', rc.get('data_type', 'String')).upper()
        case3b_exprs.append(
            f"""
                transform(
                    sequence(0, {history_len - 1}),
                    pos -> CASE
                        WHEN c.peer_map[CAST(c.summary_month_int - pos AS INT)] IS NOT NULL
                        THEN CAST(c.peer_map[CAST(c.summary_month_int - pos AS INT)].{val_col} AS {dtype})
                        ELSE element_at(c.{history_col}, (c.latest_month_int - c.summary_month_int + pos + 1))
                    END
                ) AS {bounded_col}
            """
        )
    case3b_sql = f"""
        SELECT
            c.{pk} AS {pk},
            c.{prt} AS {prt},
            GREATEST(c.{ts}, c.max_backfill_ts) AS {ts},
            {', '.join(case3b_exprs)}
        FROM case3_future_ctx c
    """
    case3b_df = spark.sql(case3b_sql)
    if not case3b_df.isEmpty():
        for gc in grid_columns:
            source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
            source_history = f"{source_rolling}_history"
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('seperator', gc.get('separator', ''))
            case3b_df = case3b_df.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.col(source_history),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )
        write_case_table_bucketed(
            spark=spark,
            df=case3b_df,
            table_name="temp_catalog.checkpointdb.case_3b",
            config=config,
            stage="case_3b_temp",
            expected_rows=expected_rows,
        )
    return True


def process_case_iii(spark: SparkSession, case_iii_df, config: Dict[str, Any], expected_rows: Optional[int] = None):
    """
    Process Case III - rebuild history arrays for backfill
    1. Creates NEW summary rows for backfill months (with inherited history)
    2. Updates ALL future summary rows with backfill data
    
    Logic:
    A. For each backfill record, find the closest PRIOR summary
    B. Create a new summary row for the backfill month with:
       - Position 0: backfill data
       - Position 1-N: shifted data from prior summary (based on month gap)
    C. Update all FUTURE summaries with backfill data at correct position
    """
    process_start_time = time.time()

    logger.info("=" * 80)
    logger.info("STEP 2c: Process Case III (Backfill)")
    logger.info("=" * 80)

    logger.info(f"CASE III - Processing backfill records")

    if case_iii_df is None or case_iii_df.isEmpty():
        logger.info("Case III skipped: no records")
        return

    pk = config['primary_column']
    split_enabled = bool(config.get("enable_case3_hot_cold_split", True))
    if split_enabled and not bool(config.get("_case3_split_internal", False)):
        prt = config['partition_column']
        hot_window = max(1, int(config.get("case3_hot_window_months", 36)))
        split_df = case_iii_df
        if "month_int" not in split_df.columns:
            split_df = split_df.withColumn("month_int", F.expr(month_to_int_expr(prt)))

        global_latest_month = (
            split_df
            .agg(F.max("max_existing_month").alias("global_latest_month"))
            .first()["global_latest_month"]
        )
        if global_latest_month is not None:
            latest_int = int(global_latest_month[:4]) * 12 + int(global_latest_month[5:7])
            hot_cutoff_int = latest_int - (hot_window - 1)
            split_counts = (
                split_df
                .agg(
                    F.sum(F.when(F.col("month_int") >= F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("hot_count"),
                    F.sum(F.when(F.col("month_int") < F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("cold_count"),
                )
                .first()
            )
            hot_count = int(split_counts["hot_count"] or 0)
            cold_count = int(split_counts["cold_count"] or 0)
            logger.info(
                f"Case III split active for latest_month={global_latest_month} with hot_window={hot_window}"
            )

            hot_df = split_df.filter(F.col("month_int") >= F.lit(hot_cutoff_int))
            cold_df = split_df.filter(F.col("month_int") < F.lit(hot_cutoff_int))

            has_account_overlap = False
            overlap_accounts = None
            if hot_count > 0 and cold_count > 0:
                overlap_accounts = hot_df.select(pk).intersect(cold_df.select(pk)).distinct()
                has_account_overlap = overlap_accounts.limit(1).count() > 0

            force_unified_on_overlap = bool(config.get("force_case3_unified_on_any_overlap", False))
            if has_account_overlap and force_unified_on_overlap:
                logger.info(
                    "Case III split safety fallback activated on overlap; using unified processing path"
                )
                unified_cfg = dict(config)
                unified_cfg["_case3_split_internal"] = True
                unified_cfg["use_latest_history_context_case3"] = False
                unified_cfg["_case3_latest_month_patch_table"] = CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE
                unified_cfg["force_cold_case3_broadcast"] = bool(
                    config.get("force_cold_case3_broadcast", True)
                )
                process_case_iii(
                    spark,
                    split_df,
                    unified_cfg,
                    expected_rows=hot_count + cold_count,
                )
                return

            if has_account_overlap:
                mixed_df = split_df.join(overlap_accounts, on=pk, how="left_semi")
                hot_only_df = hot_df.join(overlap_accounts, on=pk, how="left_anti")
                cold_only_df = cold_df.join(overlap_accounts, on=pk, how="left_anti")
                logger.info(
                    "Case III split mode: account-level lanes active (mixed + hot-only + cold-only)"
                )
            else:
                mixed_df = None
                hot_only_df = hot_df
                cold_only_df = cold_df
                logger.info("Case III split mode: hot-only + cold-only lanes (no account overlap)")

            split_case_tables = {
                "temp_catalog.checkpointdb.case_3a": "case_3a_temp_split_combine",
                "temp_catalog.checkpointdb.case_3b": "case_3b_temp_split_combine",
                CASE3_LATEST_MONTH_PATCH_TABLE: "case_3_latest_month_patch_temp_split_combine",
                CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE: "case_3_unified_latest_month_patch_temp_split_combine",
                "temp_catalog.checkpointdb.case_3_latest_history_context_patch": "case_3_latest_history_context_patch_temp_split_combine",
            }
            snapshot_groups: List[Dict[str, str]] = []

            if mixed_df is not None and not mixed_df.isEmpty():
                drop_case_tables(spark, split_case_tables)
                mixed_cfg = dict(config)
                mixed_cfg["_case3_split_internal"] = True
                mixed_cfg["use_latest_history_context_case3"] = False
                mixed_cfg["_case3_latest_month_patch_table"] = CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE
                mixed_cfg["force_cold_case3_broadcast"] = bool(
                    config.get("force_cold_case3_broadcast", True)
                )
                process_case_iii(
                    spark,
                    mixed_df,
                    mixed_cfg,
                    expected_rows=hot_count + cold_count,
                )
                snapshot_groups.append(
                    snapshot_case_tables(spark, split_case_tables, "mixed_lane_case3")
                )

            if hot_only_df is not None and not hot_only_df.isEmpty():
                drop_case_tables(spark, split_case_tables)
                hot_cfg = dict(config)
                hot_cfg["_case3_split_internal"] = True
                hot_cfg["use_latest_history_context_case3"] = True
                hot_cfg["_case3_latest_month_patch_table"] = CASE3_LATEST_MONTH_PATCH_TABLE
                hot_handled = process_case_iii_using_latest_history_context(
                    spark,
                    hot_only_df,
                    hot_cfg,
                    expected_rows=hot_count,
                )
                if not hot_handled:
                    logger.info("Case III hot-only lane fell back to legacy processing")
                    hot_fallback_cfg = dict(config)
                    hot_fallback_cfg["_case3_split_internal"] = True
                    hot_fallback_cfg["use_latest_history_context_case3"] = False
                    process_case_iii(
                        spark,
                        hot_only_df,
                        hot_fallback_cfg,
                        expected_rows=hot_count,
                    )
                snapshot_groups.append(
                    snapshot_case_tables(spark, split_case_tables, "hot_lane_case3")
                )

            if cold_only_df is not None and not cold_only_df.isEmpty():
                drop_case_tables(spark, split_case_tables)
                cold_cfg = dict(config)
                cold_cfg["_case3_split_internal"] = True
                cold_cfg["use_latest_history_context_case3"] = False
                cold_cfg["_case3_latest_month_patch_table"] = CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE
                cold_cfg["force_cold_case3_broadcast"] = bool(config.get("force_cold_case3_broadcast", True))
                process_case_iii(
                    spark,
                    cold_only_df,
                    cold_cfg,
                    expected_rows=cold_count,
                )
                snapshot_groups.append(
                    snapshot_case_tables(spark, split_case_tables, "cold_lane_case3")
                )

            combine_case_table_snapshots(
                spark=spark,
                config=config,
                table_stage_map=split_case_tables,
                snapshot_groups=snapshot_groups,
                expected_rows=expected_rows,
            )
            cleanup_snapshot_tables(spark, snapshot_groups)
            return

    if config.get("use_latest_history_context_case3", True):
        if process_case_iii_using_latest_history_context(spark, case_iii_df, config, expected_rows=expected_rows):
            return
        logger.info("Case III latest-history path fell back to legacy mode")

    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = get_case3_summary_context_table(spark, config)
    history_len = get_summary_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    delete_codes_sql = ",".join([f"'{code}'" for code in SOFT_DELETE_CODES])
    case3_latest_patch_table = get_case3_latest_month_patch_table(config)

    global_latest_month = (
        case_iii_df
        .agg(F.max("max_existing_month").alias("global_latest_month"))
        .first()["global_latest_month"]
    )
    if global_latest_month is None:
        logger.info("Case III skipped: no existing latest month metadata available")
        return

    case_iii_latest_df = case_iii_df.filter(F.col(prt) == F.lit(global_latest_month))
    case_iii_older_df = case_iii_df.filter(F.col(prt) < F.lit(global_latest_month))

    split_counts = (
        case_iii_df
        .agg(
            F.sum(F.when(F.col(prt) == F.lit(global_latest_month), F.lit(1)).otherwise(F.lit(0))).alias("latest_count"),
            F.sum(F.when(F.col(prt) < F.lit(global_latest_month), F.lit(1)).otherwise(F.lit(0))).alias("older_count"),
        )
        .first()
    )
    latest_count = int(split_counts["latest_count"] or 0)
    older_count = int(split_counts["older_count"] or 0)
    logger.info(
        f"Case III split by month: latest_month={global_latest_month}, "
        f"latest_rows={latest_count:,}, older_rows={older_count:,}"
    )

    if bool(config.get("force_cold_case3_broadcast", False)) and older_count > 0:
        row_cap = int(config.get("cold_case3_broadcast_row_cap", 10_000_000))
        if older_count > row_cap:
            raise ValueError(
                f"Cold Case III broadcast guard failed: older_rows={older_count:,} exceeds cap={row_cap:,}"
            )
        case_iii_older_df = F.broadcast(case_iii_older_df)
        logger.info(f"Forced broadcast enabled for cold Case III input (rows={older_count:,}, cap={row_cap:,})")

    if latest_count > 0:
        logger.info("Case III Part 2: building latest-month patch rows")
        latest_dedupe_window = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
        case_iii_latest_dedup = (
            case_iii_latest_df
            .withColumn("_rn", F.row_number().over(latest_dedupe_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        temp_exclusions = {
            "month_int",
            "max_existing_month",
            "max_existing_ts",
            "max_month_int",
            "_is_soft_delete",
            "case_type",
            "MONTH_DIFF",
            "min_month_for_new_account",
            "count_months_for_new_account",
        }
        latest_source_cols = [
            c for c in case_iii_latest_dedup.columns
            if c not in temp_exclusions and not c.startswith("_prepared_")
        ]

        latest_value_cols = [
            F.col(rc['mapper_column']).alias(f"latest_val_{rc['name']}")
            for rc in rolling_columns
        ]

        latest_patch_df = case_iii_latest_dedup.select(
            *[F.col(c) for c in latest_source_cols],
            *latest_value_cols,
        )

        write_case_table_bucketed(
            spark=spark,
            df=latest_patch_df,
            table_name=case3_latest_patch_table,
            config=config,
            stage=f"{case3_latest_patch_table.split('.')[-1]}_temp",
            expected_rows=latest_count,
        )
        logger.info(f"Case III Part 2 generated: {case3_latest_patch_table}")
        logger.info("-" * 60)
    else:
        logger.info("Case III Part 2 skipped: no latest-month rows")
        logger.info("-" * 60)

    if older_count == 0:
        logger.info("Case III Part 1 skipped: no older-month backfill rows")
        return

    # Get affected accounts
    affected_accounts = case_iii_older_df.select(pk).distinct()

    # Get the min/max backfill months to calculate partition range
    backfill_range = case_iii_older_df.agg(
        F.min(prt).alias("min_backfill_month"),
        F.max(prt).alias("max_backfill_month")
    ).first()

    min_backfill = backfill_range['min_backfill_month']
    max_backfill = backfill_range['max_backfill_month']

    logger.info(f"Backfill month range: {min_backfill} to {max_backfill}")

    # Calculate the partition range we need to read
    # We need prior months (up to 36 months before min backfill) for history inheritance
    # We need future months (up to 36 months after max backfill) for updating
    # Convert to month_int for calculation
    min_backfill_int = int(min_backfill[:4]) * 12 + int(min_backfill[5:7])
    max_backfill_int = int(max_backfill[:4]) * 12 + int(max_backfill[5:7])

    # Calculate boundary months
    earliest_needed_int = min_backfill_int - history_len
    latest_needed_int = max_backfill_int + history_len

    earliest_year = earliest_needed_int // 12
    earliest_month = earliest_needed_int % 12
    if earliest_month == 0:
        earliest_month = 12
        earliest_year -= 1
    earliest_partition = f"{earliest_year}-{earliest_month:02d}"

    latest_year = latest_needed_int // 12
    latest_month = latest_needed_int % 12
    if latest_month == 0:
        latest_month = 12
        latest_year -= 1
    latest_partition = f"{latest_year}-{latest_month:02d}"

    logger.info(f"Reading summary partitions from {earliest_partition} to {latest_partition}")

    # Load history columns list
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]

    # Build select for summary - include all columns we need
    summary_select_cols = [pk, prt, ts] + history_cols 

    # PARTITION-PRUNED READ: Only read the partitions we need with partition filter to leverage Iceberg's partition pruning
    partition_filter_sql = f"""
        SELECT {', '.join(summary_select_cols)}
        FROM {summary_table}
        WHERE {prt} >= '{earliest_partition}' AND {prt} <= '{latest_partition}'
          AND COALESCE({SOFT_DELETE_COLUMN}, '') NOT IN ({delete_codes_sql})
    """
    summary_df = spark.sql(partition_filter_sql)

    logger.info(f"Applied partition filter: {prt} BETWEEN '{earliest_partition}' AND '{latest_partition}'")

    summary_filtered = summary_df.join(
        F.broadcast(affected_accounts),
        pk,
        "left_semi"
    )

    # =========================================================================
    # Pre-calculate peer backfills for gap filling (A+B Combined)
    # =========================================================================
    # Collect all backfill values for the account into a MAP to fill gaps immediately
    # Map Key: month_int, Map Value: Struct(val_col1, val_col2...)

    val_struct_fields = []
    for rc in rolling_columns:
        mapper_column = rc['mapper_column']
        val_struct_fields.append(F.col(mapper_column).alias(f"val_{rc['name']}"))

    peer_window = Window.partitionBy(pk)
    case_iii_with_peers = case_iii_older_df.withColumn(
        "peer_map", 
        F.map_from_entries(
            F.collect_list(
                F.struct(
                    F.col("month_int"), 
                    F.struct(*val_struct_fields)
                )
            ).over(peer_window)
        )
    )

    summary_filtered = summary_filtered.withColumn("month_int",F.expr(month_to_int_expr(prt)))

    # Create temp views
    case_iii_with_peers.createOrReplaceTempView("backfill_records")
    summary_filtered.createOrReplaceTempView("summary_affected")

    case_iii_with_peers.cache()
    summary_filtered.cache()

    # =========================================================================
    # PART A: Create NEW summary rows for backfill months
    # =========================================================================
    logger.info("Part A: Creating new summary rows for backfill months...")

    # Find the closest PRIOR summary for each backfill record
    prior_summary_joined = spark.sql(f"""
        SELECT /*+ BROADCAST(b) */
            b.*,
            s.{prt} as prior_month,
            s.month_int as prior_month_int,            
            s.{ts} as prior_ts,
            {', '.join([f's.{arr} as prior_{arr}' for arr in history_cols])},
            (
                b.month_int - s.month_int
            ) as months_since_prior,
            ROW_NUMBER() OVER (
                PARTITION BY b.{pk}, b.{prt} 
                ORDER BY s.month_int DESC
            ) as rn
        FROM backfill_records b
        LEFT JOIN summary_affected s 
            ON b.{pk} = s.{pk} 
            AND s.month_int < b.month_int
    """)

    # Keep only the closest prior summary (rn = 1)
    prior_summary = prior_summary_joined.filter(F.col("rn") == 1).drop("rn")
    prior_summary.createOrReplaceTempView("backfill_with_prior")

    # Build expressions for creating new summary row
    new_row_exprs = []
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']
        val_col_name = f"val_{rc['name']}"

        # Logic:
        # 1. Start with current value
        # 2. Fill Gap: Use transform on sequence to lookup peer_map
        #    Map lookup returns NULL if key not found (for gaps)
        # 3. Append Prior History - AND PATCH IT with peer_map to resolve conflicts

        new_row_expr = f"""
            CASE
                WHEN prior_month IS NOT NULL AND months_since_prior > 0 THEN
                    slice(
                        concat(
                            array({mapper_column}),
                            CASE 
                                WHEN months_since_prior > 1 THEN 
                                    transform(
                                        sequence(1, months_since_prior - 1),
                                        i -> peer_map[CAST(month_int - i AS INT)].{val_col_name}
                                    )
                                ELSE array()
                            END,
                            transform(
                                prior_{array_name},
                                (val, i) -> CASE 
                                    WHEN peer_map[CAST(prior_month_int - i AS INT)] IS NOT NULL
                                    THEN peer_map[CAST(prior_month_int - i AS INT)].{val_col_name}
                                    ELSE val
                                END
                            )
                        ),
                        1, {history_len}
                    )
                ELSE
                    concat(
                        array({mapper_column}),
                        transform(
                            sequence(1, {history_len} - 1),
                            i -> peer_map[CAST(month_int - i AS INT)].{val_col_name}
                        )
                    )
            END as {array_name}
        """
        new_row_exprs.append(new_row_expr)

    # Get columns from backfill record (excluding temp columns)
    exclude_backfill = {
        "month_int",
        "max_existing_month",
        "max_existing_ts",
        "max_month_int",
        "_is_soft_delete",
        "case_type",
        "MONTH_DIFF",
        "prior_month",
        "prior_ts",
        "months_since_prior",
        "min_month_for_new_account",
        "count_months_for_new_account",
    }
    exclude_backfill.update([f"prior_{arr}" for arr in history_cols])
    exclude_backfill.update([c for c in case_iii_df.columns if c.startswith("_prepared_")])

    backfill_cols = [c for c in case_iii_df.columns if c not in exclude_backfill]
    backfill_select = ", ".join(backfill_cols)

    new_rows_sql = f"""
        SELECT 
            {backfill_select},
            {', '.join(new_row_exprs)}
        FROM backfill_with_prior
    """

    new_backfill_rows = spark.sql(new_rows_sql)

    # Generate grid columns for new rows
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))

        new_backfill_rows = new_backfill_rows.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )

    # Deduplicate new backfill rows - keep only one row per account+month
    window_spec = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
    new_backfill_rows = (
        new_backfill_rows
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    write_case_table_bucketed(
        spark=spark,
        df=new_backfill_rows,
        table_name="temp_catalog.checkpointdb.case_3a",
        config=config,
        stage="case_3a_temp",
        expected_rows=expected_rows,
    )

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case III Part A Generated | Time Elapsed: {process_total_minutes:.2f} minutes")    
    logger.info("-" * 60)


    # =========================================================================
    # PART B: Update FUTURE summary rows with backfill data
    # =========================================================================
    logger.info("Part B: Updating future summary rows...")
    process_start_time = time.time()

    # Build value column references for backfill
    backfill_value_cols = []
    for rc in rolling_columns:
        array_name = f"{rc['name']}_history"
        mapper_column = rc['mapper_column']
        backfill_value_cols.append(f'b.{mapper_column} as backfill_{rc["name"]}')

    # Join backfill with future summaries
    future_joined = spark.sql(f"""
        SELECT /*+ BROADCAST(b) */
            b.{pk},
            b.{ts} as backfill_ts,
            {', '.join(backfill_value_cols)},
            s.{prt} as summary_month,
            s.{ts} as summary_ts,
            {', '.join([f's.{arr} as existing_{arr}' for arr in history_cols])},
            (
                s.month_int - b.month_int
            ) as backfill_position
        FROM backfill_records b
        JOIN summary_affected s ON b.{pk} = s.{pk}
        WHERE (s.month_int >= b.month_int)
          AND (s.month_int - b.month_int) < {history_len}
    """)


    backfill_updates_df = None

    if not future_joined.isEmpty():
        future_joined.createOrReplaceTempView("future_backfill_joined")

        # =====================================================================
        # MULTIPLE BACKFILL
        # When multiple backfills arrive for the same account in the same batch,
        # we need to MERGE all their updates into the future summary rows.
        #
        # Strategy:
        # 1. Collect all (position, value) pairs for each account+month+array
        # 2. Create a map from positions to values
        # 3. Apply all updates in one pass using transform()
        # =====================================================================

        # First, collect backfill positions and values per account+month
        # We need to aggregate: for each (pk, summary_month), collect all backfills
        backfill_collect_cols = [f"backfill_{rc['name']}" for rc in rolling_columns]
        # Create a struct with position and all backfill values, then collect as array
        struct_fields = ["backfill_position"] + backfill_collect_cols

        # For each account+month, collect all backfill structs and the existing arrays
        # We use first() for existing arrays since they're the same for all rows of same account+month
        agg_exprs = [
            F.collect_list(F.struct(*[F.col(c) for c in struct_fields])).alias("backfill_list"),
            # Propagate latest source timestamp to future summaries updated by backfill
            F.max(F.col("backfill_ts")).alias("new_base_ts"),
            F.first(F.col("summary_ts")).alias("existing_summary_ts")
        ]
        for rc in rolling_columns:
            array_name = f"{rc['name']}_history"
            agg_exprs.append(F.first(f"existing_{array_name}").alias(f"existing_{array_name}"))

        aggregated_df = future_joined.groupBy(pk, "summary_month").agg(*agg_exprs)
        aggregated_df.createOrReplaceTempView("aggregated_backfills")

        # Build array update expressions that apply ALL collected backfills
        # For each position in the array, check if any backfill targets that position
        update_exprs = []
        for rc in rolling_columns:
            array_name = f"{rc['name']}_history"
            backfill_col = f"backfill_{rc['name']}"

            # Use filter to find matching backfill for position i
            # If match found, use its value (even if NULL). If no match, keep existing x.
            update_expr = f"""
                transform(
                    existing_{array_name},
                    (x, i) -> CASE 
                        WHEN size(filter(backfill_list, b -> b.backfill_position = i)) > 0
                        THEN element_at(filter(backfill_list, b -> b.backfill_position = i), 1).{backfill_col}
                        ELSE x
                    END
                ) as {array_name}
            """
            update_exprs.append(update_expr)

        # Create DataFrame for backfill updates with all backfills merged
        backfill_updates_df = spark.sql(f"""
            SELECT 
                {pk},
                summary_month as {prt},
                GREATEST(existing_summary_ts, new_base_ts) as {ts},
                {', '.join(update_exprs)}
            FROM aggregated_backfills
        """)


    # Generate grid columns for backfill_updates_df (for direct UPDATE)
    if backfill_updates_df is not None:
        for gc in grid_columns:
            source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
            source_history = f"{source_rolling}_history"
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('seperator', gc.get('separator', ''))

            backfill_updates_df = backfill_updates_df.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.col(source_history),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )

    if backfill_updates_df is not None:
        write_case_table_bucketed(
            spark=spark,
            df=backfill_updates_df,
            table_name="temp_catalog.checkpointdb.case_3b",
            config=config,
            stage="case_3b_temp",
            expected_rows=expected_rows,
        )
        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Case III Part B Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
    else:
        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Case III Part B Skipped (No Future Updates) | Time Elapsed: {process_total_minutes:.2f} minutes")
    logger.info("-" * 60)

    case_iii_with_peers.unpersist()
    summary_filtered.unpersist()

    return


def process_case_iii_soft_delete_using_latest_history_context(
    spark: SparkSession,
    case_iii_delete_df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
) -> bool:
    """
    Hot-lane soft-delete processing using latest_summary history context.

    This path avoids broad summary range scans by deriving future-month patches from
    latest_summary context for touched accounts. Returns False when context is not
    usable, so caller can fall back to legacy behavior safely.
    """
    if case_iii_delete_df is None or case_iii_delete_df.isEmpty():
        return True

    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = get_case3_summary_context_table(spark, config)
    latest_summary_table = get_latest_context_table(spark, config)
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    latest_cols = set(get_latest_cols(config))
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    case3d_latest_history_patch_table = get_case3d_latest_history_patch_table(config)

    if not all(c in latest_cols for c in history_cols):
        logger.info(
            "Case III Soft Delete latest-context path unavailable: latest_summary missing required history columns"
        )
        return False

    delete_df = case_iii_delete_df.withColumn("delete_month_int", F.expr(month_to_int_expr(prt)))
    affected_accounts = delete_df.select(pk).distinct()

    latest_ctx = (
        spark.table(latest_summary_table)
        .select(pk, prt, ts, *history_cols)
        .join(affected_accounts, pk, "inner")
        .withColumn("latest_month_int", F.expr(month_to_int_expr(prt)))
    )

    if latest_ctx.isEmpty():
        logger.info("Case III Soft Delete latest-context path unavailable: no matching latest_summary rows")
        return False

    # Keep only delete rows that already exist in summary month table.
    delete_months = delete_df.select(prt).distinct()
    summary_month_keys = (
        spark.table(summary_table)
        .select(pk, prt)
        .join(F.broadcast(delete_months), prt, "inner")
    )
    delete_existing = (
        delete_df.alias("d")
        .join(
            summary_month_keys.alias("s"),
            (F.col(f"d.{pk}") == F.col(f"s.{pk}")) & (F.col(f"d.{prt}") == F.col(f"s.{prt}")),
            "inner",
        )
        .select("d.*")
    )

    if delete_existing.isEmpty():
        logger.info("No existing summary rows matched soft-delete records; nothing to update")
        return True

    # Part A: month-row flag updates (soft_del_cd + base_ts only)
    delete_month_update_df = delete_existing.select(
        F.col(pk),
        F.col(prt),
        F.col(ts),
        F.col(SOFT_DELETE_COLUMN),
    )
    write_case_table_bucketed(
        spark=spark,
        df=delete_month_update_df,
        table_name="temp_catalog.checkpointdb.case_3d_month",
        config=config,
        stage="case_3d_month_temp",
        expected_rows=expected_rows,
    )
    logger.info("Case III Soft Delete - Month-row updates generated (context path)")

    # Part B: future month patches for summary derived from latest_summary context
    future_candidates = (
        delete_existing.alias("d")
        .join(latest_ctx.select(pk, "latest_month_int").alias("l"), pk, "inner")
        .filter(F.col("l.latest_month_int") > F.col("d.delete_month_int"))
        .withColumn(
            "target_month_int",
            F.explode(
                F.sequence(
                    F.col("d.delete_month_int") + F.lit(1),
                    F.least(
                        F.col("d.delete_month_int") + F.lit(history_len - 1),
                        F.col("l.latest_month_int"),
                    ),
                )
            ),
        )
        .select(
            F.col(f"d.{pk}").alias(pk),
            F.col("target_month_int"),
            (F.col("target_month_int") - F.col("d.delete_month_int")).alias("delete_position"),
            F.col(f"d.{ts}").alias("delete_ts"),
        )
    )

    if not future_candidates.isEmpty():
        future_agg = (
            future_candidates
            .groupBy(pk, "target_month_int")
            .agg(
                F.collect_set("delete_position").alias("delete_positions"),
                F.max("delete_ts").alias("new_base_ts"),
            )
        )

        future_ctx = future_agg.join(latest_ctx, pk, "inner")
        future_ctx.createOrReplaceTempView("case3d_future_ctx")

        target_month_expr = (
            "format_string('%04d-%02d', "
            "CAST(floor((target_month_int - 1) / 12) AS INT), "
            "CAST(((target_month_int - 1) % 12) + 1 AS INT))"
        )

        patch_exprs = []
        for rc in rolling_columns:
            history_col = f"{rc['name']}_history"
            dtype = rc.get('type', rc.get('data_type', 'String')).upper()
            patch_exprs.append(
                f"""
                    transform(
                        sequence(0, {history_len - 1}),
                        pos -> CASE
                            WHEN array_contains(delete_positions, pos) THEN CAST(NULL AS {dtype})
                            ELSE CAST(
                                element_at({history_col}, (latest_month_int - target_month_int + pos + 1))
                                AS {dtype}
                            )
                        END
                    ) AS {history_col}
                """
            )

        future_patch_df = spark.sql(
            f"""
                SELECT
                    {pk},
                    {target_month_expr} AS {prt},
                    GREATEST({ts}, new_base_ts) AS {ts},
                    {', '.join(patch_exprs)}
                FROM case3d_future_ctx
            """
        )

        for gc in grid_columns:
            source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
            source_history = f"{source_rolling}_history"
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('seperator', gc.get('separator', ''))
            future_patch_df = future_patch_df.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.col(source_history),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )

        write_case_table_bucketed(
            spark=spark,
            df=future_patch_df,
            table_name="temp_catalog.checkpointdb.case_3d_future",
            config=config,
            stage="case_3d_future_temp",
            expected_rows=expected_rows,
        )
        logger.info("Case III Soft Delete - Future month patches generated (context path)")
    else:
        logger.info("Case III Soft Delete - No future month patches required (context path)")

    # Part C: latest-summary history nullification from earliest delete month onward
    delete_scope = (
        delete_existing
        .groupBy(pk)
        .agg(
            F.min("delete_month_int").alias("min_delete_month_int"),
            F.max(F.col(ts)).alias("max_delete_ts"),
        )
    )
    latest_patch_base = (
        latest_ctx.join(delete_scope, pk, "inner")
        .filter(F.col("latest_month_int") >= F.col("min_delete_month_int"))
    )

    if not latest_patch_base.isEmpty():
        latest_patch_base.createOrReplaceTempView("case3d_latest_base")
        patch_history_exprs = []
        for rc in rolling_columns:
            history_col = f"{rc['name']}_history"
            dtype = rc.get('type', rc.get('data_type', 'String')).upper()
            patch_history_exprs.append(
                f"""
                    transform(
                        sequence(0, {latest_history_len - 1}),
                        i -> CASE
                            WHEN (latest_month_int - i) >= min_delete_month_int THEN CAST(NULL AS {dtype})
                            ELSE element_at({history_col}, i + 1)
                        END
                    ) AS {history_col}
                """
            )

        latest_delete_patch_df = spark.sql(
            f"""
                SELECT
                    {pk},
                    {prt},
                    GREATEST({ts}, max_delete_ts) AS {ts},
                    {', '.join(patch_history_exprs)}
                FROM case3d_latest_base
            """
        )

        for rc in rolling_columns:
            latest_delete_patch_df = latest_delete_patch_df.withColumn(
                f"{rc['name']}_history",
                F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
            )

        for gc in grid_columns:
            source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
            source_history = f"{source_rolling}_history"
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('seperator', gc.get('separator', ''))
            latest_delete_patch_df = latest_delete_patch_df.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.slice(F.col(source_history), 1, history_len),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )

        write_case_table_bucketed(
            spark=spark,
            df=latest_delete_patch_df,
            table_name=case3d_latest_history_patch_table,
            config=config,
            stage=f"{case3d_latest_history_patch_table.split('.')[-1]}_temp",
            expected_rows=expected_rows,
        )
        logger.info(f"Case III Soft Delete - Latest history patches generated (context path): {case3d_latest_history_patch_table}")

    return True


def process_case_iii_soft_delete(
    spark: SparkSession,
    case_iii_delete_df,
    config: Dict[str, Any],
    expected_rows: Optional[int] = None,
):
    """
    Handle soft delete rows for Case III (existing month corrections).

    Behavior:
    1. Update existing target month row in summary with soft_del_cd (arrays/grids unchanged)
    2. Nullify the deleted month's position in FUTURE months' rolling arrays
    """
    process_start_time = time.time()
    logger.info("=" * 80)
    logger.info("STEP 2c-DEL: Process Case III Soft Deletes")
    logger.info("=" * 80)

    if case_iii_delete_df is None or case_iii_delete_df.isEmpty():
        logger.info("No Case III soft-delete rows to process")
        return

    pk = config['primary_column']
    case3d_latest_history_patch_table = get_case3d_latest_history_patch_table(config)

    # Split soft-delete lane by month recency:
    # hot path uses latest_summary context-aware processing,
    # cold path falls back to legacy summary-scan processing.
    split_enabled = bool(config.get("enable_case3_hot_cold_split", True))
    if split_enabled and not bool(config.get("_case3d_split_internal", False)):
        prt = config['partition_column']
        hot_window = max(1, int(config.get("case3_hot_window_months", 36)))
        split_df = case_iii_delete_df
        if "month_int" not in split_df.columns:
            split_df = split_df.withColumn("month_int", F.expr(month_to_int_expr(prt)))

        global_latest_month = (
            split_df
            .agg(F.max("max_existing_month").alias("global_latest_month"))
            .first()["global_latest_month"]
        )
        if global_latest_month is not None:
            latest_int = int(global_latest_month[:4]) * 12 + int(global_latest_month[5:7])
            hot_cutoff_int = latest_int - (hot_window - 1)
            split_counts = (
                split_df
                .agg(
                    F.sum(F.when(F.col("month_int") >= F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("hot_count"),
                    F.sum(F.when(F.col("month_int") < F.lit(hot_cutoff_int), F.lit(1)).otherwise(F.lit(0))).alias("cold_count"),
                )
                .first()
            )
            hot_count = int(split_counts["hot_count"] or 0)
            cold_count = int(split_counts["cold_count"] or 0)
            logger.info(
                f"Case III soft-delete split active for latest_month={global_latest_month} with hot_window={hot_window}"
            )

            hot_df = split_df.filter(F.col("month_int") >= F.lit(hot_cutoff_int))
            cold_df = split_df.filter(F.col("month_int") < F.lit(hot_cutoff_int))

            has_account_overlap = False
            overlap_accounts = None
            if hot_count > 0 and cold_count > 0:
                overlap_accounts = hot_df.select(pk).intersect(cold_df.select(pk)).distinct()
                has_account_overlap = overlap_accounts.limit(1).count() > 0

            force_unified_on_overlap = bool(config.get("force_case3d_unified_on_any_overlap", False))
            if has_account_overlap and force_unified_on_overlap:
                logger.info(
                    "Case III soft-delete split safety fallback activated on overlap; using unified processing path"
                )
                unified_cfg = dict(config)
                unified_cfg["_case3d_split_internal"] = True
                unified_cfg["use_latest_history_context_case3d"] = False
                unified_cfg["_case3d_latest_history_patch_table"] = CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE
                process_case_iii_soft_delete(
                    spark,
                    split_df,
                    unified_cfg,
                    expected_rows=hot_count + cold_count,
                )
                return

            if has_account_overlap:
                mixed_df = split_df.join(overlap_accounts, on=pk, how="left_semi")
                hot_only_df = hot_df.join(overlap_accounts, on=pk, how="left_anti")
                cold_only_df = cold_df.join(overlap_accounts, on=pk, how="left_anti")
                logger.info(
                    "Case III soft-delete split mode: account-level lanes active (mixed + hot-only + cold-only)"
                )
            else:
                mixed_df = None
                hot_only_df = hot_df
                cold_only_df = cold_df
                logger.info("Case III soft-delete split mode: hot-only + cold-only lanes (no account overlap)")

            split_case_tables = {
                "temp_catalog.checkpointdb.case_3d_month": "case_3d_month_temp_split_combine",
                "temp_catalog.checkpointdb.case_3d_future": "case_3d_future_temp_split_combine",
                CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE: "case_3d_latest_history_context_patch_temp_split_combine",
                CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE: "case_3d_unified_latest_history_patch_temp_split_combine",
            }
            snapshot_groups: List[Dict[str, str]] = []

            if mixed_df is not None and not mixed_df.isEmpty():
                drop_case_tables(spark, split_case_tables)
                mixed_cfg = dict(config)
                mixed_cfg["_case3d_split_internal"] = True
                mixed_cfg["use_latest_history_context_case3d"] = False
                mixed_cfg["_case3d_latest_history_patch_table"] = CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE
                process_case_iii_soft_delete(
                    spark,
                    mixed_df,
                    mixed_cfg,
                    expected_rows=hot_count + cold_count,
                )
                snapshot_groups.append(
                    snapshot_case_tables(spark, split_case_tables, "mixed_lane_case3d")
                )

            if hot_only_df is not None and not hot_only_df.isEmpty():
                drop_case_tables(spark, split_case_tables)
                hot_cfg = dict(config)
                hot_cfg["_case3d_split_internal"] = True
                hot_cfg["use_latest_history_context_case3d"] = True
                hot_cfg["_case3d_latest_history_patch_table"] = CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE
                hot_handled = process_case_iii_soft_delete_using_latest_history_context(
                    spark,
                    hot_only_df,
                    hot_cfg,
                    expected_rows=hot_count,
                )
                if not hot_handled:
                    logger.info("Case III soft-delete hot-only lane fell back to legacy processing")
                    hot_fallback_cfg = dict(config)
                    hot_fallback_cfg["_case3d_split_internal"] = True
                    hot_fallback_cfg["use_latest_history_context_case3d"] = False
                    process_case_iii_soft_delete(
                        spark,
                        hot_only_df,
                        hot_fallback_cfg,
                        expected_rows=hot_count,
                    )
                snapshot_groups.append(
                    snapshot_case_tables(spark, split_case_tables, "hot_lane_case3d")
                )

            if cold_only_df is not None and not cold_only_df.isEmpty():
                drop_case_tables(spark, split_case_tables)
                cold_cfg = dict(config)
                cold_cfg["_case3d_split_internal"] = True
                cold_cfg["use_latest_history_context_case3d"] = False
                cold_cfg["_case3d_latest_history_patch_table"] = CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE
                force_cold_case3d_broadcast = bool(
                    config.get(
                        "force_cold_case3d_broadcast",
                        config.get("force_cold_case3_broadcast", True),
                    )
                )
                cold_lane_df = cold_only_df
                if force_cold_case3d_broadcast:
                    row_cap = int(
                        config.get(
                            "cold_case3d_broadcast_row_cap",
                            config.get("cold_case3_broadcast_row_cap", 10_000_000),
                        )
                    )
                    if cold_count > row_cap:
                        raise ValueError(
                            f"Cold Case III soft-delete broadcast guard failed: "
                            f"cold rows exceed cap={row_cap:,}"
                        )
                    cold_lane_df = F.broadcast(cold_only_df)
                    logger.info(
                        f"Forced broadcast enabled for cold Case III soft-delete input (cap={row_cap:,})"
                    )
                process_case_iii_soft_delete(
                    spark,
                    cold_lane_df,
                    cold_cfg,
                    expected_rows=cold_count,
                )
                snapshot_groups.append(
                    snapshot_case_tables(spark, split_case_tables, "cold_lane_case3d")
                )

            combine_case_table_snapshots(
                spark=spark,
                config=config,
                table_stage_map=split_case_tables,
                snapshot_groups=snapshot_groups,
                expected_rows=expected_rows,
            )
            cleanup_snapshot_tables(spark, snapshot_groups)
            return

    if config.get("use_latest_history_context_case3d", True):
        if process_case_iii_soft_delete_using_latest_history_context(
            spark,
            case_iii_delete_df,
            config,
            expected_rows=expected_rows,
        ):
            return
        logger.info("Case III soft-delete latest-context path fell back to legacy mode")

    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_table = get_case3_summary_context_table(spark, config)
    latest_summary_table = get_latest_context_table(spark, config)
    history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    latest_cols = set(get_latest_cols(config))

    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    affected_accounts = case_iii_delete_df.select(pk).distinct()

    # Partition-pruned summary read around the delete month range.
    backfill_range = case_iii_delete_df.agg(
        F.min(prt).alias("min_backfill_month"),
        F.max(prt).alias("max_backfill_month")
    ).first()
    min_backfill = backfill_range['min_backfill_month']
    max_backfill = backfill_range['max_backfill_month']

    min_backfill_int = int(min_backfill[:4]) * 12 + int(min_backfill[5:7])
    max_backfill_int = int(max_backfill[:4]) * 12 + int(max_backfill[5:7])

    earliest_needed_int = min_backfill_int - history_len
    latest_needed_int = max_backfill_int + history_len

    earliest_year = earliest_needed_int // 12
    earliest_month = earliest_needed_int % 12
    if earliest_month == 0:
        earliest_month = 12
        earliest_year -= 1
    earliest_partition = f"{earliest_year}-{earliest_month:02d}"

    latest_year = latest_needed_int // 12
    latest_month = latest_needed_int % 12
    if latest_month == 0:
        latest_month = 12
        latest_year -= 1
    latest_partition = f"{latest_year}-{latest_month:02d}"

    summary_filtered = (
        spark.sql(
            f"""
                SELECT *
                FROM {summary_table}
                WHERE {prt} >= '{earliest_partition}' AND {prt} <= '{latest_partition}'
            """
        )
        .join(affected_accounts, pk, "left_semi")
        .withColumn("month_int", F.expr(month_to_int_expr(prt)))
    )

    # Keep only deletes that already exist in summary. Non-existing delete rows are ignored.
    delete_existing = (
        case_iii_delete_df.alias("d")
        .join(
            summary_filtered.select(pk, prt).alias("s"),
            (F.col(f"d.{pk}") == F.col(f"s.{pk}")) & (F.col(f"d.{prt}") == F.col(f"s.{prt}")),
            "inner"
        )
        .select("d.*")
    )

    if delete_existing.isEmpty():
        logger.info("No existing summary rows matched soft-delete records; nothing to update")
        return

    # ---------------------------------------------------------------------
    # Part A: Month-row updates (ONLY soft_del_cd + base_ts)
    # ---------------------------------------------------------------------
    delete_month_update_df = delete_existing.select(
        F.col(pk),
        F.col(prt),
        F.col(ts),
        F.col(SOFT_DELETE_COLUMN),
    )
    write_case_table_bucketed(
        spark=spark,
        df=delete_month_update_df,
        table_name="temp_catalog.checkpointdb.case_3d_month",
        config=config,
        stage="case_3d_month_temp",
        expected_rows=expected_rows,
    )
    logger.info("Case III Soft Delete - Month-row updates generated")

    # ---------------------------------------------------------------------
    # Part B: Future month array nullification
    # ---------------------------------------------------------------------
    delete_keys = (
        delete_existing
        .withColumn("delete_month_int", F.expr(month_to_int_expr(prt)))
        .select(
            F.col(pk),
            F.col(prt).alias("delete_month"),
            F.col("delete_month_int"),
            F.col(ts).alias("delete_ts"),
        )
    )

    summary_future = summary_filtered.select(
        pk, prt, ts, "month_int", *history_cols
    )

    future_delete_joined = (
        delete_keys.alias("d")
        .join(
            summary_future.alias("s"),
            (F.col(f"s.{pk}") == F.col(f"d.{pk}"))
            & (F.col("s.month_int") > F.col("d.delete_month_int"))
            & ((F.col("s.month_int") - F.col("d.delete_month_int")) < F.lit(history_len)),
            "inner"
        )
        .select(
            F.col(f"d.{pk}").alias(pk),
            F.col("d.delete_ts").alias("delete_ts"),
            F.col(f"s.{prt}").alias("summary_month"),
            F.col(f"s.{ts}").alias("summary_ts"),
            (F.col("s.month_int") - F.col("d.delete_month_int")).alias("delete_position"),
            *[F.col(f"s.{arr}").alias(f"existing_{arr}") for arr in history_cols],
        )
    )

    if not future_delete_joined.isEmpty():
        agg_exprs = [
            F.collect_set("delete_position").alias("delete_positions"),
            F.max("delete_ts").alias("new_base_ts"),
            F.first("summary_ts").alias("existing_summary_ts"),
        ]
        for arr in history_cols:
            agg_exprs.append(F.first(f"existing_{arr}").alias(f"existing_{arr}"))

        aggregated_df = future_delete_joined.groupBy(pk, "summary_month").agg(*agg_exprs)
        aggregated_df.createOrReplaceTempView("aggregated_delete_backfills")

        update_exprs = []
        for rc in rolling_columns:
            array_name = f"{rc['name']}_history"
            update_exprs.append(
                f"""
                    transform(
                        existing_{array_name},
                        (x, i) -> CASE
                            WHEN array_contains(delete_positions, i) THEN NULL
                            ELSE x
                        END
                    ) as {array_name}
                """
            )

        future_delete_updates_df = spark.sql(
            f"""
                SELECT
                    {pk},
                    summary_month as {prt},
                    GREATEST(existing_summary_ts, new_base_ts) as {ts},
                    {', '.join(update_exprs)}
                FROM aggregated_delete_backfills
            """
        )

        for gc in grid_columns:
            source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
            source_history = f"{source_rolling}_history"
            placeholder = gc.get('placeholder', '?')
            separator = gc.get('seperator', gc.get('separator', ''))
            future_delete_updates_df = future_delete_updates_df.withColumn(
                gc['name'],
                F.concat_ws(
                    separator,
                    F.transform(
                        F.col(source_history),
                        lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                    )
                )
            )

        write_case_table_bucketed(
            spark=spark,
            df=future_delete_updates_df,
            table_name="temp_catalog.checkpointdb.case_3d_future",
            config=config,
            stage="case_3d_future_temp",
            expected_rows=expected_rows,
        )
        logger.info("Case III Soft Delete - Future month patches generated")
    else:
        logger.info("Case III Soft Delete - No future month patches required")

    # ---------------------------------------------------------------------
    # Part C: Latest-summary history patches (null from delete month onward)
    # ---------------------------------------------------------------------
    if all(c in latest_cols for c in history_cols):
        delete_scope = (
            delete_existing
            .withColumn("delete_month_int", F.expr(month_to_int_expr(prt)))
            .groupBy(pk)
            .agg(
                F.min("delete_month_int").alias("min_delete_month_int"),
                F.max(F.col(ts)).alias("max_delete_ts"),
            )
        )
        latest_select_exprs = [F.col(pk), F.col(prt), F.col(ts)] + [F.col(c) for c in history_cols]
        latest_base = (
            spark.table(latest_summary_table)
            .select(*latest_select_exprs)
            .join(delete_scope, on=pk, how="inner")
            .withColumn("latest_month_int", F.expr(month_to_int_expr(prt)))
            .filter(F.col("latest_month_int") >= F.col("min_delete_month_int"))
        )
        if not latest_base.isEmpty():
            latest_base.createOrReplaceTempView("case3d_latest_base")
            patch_history_exprs = []
            for rc in rolling_columns:
                history_col = f"{rc['name']}_history"
                dtype = rc.get('type', rc.get('data_type', 'String')).upper()
                patch_history_exprs.append(
                    f"""
                        transform(
                            sequence(0, {latest_history_len - 1}),
                            i -> CASE
                                WHEN (latest_month_int - i) >= min_delete_month_int THEN CAST(NULL AS {dtype})
                                ELSE element_at({history_col}, i + 1)
                            END
                        ) AS {history_col}
                    """
                )
            latest_patch_sql = f"""
                SELECT
                    {pk},
                    {prt},
                    GREATEST({ts}, max_delete_ts) AS {ts},
                    {', '.join(patch_history_exprs)}
                FROM case3d_latest_base
            """
            latest_delete_patch_df = spark.sql(latest_patch_sql)
            for rc in rolling_columns:
                latest_delete_patch_df = latest_delete_patch_df.withColumn(
                    f"{rc['name']}_history",
                    F.slice(F.col(f"{rc['name']}_history"), 1, latest_history_len),
                )
            for gc in grid_columns:
                source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
                source_history = f"{source_rolling}_history"
                placeholder = gc.get('placeholder', '?')
                separator = gc.get('seperator', gc.get('separator', ''))
                latest_delete_patch_df = latest_delete_patch_df.withColumn(
                    gc['name'],
                    F.concat_ws(
                        separator,
                        F.transform(
                            F.slice(F.col(source_history), 1, history_len),
                            lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                        )
                    )
                )
            write_case_table_bucketed(
                spark=spark,
                df=latest_delete_patch_df,
                table_name=case3d_latest_history_patch_table,
                config=config,
                stage=f"{case3d_latest_history_patch_table.split('.')[-1]}_temp",
                expected_rows=expected_rows,
            )
            logger.info(f"Case III Soft Delete - Latest history patches generated: {case3d_latest_history_patch_table}")

    process_end_time = time.time()
    logger.info(
        f"Case III Soft Deletes generated | Time Elapsed: {(process_end_time - process_start_time) / 60:.2f} minutes"
    )
    logger.info("-" * 60)
    return


def process_case_iv(spark: SparkSession, case_iv_df, case_i_result, config: Dict[str, Any], expected_rows: Optional[int] = None):
    """
    Process Case IV - Bulk historical load for new accounts with multiple months
        
    Logic:
    - For new accounts with multiple months uploaded at once
    - Build rolling history arrays using window functions
    - Each month's array contains up to 36 months of prior data from the batch
    
    Args:
        spark: SparkSession
        case_iv_df: DataFrame with Case IV records (subsequent months for new accounts)
        case_i_result: DataFrame with Case I results (first month for each new account)
        config: Config dict
    
    Returns:
        None
    """
    logger.info("=" * 80)
    logger.info("STEP 2d: Process Case IV (Bulk Historical Load)")
    logger.info("=" * 80)
    process_start_time = time.time()
   
    logger.info(f"Processing bulk historical records")
    
    pk = config['primary_column']
    prt = config['partition_column']
    history_len = get_summary_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    
    # Combine Case I results with Case IV records to build complete history
    # Case I records are the "first month" for each new account
    # Case IV records are "subsequent months" that need rolling history
    
    # Get unique accounts in Case IV
    case_iv_accounts = case_iv_df.select(pk).distinct()
      
    case_iv_df.createOrReplaceTempView("case_iv_records")
    
    # Get all months for bulk historical accounts (including the first month)
    # We need to include Case I records to build complete history
    logger.info("Building complete history using window functions")
    
    all_new_account_months = spark.sql(f"""
        SELECT * FROM case_iv_records
    """)
    
    # Add Case I first months if available
    if case_i_result is not None and case_i_result.count() > 0:
        # Get Case I records that belong to accounts with Case IV records
        case_i_for_iv = case_i_result.join(case_iv_accounts, pk, "inner")
        
        # We need to ensure the columns match
        common_cols = list(set(case_iv_df.columns) & set(case_i_for_iv.columns))
        if common_cols:
            # Add month_int if not present in case_i_result
            if "month_int" not in case_i_for_iv.columns:
                case_i_for_iv = case_i_for_iv.withColumn(
                    "month_int",
                    F.expr(month_to_int_expr(prt))
                )
            
            # Select matching columns and union
            case_iv_cols = case_iv_df.columns
            
            # Add missing columns to case_i_for_iv with NULL values
            for col in case_iv_cols:
                if col not in case_i_for_iv.columns:
                    case_i_for_iv = case_i_for_iv.withColumn(col, F.lit(None))
            
            case_i_for_iv = case_i_for_iv.select(*case_iv_cols)
            all_new_account_months = case_iv_df.unionByName(case_i_for_iv, allowMissingColumns=True)
    
    all_new_account_months.createOrReplaceTempView("all_bulk_records")
    
    # =========================================================================
    #  GAP HANDLING
    # =========================================================================
    #  MAP_FROM_ENTRIES + TRANSFORM to look up values by month_int
    #   - Create MAP of month_int -> value for each account
    #   - For each row, generate positions 0-35
    #   - Look up value for (current_month_int - position)
    #   - Missing months return NULL automatically
    # =========================================================================
    
    # Step 1: Build MAP for each column by account
    # MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, value)))
    map_build_exprs = []
    for rc in rolling_columns:
        col_name = rc['name']
        mapper_column = rc['mapper_column']             
 
        map_build_exprs.append(f"""
            MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT(month_int, {mapper_column}))) as value_map_{col_name}
        """)
    
    # Create account-level maps
    map_sql = f"""
        SELECT 
            {pk},
            MIN(month_int) as min_month_int,
            MAX(month_int) as max_month_int,
            {', '.join(map_build_exprs)}
        FROM all_bulk_records
        GROUP BY {pk}
    """
    
    account_maps = spark.sql(map_sql)
    account_maps.createOrReplaceTempView("account_value_maps")
    
    # Step 2: Join maps back to records and build arrays using TRANSFORM
    # For each position 0-35, look up value from map using (month_int - position)
    
    array_build_exprs = []
    for rc in rolling_columns:
        col_name = rc['name']
        # Use 'type' to match original format (instead of 'data_type')
        data_type = rc.get('type', rc.get('data_type', 'String'))
        array_name = f"{col_name}_history"
        
        # TRANSFORM generates array by looking up each position
        # Position 0 = current month, Position N = N months ago
        # If month doesn't exist in map, returns NULL (correct for gaps!)
        array_build_exprs.append(f"""
            TRANSFORM(
                SEQUENCE(0, {history_len - 1}),
                pos -> CAST(m.value_map_{col_name}[r.month_int - pos] AS {data_type.upper()})
            ) as {array_name}
        """)
    
    # Get base columns from the records table
    exclude_cols = set(["month_int", "max_existing_month", "max_existing_ts", "max_month_int",
                        "_is_soft_delete", "case_type", "MONTH_DIFF", "min_month_for_new_account", 
                        "count_months_for_new_account"])
    exclude_cols.update([c for c in all_new_account_months.columns if c.startswith("_prepared_")])
    
    base_cols = [f"r.{c}" for c in all_new_account_months.columns if c not in exclude_cols]
    
    # Build final SQL joining records with maps
    final_sql = f"""
        SELECT 
            {', '.join(base_cols)},
            {', '.join(array_build_exprs)}
        FROM all_bulk_records r
        JOIN account_value_maps m ON r.{pk} = m.{pk}
    """
    
    result = spark.sql(final_sql)
    
    # Generate grid columns
    for gc in grid_columns:
        source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
        source_history = f"{source_rolling}_history"
        placeholder = gc.get('placeholder', '?')
        separator = gc.get('seperator', gc.get('separator', ''))
        
        result = result.withColumn(
            gc['name'],
            F.concat_ws(
                separator,
                F.transform(
                    F.col(source_history),
                    lambda x: F.coalesce(x.cast(StringType()), F.lit(placeholder))
                )
            )
        )
    
    # Filter to only Case IV records (exclude the first month which is already processed as Case I)
    # Join back with original case_iv_df to get only the subsequent months
    result = result.join(
        case_iv_df.select(pk, prt).distinct(),
        [pk, prt],
        "inner"
    )
    
    write_case_table_bucketed(
        spark=spark,
        df=result,
        table_name="temp_catalog.checkpointdb.case_4",
        config=config,
        stage="case_4_temp",
        expected_rows=expected_rows,
    )

    process_end_time = time.time()
    process_total_minutes = (process_end_time - process_start_time) / 60
    logger.info(f"Case IV Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
    logger.info("-" * 60)
    return


def build_latest_merge_columns(
    config: Dict[str, Any],
    source_cols: List[str],
    pk: str,
) -> Tuple[List[str], str]:
    latest_cols = get_latest_cols(config)
    shared_cols = [c for c in source_cols if c in latest_cols]
    if pk not in shared_cols:
        raise ValueError(f"Primary key '{pk}' missing from latest_summary merge columns")
    update_cols = [c for c in shared_cols if c != pk]
    update_set_expr = ", ".join([f"s.{c} = c.{c}" for c in update_cols])
    return shared_cols, update_set_expr


def write_backfill_results(spark: SparkSession, config: Dict[str, Any], expected_rows_append: Optional[int] = None):
    summary_table = config['destination_table']
    latest_summary_table = config['latest_history_table']
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    grid_columns = config.get('grid_columns', [])
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
    grid_cols = [gc['name'] for gc in grid_columns]
    case3_latest_month_patch_tables = [
        CASE3_LATEST_MONTH_PATCH_TABLE,
        CASE3_UNIFIED_LATEST_MONTH_PATCH_TABLE,
    ]
    case3d_latest_history_patch_tables = [
        CASE3D_LATEST_HISTORY_CONTEXT_PATCH_TABLE,
        CASE3D_UNIFIED_LATEST_HISTORY_PATCH_TABLE,
    ]

    try:
        logger.info("-" * 60)
        logger.info("MERGING RECORDS:")
        process_start_time = time.time()

        case_3a_df = None
        case_3b_filtered_df = None
        case_3_latest_month_patch_df = None
        case_3d_month_df = None
        case_3d_future_df = None
        case_3d_latest_history_patch_df = None
        month_chunks: List[List[str]] = []

        def _read_union_if_exists(table_names: List[str]):
            dfs = [spark.read.table(t) for t in table_names if spark.catalog.tableExists(t)]
            if not dfs:
                return None
            merged = dfs[0]
            for d in dfs[1:]:
                merged = merged.unionByName(d, allowMissingColumns=True)
            return merged

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3a"):
            case_3a_df = spark.read.table('temp_catalog.checkpointdb.case_3a')

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3b"):
            case_3b_df = spark.read.table('temp_catalog.checkpointdb.case_3b')
            if case_3a_df is not None:
                case_3b_filtered_df = case_3b_df.join(case_3a_df.select(pk, prt), [pk, prt], "left_anti")
            else:
                case_3b_filtered_df = case_3b_df

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3d_month"):
            case_3d_month_df = spark.read.table("temp_catalog.checkpointdb.case_3d_month")

        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3d_future"):
            case_3d_future_df = spark.read.table("temp_catalog.checkpointdb.case_3d_future")

        case_3_latest_month_patch_df = _read_union_if_exists(case3_latest_month_patch_tables)
        case_3d_latest_history_patch_df = _read_union_if_exists(case3d_latest_history_patch_tables)

        if case_3a_df is not None or case_3b_filtered_df is not None:
            case3b_weight = float(config.get("case3_merge_case3b_weight", 1.3))
            overflow_ratio = float(config.get("case3_merge_overflow_ratio", 0.10))
            logger.info(
                f"Case III merge chunking enabled "
                f"(case3b_weight={case3b_weight}, overflow_ratio={overflow_ratio})"
            )

            month_load_df = None
            if case_3a_df is not None:
                month_load_df = (
                    case_3a_df.groupBy(prt)
                    .count()
                    .withColumnRenamed("count", "case3a_count")
                )

            if case_3b_filtered_df is not None:
                case_3b_month_load = (
                    case_3b_filtered_df.groupBy(prt)
                    .count()
                    .withColumnRenamed("count", "case3b_count")
                )
                if month_load_df is None:
                    month_load_df = case_3b_month_load.withColumn("case3a_count", F.lit(0))
                else:
                    month_load_df = month_load_df.join(case_3b_month_load, prt, "full_outer")

            month_load_df = (
                month_load_df
                .na.fill(0, ["case3a_count", "case3b_count"])
                .withColumn(
                    "weighted_load",
                    F.col("case3a_count") + (F.lit(case3b_weight) * F.col("case3b_count")),
                )
            )

            month_weights = [
                (r[prt], float(r["weighted_load"]))
                for r in month_load_df.select(prt, "weighted_load").collect()
                if r[prt] is not None
            ]

            month_chunks = build_balanced_month_chunks(month_weights, overflow_ratio=overflow_ratio)

            if month_chunks:
                logger.info(f"Case III merge month chunks: {len(month_chunks)}")
                for idx, months in enumerate(month_chunks, 1):
                    logger.info(f"  Chunk {idx}: months={months}")

        if case_3a_df is not None:
            if not month_chunks:
                month_chunks = [[r[prt]] for r in case_3a_df.select(prt).distinct().orderBy(prt).collect()]
            case_3a_month_set = {
                r[prt] for r in case_3a_df.select(prt).distinct().collect() if r[prt] is not None
            }

            for idx, months in enumerate(month_chunks, 1):
                case_3a_chunk_months = [m for m in months if m in case_3a_month_set]
                if not case_3a_chunk_months:
                    continue

                case_3a_chunk_df = case_3a_df.filter(F.col(prt).isin(case_3a_chunk_months))
                case_3a_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3a_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3a_chunk_{idx}",
                )
                case_3a_chunk_df.createOrReplaceTempView("case_3a_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3a_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """
                )
                logger.info(
                    f"MERGED - CASE III-A chunk {idx}/{len(month_chunks)} "
                    f"(months={case_3a_chunk_months})"
                )
        
        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"MERGED - CASE III-A (NEW summary rows for backfill months with inherited history)| Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)

        process_start_time = time.time()

        if case_3b_filtered_df is not None:
            update_cols = list(dict.fromkeys([ts] + history_cols + grid_cols))

            update_set_exprs = []
            for col_name in update_cols:
                if col_name == ts:
                    update_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                elif col_name in history_cols:
                    update_set_exprs.append(
                        f"s.{col_name} = {summary_history_trim_expr(col_name, summary_history_len)}"
                    )
                else:
                    update_set_exprs.append(f"s.{col_name} = c.{col_name}")

            if not month_chunks:
                month_chunks = [[r[prt]] for r in case_3b_filtered_df.select(prt).distinct().orderBy(prt).collect()]
            case_3b_month_set = {
                r[prt] for r in case_3b_filtered_df.select(prt).distinct().collect() if r[prt] is not None
            }

            for idx, months in enumerate(month_chunks, 1):
                case_3b_chunk_months = [m for m in months if m in case_3b_month_set]
                if not case_3b_chunk_months:
                    continue

                case_3b_chunk_df = case_3b_filtered_df.filter(F.col(prt).isin(case_3b_chunk_months))
                case_3b_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3b_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3b_chunk_{idx}",
                )
                case_3b_chunk_df.createOrReplaceTempView("case_3b_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3b_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET {', '.join(update_set_exprs)}
                    """
                )
                logger.info(
                    f"MERGED - CASE III-B chunk {idx}/{len(month_chunks)} "
                    f"(months={case_3b_chunk_months})"
                )

        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Updated Summary | MERGED - CASE III-B (future summary rows with backfill data) | Time Elapsed: {process_total_minutes:.2f} minutes")
        logger.info("-" * 60)

        process_start_time = time.time()
        if case_3_latest_month_patch_df is not None:
            latest_value_cols = [f"latest_val_{rc['name']}" for rc in rolling_columns]
            summary_cols_set = set(get_summary_cols(config))

            scalar_update_cols = [
                c for c in case_3_latest_month_patch_df.columns
                if c in summary_cols_set
                and c not in set([pk, prt, ts] + history_cols + grid_cols + latest_value_cols)
            ]

            history_update_sql_map: Dict[str, str] = {}
            for rc in rolling_columns:
                array_name = f"{rc['name']}_history"
                latest_val_col = f"latest_val_{rc['name']}"
                data_type = rc.get('type', rc.get('data_type', 'String'))
                history_update_sql_map[array_name] = (
                    f"transform(s.{array_name}, (x, i) -> CASE "
                    f"WHEN i = 0 THEN CAST(c.{latest_val_col} AS {data_type.upper()}) "
                    f"ELSE x END)"
                )

            update_set_exprs = [f"s.{ts} = GREATEST(s.{ts}, c.{ts})"]
            for col_name in scalar_update_cols:
                update_set_exprs.append(f"s.{col_name} = c.{col_name}")
            for array_name in history_cols:
                update_set_exprs.append(f"s.{array_name} = {history_update_sql_map[array_name]}")

            for gc in grid_columns:
                source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
                source_history = f"{source_rolling}_history"
                placeholder = gc.get('placeholder', '?').replace("'", "''")
                separator = gc.get('seperator', gc.get('separator', '')).replace("'", "''")
                source_expr = history_update_sql_map.get(source_history, f"s.{source_history}")
                grid_expr = (
                    f"concat_ws('{separator}', "
                    f"transform({source_expr}, x -> coalesce(cast(x as string), '{placeholder}')))"
                )
                update_set_exprs.append(f"s.{gc['name']} = {grid_expr}")

            case_3_latest_patch_overflow = float(
                config.get("case3_latest_month_merge_overflow_ratio", config.get("case3_merge_overflow_ratio", 0.10))
            )
            case_3_latest_patch_chunks = build_month_chunks_from_df(
                case_3_latest_month_patch_df,
                prt,
                overflow_ratio=case_3_latest_patch_overflow,
            )
            logger.info(f"Case III Latest-Month Patch merge chunks: {len(case_3_latest_patch_chunks)}")
            for idx, months in enumerate(case_3_latest_patch_chunks, 1):
                case_3_latest_patch_chunk_df = case_3_latest_month_patch_df.filter(F.col(prt).isin(months))
                case_3_latest_patch_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3_latest_patch_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3_latest_month_patch_chunk_{idx}",
                )
                case_3_latest_patch_chunk_df.createOrReplaceTempView("case_3_latest_month_patch_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3_latest_month_patch_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED AND c.{ts} >= s.{ts}
                        THEN UPDATE SET {', '.join(update_set_exprs)}
                    """
                )
                logger.info(
                    f"MERGED - CASE III Latest-Month Patch chunk {idx}/{len(case_3_latest_patch_chunks)} "
                    f"(months={months})"
                )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated Summary | MERGED - CASE III Latest-Month Patch | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        process_start_time = time.time()
        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3_latest_history_context_patch"):
            latest_case_3_history_df = spark.read.table("temp_catalog.checkpointdb.case_3_latest_history_context_patch")
            latest_case_3_history_df = align_history_arrays_to_length(
                latest_case_3_history_df,
                rolling_columns,
                latest_history_len,
            )
            latest_shared_cols, latest_update_set_expr = build_latest_merge_columns(
                config,
                latest_case_3_history_df.columns,
                pk,
            )
            latest_case_3_history_df.select(*latest_shared_cols).createOrReplaceTempView("latest_case_3_history_patch")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3_history_patch c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        c.{prt} > s.{prt}
                        OR (c.{prt} = s.{prt} AND c.{ts} >= s.{ts})
                    ) THEN UPDATE SET {latest_update_set_expr}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated latest_summary | MERGE - CASE III history patches | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3d_month_df is not None:
            case_3d_month_overflow = float(
                config.get("case3d_month_merge_overflow_ratio", config.get("case3_merge_overflow_ratio", 0.10))
            )
            case_3d_month_chunks = build_month_chunks_from_df(
                case_3d_month_df,
                prt,
                overflow_ratio=case_3d_month_overflow,
            )
            logger.info(f"Case III Soft Delete Month merge chunks: {len(case_3d_month_chunks)}")
            for idx, months in enumerate(case_3d_month_chunks, 1):
                case_3d_month_chunk_df = case_3d_month_df.filter(F.col(prt).isin(months))
                case_3d_month_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3d_month_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3d_month_chunk_{idx}",
                )
                case_3d_month_chunk_df.createOrReplaceTempView("case_3d_month_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3d_month_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET
                            s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN},
                            s.{ts} = GREATEST(s.{ts}, c.{ts})
                    """
                )
                logger.info(
                    f"MERGED - CASE III Soft Delete Month chunk {idx}/{len(case_3d_month_chunks)} "
                    f"(months={months})"
                )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE III Soft Delete Month Rows | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3d_future_df is not None:
            soft_delete_update_cols = list(dict.fromkeys([ts] + history_cols + grid_cols))
            soft_delete_set_exprs = []
            for col_name in soft_delete_update_cols:
                if col_name == ts:
                    soft_delete_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                else:
                    soft_delete_set_exprs.append(f"s.{col_name} = c.{col_name}")

            case_3d_future_overflow = float(
                config.get("case3d_future_merge_overflow_ratio", config.get("case3_merge_overflow_ratio", 0.10))
            )
            case_3d_future_chunks = build_month_chunks_from_df(
                case_3d_future_df,
                prt,
                overflow_ratio=case_3d_future_overflow,
            )
            logger.info(f"Case III Soft Delete Future merge chunks: {len(case_3d_future_chunks)}")
            for idx, months in enumerate(case_3d_future_chunks, 1):
                case_3d_future_chunk_df = case_3d_future_df.filter(F.col(prt).isin(months))
                case_3d_future_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_3d_future_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_3d_future_chunk_{idx}",
                )
                case_3d_future_chunk_df.createOrReplaceTempView("case_3d_future_chunk")
                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_3d_future_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET {', '.join(soft_delete_set_exprs)}
                    """
                )
                logger.info(
                    f"MERGED - CASE III Soft Delete Future chunk {idx}/{len(case_3d_future_chunks)} "
                    f"(months={months})"
                )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE III Soft Delete Future Patches | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        logger.info("MERGING NEW/BULK RECORDS:")
        process_start_time = time.time()

        append_tables = ["temp_catalog.checkpointdb.case_1","temp_catalog.checkpointdb.case_4"]

        dfs = []
        for t in append_tables:
            if spark.catalog.tableExists(t):
                dfs.append(spark.read.table(t))

        if dfs:
            append_df = reduce(lambda a, b: a.unionByName(b), dfs)

            merge_window = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
            append_merge_df = (
                append_df
                .withColumn("_rn", F.row_number().over(merge_window))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            case_1_4_overflow = float(config.get("case1_4_merge_overflow_ratio", 0.10))
            case_1_4_chunks = build_month_chunks_from_df(
                append_merge_df,
                prt,
                overflow_ratio=case_1_4_overflow,
            )
            logger.info(f"Case I/IV summary merge chunks: {len(case_1_4_chunks)}")

            for idx, months in enumerate(case_1_4_chunks, 1):
                case_1_4_chunk_df = append_merge_df.filter(F.col(prt).isin(months))
                case_1_4_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_1_4_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_1_4_chunk_{idx}",
                    expected_rows=expected_rows_append,
                )
                case_1_4_chunk_df.createOrReplaceTempView("case_1_4_merge_chunk")

                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_1_4_merge_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED AND c.{ts} >= s.{ts} THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """
                )
                logger.info(
                    f"MERGED - CASE I/IV chunk {idx}/{len(case_1_4_chunks)} "
                    f"(months={months})"
                )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE I & IV (New Records + Bulk Historical) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

            logger.info("UPDATING LATEST SUMMARY:")
            process_start_time = time.time()
            # Get Latest from append_df - case I & IV for appending to latest_summary
            window_spec = Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())
            latest_append_df = (
                append_merge_df
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
            latest_append_df = align_history_arrays_to_length(
                latest_append_df,
                rolling_columns,
                latest_history_len,
            )
            latest_cols_set = set(get_latest_cols(config))
            latest_shared_cols = [c for c in latest_append_df.columns if c in latest_cols_set]
            if pk not in latest_shared_cols:
                raise ValueError(f"Primary key '{pk}' missing from latest_summary merge columns")

            latest_update_set_exprs = []
            for col_name in latest_shared_cols:
                if col_name == pk:
                    continue
                if col_name in history_cols:
                    latest_update_set_exprs.append(
                        f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                    )
                else:
                    latest_update_set_exprs.append(f"s.{col_name} = c.{col_name}")

            latest_append_df.select(*latest_shared_cols).createOrReplaceTempView("latest_case_1_4")

            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_1_4 c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        s.{prt} < c.{prt}
                        OR (s.{prt} = c.{prt} AND s.{ts} <= c.{ts})
                    ) THEN UPDATE SET {', '.join(latest_update_set_exprs)}
                    WHEN NOT MATCHED THEN INSERT ({', '.join(latest_shared_cols)})
                    VALUES ({', '.join([f'c.{col}' for col in latest_shared_cols])})
                """
            )

            logger.info(f"Updated latest_summary | MERGED - CASE I & IV (New Records + Bulk Historical) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)
        else:
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"No Case I/IV records to merge | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        logger.info("UPDATING LATEST SUMMARY FROM CASE III:")
        process_start_time = time.time()

        update_cols = list(dict.fromkeys([prt, ts] + history_cols + grid_cols))
        case3_select_cols = [pk] + update_cols

        case_3b_latest_df = None
        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3b"):
            case_3b_latest_df = spark.read.table("temp_catalog.checkpointdb.case_3b").select(*case3_select_cols)
            case_3b_latest_df = (
                case_3b_latest_df
                .withColumn("_rn", F.row_number().over(Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

        case_3a_latest_df = None
        if spark.catalog.tableExists("temp_catalog.checkpointdb.case_3a"):
            case_3a_latest_df = spark.read.table("temp_catalog.checkpointdb.case_3a").select(*case3_select_cols)
            if case_3b_latest_df is not None:
                case_3a_latest_df = case_3a_latest_df.join(
                    case_3b_latest_df.select(pk).distinct(),
                    [pk],
                    "left_anti",
                )
            case_3a_latest_df = (
                case_3a_latest_df
                .withColumn("_rn", F.row_number().over(Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

        case3_latest_df = None
        if case_3b_latest_df is not None:
            case3_latest_df = case_3b_latest_df
        if case_3a_latest_df is not None:
            case3_latest_df = case_3a_latest_df if case3_latest_df is None else case3_latest_df.unionByName(case_3a_latest_df)

        if case3_latest_df is not None:
            case3_latest_df = (
                case3_latest_df
                .withColumn("_rn", F.row_number().over(Window.partitionBy(pk).orderBy(F.col(prt).desc(), F.col(ts).desc())))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
            case3_latest_df.createOrReplaceTempView("latest_case_3")

            update_set_exprs = []
            for col_name in update_cols:
                if col_name == ts:
                    update_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                elif col_name in history_cols:
                    update_set_exprs.append(
                        f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                    )
                else:
                    update_set_exprs.append(f"s.{col_name} = c.{col_name}")

            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3 c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        s.{prt} < c.{prt}
                        OR (s.{prt} = c.{prt} AND s.{ts} <= c.{ts})
                    ) THEN UPDATE SET {', '.join(update_set_exprs)}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated latest_summary | MERGE - CASE III candidates | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)
        else:
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"No Case III candidates for latest_summary | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3_latest_month_patch_df is not None:
            latest_value_cols = [f"latest_val_{rc['name']}" for rc in rolling_columns]
            latest_cols_set = set(get_latest_cols(config))

            latest_scalar_update_cols = [
                c for c in case_3_latest_month_patch_df.columns
                if c in latest_cols_set
                and c not in set([pk, prt, ts] + history_cols + grid_cols + latest_value_cols)
            ]

            latest_history_update_sql_map: Dict[str, str] = {}
            for rc in rolling_columns:
                array_name = f"{rc['name']}_history"
                latest_val_col = f"latest_val_{rc['name']}"
                data_type = rc.get('type', rc.get('data_type', 'String'))
                latest_history_update_sql_map[array_name] = (
                    f"transform(sequence(0, {latest_history_len - 1}), i -> CASE "
                    f"WHEN i = 0 THEN CAST(c.{latest_val_col} AS {data_type.upper()}) "
                    f"ELSE element_at(s.{array_name}, i + 1) END)"
                )

            latest_update_set_exprs = [f"s.{ts} = GREATEST(s.{ts}, c.{ts})"]
            for col_name in latest_scalar_update_cols:
                latest_update_set_exprs.append(f"s.{col_name} = c.{col_name}")
            for array_name in history_cols:
                if array_name in latest_cols_set:
                    latest_update_set_exprs.append(f"s.{array_name} = {latest_history_update_sql_map[array_name]}")

            for gc in grid_columns:
                if gc['name'] not in latest_cols_set:
                    continue
                source_rolling = gc.get('mapper_rolling_column', gc.get('source_history', ''))
                source_history = f"{source_rolling}_history"
                placeholder = gc.get('placeholder', '?').replace("'", "''")
                separator = gc.get('seperator', gc.get('separator', '')).replace("'", "''")
                source_expr = latest_history_update_sql_map.get(source_history, f"s.{source_history}")
                grid_expr = (
                    f"concat_ws('{separator}', "
                    f"transform(slice({source_expr}, 1, {summary_history_len}), "
                    f"x -> coalesce(cast(x as string), '{placeholder}')))"
                )
                latest_update_set_exprs.append(f"s.{gc['name']} = {grid_expr}")

            case_3_latest_month_patch_df.createOrReplaceTempView("latest_case_3_latest_month_patch")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3_latest_month_patch c
                    ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                    WHEN MATCHED AND c.{ts} >= s.{ts}
                    THEN UPDATE SET {', '.join(latest_update_set_exprs)}
                """
            )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated latest_summary | MERGE - CASE III Latest-Month Patch | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        # Apply soft-delete future array/grid patches to latest_summary
        # only when latest row month matches the patched summary month.
        process_start_time = time.time()
        if case_3d_future_df is not None:
            latest_soft_delete_cols = list(dict.fromkeys([ts] + history_cols + grid_cols))
            latest_soft_delete_set_exprs = []
            for col_name in latest_soft_delete_cols:
                if col_name == ts:
                    latest_soft_delete_set_exprs.append(f"s.{col_name} = GREATEST(s.{col_name}, c.{col_name})")
                elif col_name in history_cols:
                    latest_soft_delete_set_exprs.append(
                        f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                    )
                else:
                    latest_soft_delete_set_exprs.append(f"s.{col_name} = c.{col_name}")

            # Guard against accidental duplicate source keys for latest merge.
            latest_case_3d_future_df = (
                case_3d_future_df
                .select(pk, prt, *latest_soft_delete_cols)
                .dropDuplicates([pk, prt])
            )
            latest_case_3d_future_df.createOrReplaceTempView("latest_case_3d_future")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3d_future c
                    ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                    WHEN MATCHED
                    THEN UPDATE SET {', '.join(latest_soft_delete_set_exprs)}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated latest_summary | MERGE - CASE III Soft Delete Future Patches | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        process_start_time = time.time()
        if case_3d_latest_history_patch_df is not None:
            latest_case_3d_history_df = case_3d_latest_history_patch_df
            latest_case_3d_history_df = align_history_arrays_to_length(
                latest_case_3d_history_df,
                rolling_columns,
                latest_history_len,
            )
            latest_shared_cols, latest_update_set_expr = build_latest_merge_columns(
                config,
                latest_case_3d_history_df.columns,
                pk,
            )
            latest_case_3d_history_df.select(*latest_shared_cols).createOrReplaceTempView("latest_case_3d_history_patch")
            spark.sql(
                f"""
                    MERGE INTO {latest_summary_table} s
                    USING latest_case_3d_history_patch c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        c.{prt} > s.{prt}
                        OR (c.{prt} = s.{prt} AND c.{ts} >= s.{ts})
                    ) THEN UPDATE SET {latest_update_set_expr}
                """
            )
            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(
                f"Updated latest_summary | MERGE - CASE III soft-delete history patches | "
                f"Time Elapsed: {process_total_minutes:.2f} minutes"
            )
            logger.info("-" * 60)

        # Apply month-row soft-delete flags to latest_summary when latest row month was deleted.
        process_start_time = time.time()
        if case_3d_month_df is not None:
            latest_cols = set(get_latest_cols(config))
            if SOFT_DELETE_COLUMN in latest_cols:
                latest_case_3d_month_df = (
                    case_3d_month_df
                    .select(pk, prt, ts, SOFT_DELETE_COLUMN)
                    .dropDuplicates([pk, prt])
                )
                latest_case_3d_month_df.createOrReplaceTempView("latest_case_3d_month")
                spark.sql(
                    f"""
                        MERGE INTO {latest_summary_table} s
                        USING latest_case_3d_month c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED THEN UPDATE SET
                            s.{SOFT_DELETE_COLUMN} = c.{SOFT_DELETE_COLUMN},
                            s.{ts} = GREATEST(s.{ts}, c.{ts})
                    """
                )
                process_end_time = time.time()
                process_total_minutes = (process_end_time - process_start_time) / 60
                logger.info(
                    f"Updated latest_summary | MERGE - CASE III Soft Delete Month Flags | "
                    f"Time Elapsed: {process_total_minutes:.2f} minutes"
                )
                logger.info("-" * 60)

        # Reconstruct latest_summary when the latest month itself was soft-deleted.
        process_start_time = time.time()
        if case_3d_month_df is not None:
            case_3d_month_df.select(pk, prt).distinct().createOrReplaceTempView("case_3d_deleted_months")
            delete_codes_sql = ",".join([f"'{code}'" for code in SOFT_DELETE_CODES])

            deleted_latest_accounts = spark.sql(
                f"""
                    SELECT l.{pk} as {pk}
                    FROM {latest_summary_table} l
                    JOIN case_3d_deleted_months d
                      ON l.{pk} = d.{pk}
                     AND l.{prt} = d.{prt}
                    GROUP BY l.{pk}
                """
            )

            if not deleted_latest_accounts.isEmpty():
                deleted_latest_accounts.createOrReplaceTempView("deleted_latest_accounts")
                replacement_df = spark.sql(
                    f"""
                        SELECT *
                        FROM (
                            SELECT
                                s.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY s.{pk}
                                    ORDER BY s.{prt} DESC, s.{ts} DESC
                                ) as _rn
                            FROM {summary_table} s
                            JOIN deleted_latest_accounts a ON s.{pk} = a.{pk}
                            WHERE COALESCE(s.{SOFT_DELETE_COLUMN}, '') NOT IN ({delete_codes_sql})
                        ) x
                        WHERE _rn = 1
                    """
                ).drop("_rn")

                if not replacement_df.isEmpty():
                    latest_cols_set = set(get_latest_cols(config))
                    replacement_cols = [c for c in replacement_df.columns if c in latest_cols_set]
                    if pk not in replacement_cols:
                        raise ValueError(f"Primary key '{pk}' missing from latest_summary replacement columns")
                    replacement_update_exprs = []
                    for col_name in replacement_cols:
                        if col_name == pk:
                            continue
                        if col_name in history_cols:
                            replacement_update_exprs.append(
                                f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                            )
                        else:
                            replacement_update_exprs.append(f"s.{col_name} = c.{col_name}")
                    replacement_df.select(*replacement_cols).createOrReplaceTempView("latest_delete_replacements")
                    spark.sql(
                        f"""
                            MERGE INTO {latest_summary_table} s
                            USING latest_delete_replacements c
                            ON s.{pk} = c.{pk}
                            WHEN MATCHED THEN UPDATE SET {', '.join(replacement_update_exprs)}
                        """
                    )
                    logger.info("Updated latest_summary | Reconstructed deleted-latest accounts")
                else:
                    logger.info("No latest_summary reconstruction candidates after soft deletes")
            else:
                logger.info("No latest_summary rows pointed to deleted months")

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Latest reconstruction for soft deletes | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)


    except Exception as e:
        logger.error(f"BACKFILL MERGE FAILED: {e}", exc_info=True)
        raise


def write_forward_results(spark: SparkSession, config: Dict[str, Any], expected_rows: Optional[int] = None):
    summary_table = config['destination_table']
    latest_summary_table = config["latest_history_table"]
    pk = config['primary_column']
    prt = config['partition_column']
    ts = config['max_identifier_column']
    summary_history_len = get_summary_history_len(config)
    latest_history_len = get_latest_history_len(config)
    rolling_columns = config.get('rolling_columns', [])
    history_cols = [f"{rc['name']}_history" for rc in rolling_columns]
        
    try:
        logger.info("-" * 60)
        logger.info("MERGING FORWARD RECORDS:")
        process_start_time = time.time()

        if spark.catalog.tableExists('temp_catalog.checkpointdb.case_2'):
            case_2_df = spark.read.table('temp_catalog.checkpointdb.case_2')
            merge_window = Window.partitionBy(pk, prt).orderBy(F.col(ts).desc())
            case_2_merge_df = (
                case_2_df
                .withColumn("_rn", F.row_number().over(merge_window))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )

            case_2_overflow = float(config.get("case2_merge_overflow_ratio", 0.10))
            case_2_chunks = build_month_chunks_from_df(
                case_2_merge_df,
                prt,
                overflow_ratio=case_2_overflow,
            )
            logger.info(f"Case II summary merge chunks: {len(case_2_chunks)}")

            for idx, months in enumerate(case_2_chunks, 1):
                case_2_chunk_df = case_2_merge_df.filter(F.col(prt).isin(months))
                case_2_chunk_df = align_for_summary_merge(
                    spark=spark,
                    df=case_2_chunk_df,
                    config=config,
                    stage=f"summary_merge_case_2_chunk_{idx}",
                    expected_rows=expected_rows,
                )
                case_2_chunk_df.createOrReplaceTempView("case_2_summary_chunk")

                spark.sql(
                    f"""
                        MERGE INTO {summary_table} s
                        USING case_2_summary_chunk c
                        ON s.{pk} = c.{pk} AND s.{prt} = c.{prt}
                        WHEN MATCHED AND c.{ts} >= s.{ts} THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """
                )
                logger.info(
                    f"MERGED - CASE II chunk {idx}/{len(case_2_chunks)} "
                    f"(months={months})"
                )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated Summary | MERGED - CASE II (Forward Records) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)


            process_start_time = time.time()
            # Get Latest from case 2 for merging to latest_summary
            window_spec = Window.partitionBy(pk).orderBy(F.col(prt).desc())
            latest_case_2_df = (
                case_2_merge_df
                .withColumn("_rn", F.row_number().over(window_spec))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
            latest_case_2_df = align_history_arrays_to_length(
                latest_case_2_df,
                rolling_columns,
                latest_history_len,
            )
            latest_cols_set = set(get_latest_cols(config))
            latest_shared_cols = [c for c in latest_case_2_df.columns if c in latest_cols_set]
            if pk not in latest_shared_cols:
                raise ValueError(f"Primary key '{pk}' missing from latest_summary merge columns")
            latest_update_set_exprs = []
            for col_name in latest_shared_cols:
                if col_name == pk:
                    continue
                if col_name in history_cols:
                    latest_update_set_exprs.append(
                        f"s.{col_name} = {latest_history_preserve_tail_expr(col_name, summary_history_len, latest_history_len)}"
                    )
                else:
                    latest_update_set_exprs.append(f"s.{col_name} = c.{col_name}")
            latest_case_2_df.select(*latest_shared_cols).createOrReplaceTempView("case_2")

            spark.sql(
                f"""           
                    MERGE INTO {latest_summary_table} s
                    USING case_2 c
                    ON s.{pk} = c.{pk}
                    WHEN MATCHED AND (
                        c.{prt} > s.{prt}
                        OR (c.{prt} = s.{prt} AND c.{ts} >= s.{ts})
                    ) THEN UPDATE SET {', '.join(latest_update_set_exprs)}
                """
            )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Updated latest_summary | MERGE - CASE II (Forward Records) | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)
        
        else:
            logger.info(f"No forward records to update")
            logger.info("-" * 60)

    except Exception as e:
        logger.error(f"FORWARD MERGE FAILED: {e}", exc_info=True)
        raise


def run_pipeline(spark: SparkSession, config: Dict[str, Any]):
    """  
    Pipeline Processing Order (CRITICAL for correctness):
    1. Backfill FIRST (rebuilds history with corrections)
    2. New accounts SECOND (no dependencies)
    3. Bulk Historical THIRD (builds arrays for new multi-month accounts)
    4. Forward LAST (uses corrected history from backfill)
    
    Args:
        spark: SparkSession
        config: Config dict
        filter_expr: Optional SQL filter for accounts table
    
    Returns:
        None
    """

    logger.info("=" * 80)
    logger.info("SUMMARY PIPELINE - START")
    logger.info("=" * 80)

    stats = {
        'total_records': 0,
        'case_i_records': 0,
        'case_ii_records': 0,
        'case_iii_records': 0,
        'case_iv_records': 0,
        'records_written': 0,
        'temp_table': None
    }

    run_id = f"summary_inc_{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"
    run_start_states: Optional[Dict[str, Dict[str, Any]]] = None

    try:
        ensure_soft_delete_columns(spark, config)
        preload_run_table_columns(spark, config)
        run_start_states = log_current_snapshot_state(spark, config, label="Pre-run")
        mark_run_started(spark, config, run_id=run_id, start_states=run_start_states)
        process_start_time = time.time()    

        # Step 1: Load and classify
        classified = load_and_classify_accounts(spark, config)
        classified.persist(StorageLevel.DISK_ONLY)

        case_stats = classified.groupBy("case_type", "_is_soft_delete").count().collect()
        case_breakdown: Dict[str, Dict[str, int]] = {}
        soft_delete_count = 0
        for row in case_stats:
            case_type = row["case_type"]
            is_delete = bool(row["_is_soft_delete"])
            count = int(row["count"])
            if case_type not in case_breakdown:
                case_breakdown[case_type] = {"normal": 0, "delete": 0}
            if is_delete:
                case_breakdown[case_type]["delete"] += count
                soft_delete_count += count
            else:
                case_breakdown[case_type]["normal"] += count

        logger.info("-" * 60)
        logger.info("CLASSIFICATION RESULTS:")
        for case_type in sorted(case_breakdown.keys()):
            total_count = case_breakdown[case_type]["normal"] + case_breakdown[case_type]["delete"]
            delete_count = case_breakdown[case_type]["delete"]
            if delete_count > 0:
                logger.info(f"  {case_type}: {total_count:,} records (soft-delete={delete_count:,})")
            else:
                logger.info(f"  {case_type}: {total_count:,} records")
        logger.info("-" * 60)

        process_end_time = time.time()
        process_total_minutes = (process_end_time - process_start_time) / 60
        logger.info(f"Classification | Time Elapsed: {process_total_minutes:.2f} minutes")

        stats['case_i_records'] = case_breakdown.get('CASE_I', {}).get("normal", 0) + case_breakdown.get('CASE_I', {}).get("delete", 0)
        stats['case_ii_records'] = case_breakdown.get('CASE_II', {}).get("normal", 0) + case_breakdown.get('CASE_II', {}).get("delete", 0)
        stats['case_iii_records'] = case_breakdown.get('CASE_III', {}).get("normal", 0) + case_breakdown.get('CASE_III', {}).get("delete", 0)
        stats['case_iv_records'] = case_breakdown.get('CASE_IV', {}).get("normal", 0) + case_breakdown.get('CASE_IV', {}).get("delete", 0)
        stats['total_records'] = (
            stats['case_i_records'] + stats['case_ii_records'] + stats['case_iii_records'] + stats['case_iv_records']
        )
        logger.info(f"Soft-delete rows in batch: {soft_delete_count:,}")

        materialize_working_set_context_tables(spark, classified, config)

        # =========================================================================
        # STEP 2: PROCESS EACH CASE AND WRITE TO TEMP TABLE
        # Process in CORRECT order: Backfill -> New -> Bulk Historical -> Forward
        # =========================================================================

        # 2a. Process Case III (Backfill) - HIGHEST PRIORITY
        case_iii_df_all = classified.filter(F.col("case_type") == "CASE_III")
        case_iii_df = case_iii_df_all.filter(~F.col("_is_soft_delete"))
        case_iii_delete_df = case_iii_df_all.filter(F.col("_is_soft_delete"))
        case_iii_count = case_breakdown.get("CASE_III", {}).get("normal", 0)
        case_iii_delete_count = case_breakdown.get("CASE_III", {}).get("delete", 0)
        if case_iii_count > 0:
            logger.info(f"\n>>> PROCESSING BACKFILL ({case_iii_count:,} records)")
            process_case_iii(spark, case_iii_df, config, expected_rows=case_iii_count)
        if case_iii_delete_count > 0:
            logger.info(f"\n>>> PROCESSING SOFT DELETE BACKFILL ({case_iii_delete_count:,} records)")
            process_case_iii_soft_delete(spark, case_iii_delete_df, config, expected_rows=case_iii_delete_count)

        # 2b. Process Case I (New Accounts - first month only)
        case_i_df = classified.filter((F.col("case_type") == "CASE_I") & (~F.col("_is_soft_delete")))
        case_i_count = case_breakdown.get("CASE_I", {}).get("normal", 0)
        case_i_result = None
        if case_i_count > 0:
            process_start_time = time.time()
            logger.info(f"\n>>> PROCESSING NEW ACCOUNTS ({case_i_count:,} records)")
            case_i_result = process_case_i(case_i_df, config)
            case_i_result.persist(StorageLevel.MEMORY_AND_DISK)
            write_case_table_bucketed(
                spark=spark,
                df=case_i_result,
                table_name="temp_catalog.checkpointdb.case_1",
                config=config,
                stage="case_1_temp",
                expected_rows=case_i_count,
            )

            process_end_time = time.time()
            process_total_minutes = (process_end_time - process_start_time) / 60
            logger.info(f"Case I Generated | Time Elapsed: {process_total_minutes:.2f} minutes")
            logger.info("-" * 60)

        # 2c. Process Case IV (Bulk Historical) - after Case I so we have first months
        case_iv_df = classified.filter((F.col("case_type") == "CASE_IV") & (~F.col("_is_soft_delete")))
        case_iv_count = case_breakdown.get("CASE_IV", {}).get("normal", 0)
        if case_iv_count > 0:
            logger.info(f"\n>>> PROCESSING BULK HISTORICAL ({case_iv_count:,} records)")
            if case_i_result is not None:
                process_case_iv(spark, case_iv_df, case_i_result, config, expected_rows=case_iv_count)
                case_i_result.unpersist()
                case_i_result = None
            else:
                logger.warning("Skipping Case IV processing due to missing active Case I base rows")

        write_backfill_results(
            spark,
            config,
            expected_rows_append=(case_i_count + case_iv_count)
        )

        # 2d. Process Case II (Forward Entries) - LOWEST PRIORITY
        case_ii_df = classified.filter((F.col("case_type") == "CASE_II") & (~F.col("_is_soft_delete")))
        case_ii_count = case_breakdown.get("CASE_II", {}).get("normal", 0)
        if case_ii_count > 0:
            logger.info(f"\n>>> PROCESSING FORWARD ENTRIES ({case_ii_count:,} records)")
            process_case_ii(spark, case_ii_df, config, expected_rows=case_ii_count)
            write_forward_results(spark, config, expected_rows=case_ii_count)

        logger.info("PROCESS COMPLETED - Deleting the persisted classification results")
        classified.unpersist()
        finalize_run_tracking(
            spark=spark,
            config=config,
            run_id=run_id,
            start_states=run_start_states,
            success=True,
            error_message=None,
        )
        log_current_snapshot_state(spark, config, label="Post-run")

    except Exception as e:
        failure_message = str(e)
        try:
            if run_start_states is not None:
                rollback_statuses = rollback_tables_to_run_start(
                    spark=spark,
                    config=config,
                    start_states=run_start_states,
                )
                rollback_status_message = "; ".join(
                    [f"{k}={v}" for k, v in sorted(rollback_statuses.items())]
                )
                if rollback_status_message:
                    failure_message = f"{failure_message} | rollback: {rollback_status_message}"
                finalize_run_tracking(
                    spark=spark,
                    config=config,
                    run_id=run_id,
                    start_states=run_start_states,
                    success=False,
                    error_message=failure_message,
                )
            else:
                refresh_watermark_tracker(spark, config, mark_committed=False)
        except Exception as tracker_error:
            logger.warning(f"Failed to refresh watermark tracker after pipeline error: {tracker_error}")
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


def cleanup(spark: SparkSession):
    cleanup_tables = [
        'case_1',
        'case_2',
        'case_3a',
        'case_3b',
        'case_3_latest_month_patch',
        'case_3_unified_latest_month_patch',
        'case_3_latest_history_context_patch',
        'case_3d_month',
        'case_3d_future',
        'case_3d_latest_history_context_patch',
        'case_3d_unified_latest_history_patch',
        'case_4',
        'workset_latest_summary',
        'workset_summary_case3',
    ]

    for table in cleanup_tables:
        spark.sql(f"DROP TABLE IF EXISTS temp_catalog.checkpointdb.{table}")
        logger.info(f"Cleaned {table}")

    for view_name in ["classified_accounts_all", "case3_accounts_all"]:
        if spark.catalog.tableExists(view_name):
            spark.catalog.dropTempView(view_name)

    logger.info("CLEANUP COMPLETED")
    logger.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Summary Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--config', type=json.loads, required=True, help='{"bucket_name":{bucket_name},"key":{key}}Path to pipeline config JSON')
    parser.add_argument('--mode', choices=['incremental', 'full'], default='incremental',
                       help='Processing mode')
    
    args = parser.parse_args()
    
    # Load config from s3
    config = load_config(args.config['bucket_name'], args.config['key'])
    if not validate_config(config):
        sys.exit(1)
    
    # Create Spark session
    spark = create_spark_session(config['spark']['app_name'], config['spark'])
    spark.sparkContext.setLogLevel("WARN")
    
    try: 
        # Cleanup of existing temp_catalog      
        cleanup(spark)
        run_pipeline(spark, config)
        logger.info(f"Pipeline completed")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)

    finally:

        end_time = time.time()
        total_minutes = (end_time-start_time)/60
        logger.info("COMPLETED".center(80,"="))    
        logging.info(f"Total Execution Time :{total_minutes:.2f} minutes")
        spark.stop()


if __name__ == "__main__":
    main()

