# Databricks notebook source
"""Evaluate centrally stored DQX checks for models selected from dbt artifacts."""

import base64
import json
import re
from datetime import datetime, timezone

from databricks.labs.dqx.actions import DQAction, FailPipeline
from databricks.labs.dqx.config import OutputConfig, TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.metrics_observer import DQMetricsObserver
from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


for name, default in [
    ("catalog", "main"),
    ("schema", "dbt_dqx_demo"),
    ("models_b64", "W10="),
    ("invocation_id", "manual"),
    ("job_run_id", "manual"),
]:
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
models = json.loads(base64.b64decode(dbutils.widgets.get("models_b64")).decode("utf-8"))
invocation_id = dbutils.widgets.get("invocation_id")
job_run_id = dbutils.widgets.get("job_run_id")

identifier = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
safe_relation = re.compile(r"^[A-Za-z0-9_`.-]+$")
safe_run_id = re.compile(r"^[A-Za-z0-9_.:-]+$")

if not identifier.fullmatch(catalog) or not identifier.fullmatch(schema):
    raise ValueError("catalog and schema must be simple SQL identifiers")
if not isinstance(models, list) or not models:
    raise ValueError("models_b64 must decode to at least one executed dbt model")
if not safe_run_id.fullmatch(invocation_id):
    raise ValueError(f"Unsafe dbt invocation ID: {invocation_id}")

checks_table = f"{catalog}.{schema}.dqx_rules"
quarantine_table = f"{catalog}.{schema}.dqx_quarantine"
metrics_table = f"{catalog}.{schema}.dqx_metrics"
gate_runs_table = f"{catalog}.{schema}.governance_gate_runs"

workspace_client = WorkspaceClient()
gate_run_schema = StructType(
    [
        StructField("invocation_id", StringType(), False),
        StructField("job_run_id", StringType(), False),
        StructField("dbt_unique_id", StringType(), False),
        StructField("relation_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("input_row_count", LongType(), False),
        StructField("error_row_count", LongType(), False),
        StructField("warning_row_count", LongType(), False),
        StructField("rule_count", IntegerType(), False),
        StructField("evaluated_at", TimestampType(), False),
    ]
)


def upsert_gate_summary(summary):
    summary_df = spark.createDataFrame([summary], gate_run_schema)
    summary_df.createOrReplaceTempView("_dqx_gate_summary")
    spark.sql(
        f"""
        MERGE INTO {gate_runs_table} AS target
        USING _dqx_gate_summary AS source
          ON target.invocation_id = source.invocation_id
         AND target.dbt_unique_id = source.dbt_unique_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


summaries = []
deferred_failure = None

for model in models:
    unique_id = model["unique_id"]
    relation_name = model["relation_name"]

    if not safe_relation.fullmatch(relation_name):
        raise ValueError(f"Unsafe relation name received from dbt artifacts: {relation_name}")
    if not safe_run_id.fullmatch(unique_id):
        raise ValueError(f"Unsafe dbt unique ID: {unique_id}")

    # Databricks may retry a failed serverless task. Remove previously written
    # violating rows for this dbt invocation/model before recomputing.
    if spark.catalog.tableExists(quarantine_table):
        spark.sql(
            f"""
            DELETE FROM {quarantine_table}
            WHERE _dq_invocation_id = '{invocation_id}'
              AND _dq_dbt_unique_id = '{unique_id}'
            """
        )
    if spark.catalog.tableExists(metrics_table):
        observer_name = f"{unique_id}:{invocation_id}"
        spark.sql(
            f"""
            DELETE FROM {metrics_table}
            WHERE run_name = '{observer_name}'
            """
        )

    observer = DQMetricsObserver(name=f"{unique_id}:{invocation_id}")
    engine = DQEngine(workspace_client, observer=observer)
    checks = engine.load_checks(
        config=TableChecksStorageConfig(
            location=checks_table,
            run_config_name=unique_id,
        )
    )

    if not checks:
        summary = (
            invocation_id,
            job_run_id,
            unique_id,
            relation_name,
            "SKIP",
            0,
            0,
            0,
            0,
            datetime.now(timezone.utc),
        )
        upsert_gate_summary(summary)
        summaries.append(
            {
                "unique_id": unique_id,
                "relation_name": relation_name,
                "status": "SKIP",
                "input_row_count": 0,
                "error_row_count": 0,
                "warning_row_count": 0,
                "rule_count": 0,
            }
        )
        print(f"SKIP {unique_id}: no centrally governed checks")
        continue

    input_df = spark.table(relation_name)
    valid_df, invalid_df, observation = engine.apply_checks_by_metadata_and_split(
        input_df,
        checks,
    )

    # Materialize both outputs once so the observer contains final metrics.
    valid_df.count()
    invalid_df.count()
    metrics = observation.get

    input_count = int(metrics.get("input_row_count", 0))
    error_count = int(metrics.get("error_row_count", 0))
    warning_count = int(metrics.get("warning_row_count", 0))
    status = "BLOCK" if error_count else ("WARN" if warning_count else "PASS")

    enriched_invalid_df = (
        invalid_df.withColumn("_dq_invocation_id", F.lit(invocation_id))
        .withColumn("_dq_job_run_id", F.lit(job_run_id))
        .withColumn("_dq_dbt_unique_id", F.lit(unique_id))
        .withColumn("_dq_relation_name", F.lit(relation_name))
        .withColumn("_dq_evaluated_at", F.current_timestamp())
    )

    engine.save_results_in_table(
        quarantine_df=enriched_invalid_df,
        observation=observation,
        quarantine_config=OutputConfig(
            location=quarantine_table,
            mode="append",
            options={"mergeSchema": "true"},
        ),
        metrics_config=OutputConfig(
            location=metrics_table,
            mode="append",
            options={"mergeSchema": "true"},
        ),
        run_config_name=unique_id,
    )

    summary = (
        invocation_id,
        job_run_id,
        unique_id,
        relation_name,
        status,
        input_count,
        error_count,
        warning_count,
        len(checks),
        datetime.now(timezone.utc),
    )
    upsert_gate_summary(summary)

    summaries.append(
        {
            "unique_id": unique_id,
            "relation_name": relation_name,
            "status": status,
            "input_row_count": input_count,
            "error_row_count": error_count,
            "warning_row_count": warning_count,
            "rule_count": len(checks),
        }
    )
    print(
        f"{status} {unique_id}: rows={input_count}, "
        f"errors={error_count}, warnings={warning_count}, rules={len(checks)}"
    )

    # Use DQX's native action for synchronous failure propagation, but defer
    # the exception until every selected model has produced an auditable result.
    action_engine = DQEngine(
        workspace_client,
        observer=observer,
        actions=[
            DQAction(
                condition="error_row_count > 0",
                action=FailPipeline(
                    message=(
                        f"Enterprise DQX gate blocked {unique_id}. "
                        f"See {quarantine_table} for violating rows."
                    )
                ),
            )
        ],
    )
    try:
        action_engine.evaluate_actions(metrics, input_location=relation_name)
    except Exception as exc:
        if not error_count:
            raise
        deferred_failure = deferred_failure or exc

print(json.dumps({"invocation_id": invocation_id, "models": summaries}, indent=2))
if deferred_failure:
    raise deferred_failure
