# Databricks notebook source
"""Seed an example Delta-backed DQX rule registry.

In a production deployment, a governance UI or control-plane service can
publish the same DQX check payloads. This notebook keeps the repository
self-contained and makes the registry contract explicit.
"""

from databricks.labs.dqx.config import TableChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dbt_dqx_demo")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checks_table = f"{catalog}.{schema}.dqx_rules"
gate_runs_table = f"{catalog}.{schema}.governance_gate_runs"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {gate_runs_table} (
      invocation_id STRING,
      job_run_id STRING,
      dbt_unique_id STRING,
      relation_name STRING,
      status STRING,
      input_row_count BIGINT,
      error_row_count BIGINT,
      warning_row_count BIGINT,
      rule_count INT,
      evaluated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'One DQX governance summary per dbt invocation and model'
    """
)

checks = [
    {
        "name": "customer_id_required",
        "criticality": "error",
        "check": {
            "function": "is_not_null_and_not_empty",
            "arguments": {"column": "customer_id"},
        },
        "user_metadata": {
            "owner": "data-governance",
            "control_id": "GOV-CUSTOMER-001",
            "business_term": "Customer",
            "description": "Every governed customer row must have a business identifier.",
        },
    },
    {
        "name": "email_format_valid",
        "criticality": "error",
        "check": {
            "function": "regex_match",
            "arguments": {
                "column": "email",
                "regex": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
            },
        },
        "user_metadata": {
            "owner": "data-governance",
            "control_id": "GOV-CUSTOMER-002",
            "business_term": "Customer email",
            "description": "Customer email must follow the governed contact-data format.",
        },
    },
    {
        "name": "country_code_iso2",
        "criticality": "warn",
        "check": {
            "function": "regex_match",
            "arguments": {
                "column": "country_code",
                "regex": "^[A-Z]{2}$",
            },
        },
        "user_metadata": {
            "owner": "data-governance",
            "control_id": "GOV-CUSTOMER-003",
            "business_term": "Country",
            "description": "Country should use an ISO-3166 alpha-2 code.",
        },
    },
]

run_config_name = "model.dbt_dqx_demo.dim_customer"
engine = DQEngine(WorkspaceClient())
config = TableChecksStorageConfig(
    location=checks_table,
    run_config_name=run_config_name,
    mode="overwrite",
)
engine.save_checks(checks, config=config)

saved = engine.load_checks(config=config)
assert len(saved) == len(checks), f"Expected {len(checks)} checks, loaded {len(saved)}"
print(f"Saved {len(saved)} governed DQX checks for {run_config_name} to {checks_table}")
display(spark.table(checks_table).orderBy("name"))
