# Databricks notebook source
"""Create the SQL functions used by the dbt on-run-end governance hook."""

import re


for name, default in [
    ("catalog", "main"),
    ("schema", "dbt_dqx_demo"),
    ("workspace_host", ""),
    ("gate_job_id", ""),
    ("token_scope", "dqx-governance-gate"),
    ("token_key", "jobs-api-token"),
]:
    dbutils.widgets.text(name, default)

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
workspace_host = dbutils.widgets.get("workspace_host").rstrip("/")
gate_job_id = dbutils.widgets.get("gate_job_id")
token_scope = dbutils.widgets.get("token_scope")
token_key = dbutils.widgets.get("token_key")

identifier = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
secret_name = re.compile(r"^[A-Za-z0-9_.-]+$")
if not workspace_host:
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl").rstrip("/")
    workspace_host = (
        workspace_url
        if workspace_url.startswith("https://")
        else f"https://{workspace_url}"
    )
if not identifier.fullmatch(catalog) or not identifier.fullmatch(schema):
    raise ValueError("catalog and schema must contain only letters, digits, and underscores")
if not re.fullmatch(r"https://[A-Za-z0-9.-]+", workspace_host):
    raise ValueError("workspace_host must be an HTTPS Databricks workspace URL")
if not gate_job_id.isdigit():
    raise ValueError("gate_job_id must be a Databricks job ID")
if not secret_name.fullmatch(token_scope) or not secret_name.fullmatch(token_key):
    raise ValueError("token scope and key contain unsupported characters")

inner_function = f"{catalog}.{schema}._invoke_dqx_gate"
outer_function = f"{catalog}.{schema}.run_dqx_gate"

spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION {inner_function}(
      models_json STRING,
      invocation_id STRING,
      gate_catalog STRING,
      gate_schema STRING,
      api_token STRING
    )
    RETURNS STRING
    LANGUAGE PYTHON
    NOT DETERMINISTIC
    COMMENT 'Internal synchronous Databricks Jobs API client for the dbt DQX gate'
    AS $$
import base64
import json
import time
import urllib.error
import urllib.parse
import urllib.request

WORKSPACE_HOST = {workspace_host!r}
GATE_JOB_ID = {int(gate_job_id)}
POLL_SECONDS = 5
MAX_WAIT_SECONDS = 1800

def api_request(method, path, token, payload=None):
    body = None
    headers = {{"Authorization": "Bearer " + token}}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        WORKSPACE_HOST + path,
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Databricks API {{exc.code}}: {{detail[:500]}}")

models = json.loads(models_json)
if not isinstance(models, list) or not models:
    raise ValueError("The dbt hook did not resolve any models")

models_b64 = base64.b64encode(
    json.dumps(models, separators=(",", ":")).encode("utf-8")
).decode("ascii")

run = api_request(
    "POST",
    "/api/2.1/jobs/run-now",
    api_token,
    {{
        "job_id": GATE_JOB_ID,
        "idempotency_token": invocation_id,
        "job_parameters": {{
            "models_b64": models_b64,
            "invocation_id": invocation_id,
            "catalog": gate_catalog,
            "schema": gate_schema,
        }},
    }},
)
run_id = int(run["run_id"])
deadline = time.monotonic() + MAX_WAIT_SECONDS

while True:
    current = api_request(
        "GET",
        "/api/2.1/jobs/runs/get?run_id=" + urllib.parse.quote(str(run_id)),
        api_token,
    )
    state = current.get("state", {{}})
    lifecycle = state.get("life_cycle_state")
    if lifecycle in {{"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}}:
        return json.dumps(
            {{
                "run_id": run_id,
                "life_cycle_state": lifecycle,
                "result_state": state.get("result_state"),
                "state_message": state.get("state_message"),
                "run_page_url": current.get("run_page_url"),
            }},
            separators=(",", ":"),
        )
    if time.monotonic() >= deadline:
        raise TimeoutError(f"DQX job {{run_id}} did not finish within {{MAX_WAIT_SECONDS}} seconds")
    time.sleep(POLL_SECONDS)
$$
    """
)

# SQL UDFs execute with the owner's authorization. The dbt user only needs
# EXECUTE on this wrapper; the Jobs API token stays in the secret scope.
spark.sql(
    f"""
    CREATE OR REPLACE FUNCTION {outer_function}(
      models_json STRING,
      invocation_id STRING,
      gate_catalog STRING,
      gate_schema STRING
    )
    RETURNS STRING
    NOT DETERMINISTIC
    COMMENT 'Run centrally managed DQX controls synchronously after dbt'
    RETURN {inner_function}(
      models_json,
      invocation_id,
      gate_catalog,
      gate_schema,
      secret('{token_scope}', '{token_key}')
    )
    """
)

print(f"Created dbt governance hook function: {outer_function}")
