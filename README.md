# dbt + Databricks DQX governance gate

Add centrally managed Databricks DQX controls after an existing dbt run
without rewriting dbt models, copying governance rules into YAML, or asking
analytics engineers to adopt a second daily workflow.

This repository is an executable starter for an **artifact-driven,
post-dbt governance gate**.

## Why this pattern?

dbt tests and governance controls solve related but different problems:

- **dbt tests** protect a model's engineering and transformation contract.
- **DQX controls** protect shared business meaning, policy and cross-product
  consistency.

Forcing every governed rule into dbt YAML creates duplicate ownership,
application pull requests for policy changes and inevitable rule drift. This
pattern keeps each rule in its natural control plane while returning one
developer-friendly result.

## What the workflow does

```text
existing dbt build/test
        |
        +-- on-run-end macro selects successful/tested models
                  |
                  v
Unity Catalog SQL function
        |
        +-- securely reads the Jobs API credential
        +-- submits model unique_id + relation to one DQX job
        +-- waits synchronously for completion
                  |
                  v
Delta-backed DQX registry --> DQX evaluation
                                  |
                                  +-- metrics
                                  +-- quarantine rows
                                  +-- PASS / WARN / BLOCK
                                             |
                                             v
                                  dbt-style CI output + exit code
```

`dbt test` does not embed the DQX engine. Its `on-run-end` hook calls a
Unity Catalog SQL wrapper around a Python UDF, which invokes the reusable DQX
job and waits for it. This keeps both runtimes native while giving analytics
engineers one command and one result.

## Included

- a Databricks Asset Bundle with a registry setup job and reusable DQX gate
- a Delta-backed DQX rule registry
- idempotent summary and quarantine writes for job retries
- a secure SQL/Python orchestration function and dbt `on-run-end` macro
- dbt artifact parsing for model selection
- concise, colored, dbt-style PASS/WARN/FAIL output
- a small dbt project with pass, warning and blocking scenarios
- unit tests for artifact selection and output formatting

The presentation website used to explain the design is intentionally not part
of this runtime repository.

## Prerequisites

- Python 3.11 or 3.12
- Databricks CLI with Asset Bundle support
- a Databricks workspace with Unity Catalog
- permission to create a schema and Delta tables
- permission to create Unity Catalog functions and a Databricks secret scope
- Databricks serverless jobs
- a Pro or Serverless SQL warehouse that supports Unity Catalog Python UDFs

## Quick start

### 1. Clone and install

```bash
git clone https://github.com/berrybluecode/dbt-dqx-governance-gate.git
cd dbt-dqx-governance-gate

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Install or upgrade the Databricks CLI separately if needed.

### 2. Configure your environment

```bash
cp .env.example .env
```

Edit `.env`, then export the values:

```bash
set -a
source .env
set +a
```

`DBT_DQX_CATALOG` and `DBT_DQX_SCHEMA` currently accept letters, digits and
underscores. `DATABRICKS_WAREHOUSE_ID` is optional when
`DATABRICKS_HTTP_PATH` ends in `/warehouses/<warehouse-id>`; the runner derives
it automatically.

Authenticate the Databricks CLI using OAuth:

```bash
databricks auth login --host "$DATABRICKS_HOST"
```

This login authenticates the Databricks CLI and Asset Bundle. The example dbt
profile uses `auth_type: oauth`; dbt may open its own browser authorization
the first time it connects. Remove any stale `DATABRICKS_TOKEN` if OAuth
reports conflicting credentials.

No token or workspace-specific identifier should be committed to this
repository.

### 3. Store the orchestration credential

Create a workspace secret scope and store a short-lived token for a dedicated
service principal that can run and read the DQX job. For a local demo, a
short-lived user token is sufficient:

```bash
databricks secrets create-scope dqx-governance-gate
read -s DQX_JOBS_API_TOKEN
databricks secrets put-secret dqx-governance-gate jobs-api-token \
  --string-value "$DQX_JOBS_API_TOKEN"
unset DQX_JOBS_API_TOKEN
```

The setup job creates an owner-authorized SQL wrapper, so dbt users need
`EXECUTE` on `run_dqx_gate`, not direct access to the secret.

### 4. Validate and deploy

Choose a Unity Catalog catalog where you can create the demo schema:

```bash
databricks bundle validate -t dev \
  --var="catalog=$DBT_DQX_CATALOG,schema=$DBT_DQX_SCHEMA"

databricks bundle deploy -t dev \
  --var="catalog=$DBT_DQX_CATALOG,schema=$DBT_DQX_SCHEMA"
```

### 5. Create the registry, rules, and SQL functions

```bash
databricks bundle run setup_dqx_registry -t dev \
  --var="catalog=$DBT_DQX_CATALOG,schema=$DBT_DQX_SCHEMA"
```

`src/dqx/setup_rules.py` publishes the example rules.
`src/dqx/setup_orchestration.py` creates the secure SQL/Python functions. In a
larger implementation, DQX Studio or another governance service can publish
the same native DQX payloads into the Delta-backed registry.

### 6. Run with native dbt

The example project already contains the hook. Enable it and use normal dbt:

```bash
export DBT_DQX_GATE_ENABLED=true

dbt build \
  --project-dir example/dbt_project \
  --profiles-dir example/dbt_project \
  --vars '{scenario: pass}'
```

For a standalone `dbt test`, first materialize the desired demo data, then run
the familiar test command:

```bash
DBT_DQX_GATE_ENABLED=false dbt run \
  --project-dir example/dbt_project \
  --profiles-dir example/dbt_project \
  --vars '{scenario: warn}'

dbt test \
  --project-dir example/dbt_project \
  --profiles-dir example/dbt_project
```

PASS and WARN leave dbt successful. BLOCK returns a normal failing dbt hook
with the failed control name and a non-zero exit code.

### 7. Optional wrapper-script scenarios

Put the virtual environment on `PATH` so the runner finds dbt:

```bash
export PATH="$PWD/.venv/bin:$PATH"
```

Passing data:

```bash
python scripts/run_dbt_with_dqx.py --scenario pass
```

Warning-only governance result:

```bash
python scripts/run_dbt_with_dqx.py --scenario warn
```

Blocking governance result:

```bash
python scripts/run_dbt_with_dqx.py --scenario block
echo $?  # non-zero by design
```

All three scenarios pass the native dbt tests. The warning scenario violates
the governed country-code policy. The blocking scenario violates the governed
email-format policy.

Example gate output:

```text
08:26:38  1 of 3 PASS dqx_customer_id_required_on_dim_customer ....... [PASS]
08:26:38  2 of 3 FAIL dqx_email_format_valid_on_dim_customer ........ [FAIL 1]
08:26:38  3 of 3 PASS dqx_country_code_iso2_on_dim_customer ......... [PASS]

Failure in governance control email_format_valid on dim_customer
  Got 1 violating row(s) of 2.

08:26:38  Finished running 3 governance control(s) in 160.32s.
08:26:38  Done. PASS=2 WARN=0 ERROR=1 SKIP=0 TOTAL=3
```

Set `NO_COLOR=1` for plain output. Add `--verbose-gate` to stream raw
Databricks job output for debugging.

## Add the gate to an existing dbt pipeline

Install or copy `macros/dqx_on_run_end.sql`, then add one project-level hook:

```yaml
on-run-end:
  - "{{ run_dqx_governance_gate(results) }}"
```

The team's command remains unchanged:

```bash
export DBT_DQX_GATE_ENABLED=true
dbt build --select "$DBT_SELECTOR"
```

The macro reads dbt's in-memory `results` and graph. After `dbt build`, it
selects successful materialized models. After `dbt test`, it resolves the
tested parent models from test dependencies. No model SQL or test YAML changes.

`scripts/run_dbt_with_dqx.py --artifacts-only` remains available for
environments that cannot run Unity Catalog Python UDFs.

## Delta outputs

The demo creates these tables in `${DBT_DQX_CATALOG}.${DBT_DQX_SCHEMA}`:

- `dqx_rules`: native DQX controls keyed by dbt `unique_id`
- `dqx_metrics`: per-run and per-control metrics
- `dqx_quarantine`: warning and error rows with dbt invocation metadata
- `governance_gate_runs`: one PASS/WARN/BLOCK summary per invocation and model

## Rule placement and ownership

Use dbt tests for model-local behavior: uniqueness, relationships,
transformation invariants and product-specific freshness.

Use DQX for centrally governed business meaning: shared identifiers,
reference-data standards, PII/contact formats, cross-product thresholds,
severity and exceptions.

See [Architecture and ownership](docs/architecture.md) for the team model and
production considerations.

## Repository structure

```text
.
├── databricks.yml
├── resources/jobs.yml
├── src/dqx/
│   ├── setup_rules.py
│   ├── setup_orchestration.py
│   └── run_gate.py
├── scripts/run_dbt_with_dqx.py
├── example/dbt_project/macros/dqx_on_run_end.sql
├── tests/
└── docs/architecture.md
```

## Scope and limitations

This is a working reference implementation, not a full governance product.
It matches controls to exact dbt `unique_id` values and uses a setup notebook
as the example publisher. Production deployments should add approval
workflows, history, tag-based applicability, exception management, alert
routing, access controls and compatibility testing.

## License

MIT
