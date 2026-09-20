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
        +-- manifest.json
        +-- run_results.json
                  |
                  v
shared Python integration
        |
        +-- selects models that actually succeeded
        +-- submits model unique_id + relation to one DQX job
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

Native `dbt test` does **not** run the Python DQX engine internally. The
shared workflow runs dbt first and then invokes DQX synchronously from dbt's
artifacts. That boundary is intentional: it keeps dbt native and DQX native.

## Included

- a Databricks Asset Bundle with a registry setup job and reusable DQX gate
- a Delta-backed DQX rule registry
- idempotent summary and quarantine writes for job retries
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
- serverless jobs, or an equivalent job environment configured for DQX
- a SQL warehouse for dbt and result lookup

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

Authenticate the Databricks CLI using OAuth:

```bash
databricks auth login --host "$DATABRICKS_HOST"
```

No token or workspace-specific identifier should be committed to this
repository.

### 3. Validate and deploy

Choose a Unity Catalog catalog where you can create the demo schema:

```bash
databricks bundle validate -t dev \
  --var="catalog=$DBT_DQX_CATALOG,schema=$DBT_DQX_SCHEMA"

databricks bundle deploy -t dev \
  --var="catalog=$DBT_DQX_CATALOG,schema=$DBT_DQX_SCHEMA"
```

### 4. Create the registry and example rules

```bash
databricks bundle run setup_dqx_registry -t dev \
  --var="catalog=$DBT_DQX_CATALOG,schema=$DBT_DQX_SCHEMA"
```

`src/dqx/setup_rules.py` is a self-contained publisher for the example. In a
larger implementation, DQX Studio or another governance service can publish
the same native DQX payloads into the Delta-backed registry.

### 5. Run the scenarios

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
1 of 3 PASS dqx_customer_id_required_on_dim_customer ........ [PASS]
2 of 3 FAIL dqx_email_format_valid_on_dim_customer .......... [FAIL 1]
3 of 3 PASS dqx_country_code_iso2_on_dim_customer ........... [PASS]

Failure in governance control email_format_valid on dim_customer
  Got 1 violating row(s) of 2.

Done. PASS=2 WARN=0 ERROR=1 SKIP=0 TOTAL=3
```

Set `NO_COLOR=1` for plain output. Add `--verbose-gate` to stream raw
Databricks job output for debugging.

## Add the gate to an existing dbt pipeline

Keep the team's existing dbt step:

```bash
dbt build --select "$DBT_SELECTOR"
```

Then add one central step:

```bash
python /path/to/run_dbt_with_dqx.py \
  --artifacts-only \
  --project-dir "$DBT_PROJECT_DIR"
```

The integration reads `target/manifest.json` and `target/run_results.json`.
After `dbt build`, it selects successful materialized models directly. After a
standalone successful `dbt test`, it resolves the tested parent models from
each test node's dependencies. Teams already consuming a shared CI template
do not need repository-by-repository model or test changes.

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
│   └── run_gate.py
├── scripts/run_dbt_with_dqx.py
├── example/dbt_project/
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
