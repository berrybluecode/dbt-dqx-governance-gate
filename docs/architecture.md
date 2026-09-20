# Architecture and ownership

## Runtime boundary

Native dbt does not execute the Python DQX engine. The shared workflow keeps
the boundary explicit:

```text
dbt build/test
    |
    +-- target/manifest.json
    +-- target/run_results.json
              |
              v
shared governance gate
    |
    +-- select successful models
    +-- resolve checks by dbt unique_id
    +-- run a reusable Databricks DQX job
    +-- write metrics, quarantine rows and summaries
    +-- return PASS / WARN / non-zero BLOCK
```

This gives developers one CI workflow and familiar test-style output without
embedding PySpark orchestration in a dbt macro or generic SQL test.

## Which rule goes where?

Use **dbt tests** when a rule is coupled to the implementation contract of a
model:

- primary-key uniqueness
- model relationships
- transformation invariants
- product-specific freshness

Use **centrally managed DQX controls** when a rule follows shared business
meaning or policy:

- business-term identifiers
- enterprise reference-data formats
- governed PII/contact-data formats
- controls reused across multiple products
- centrally managed severity, scope or exceptions

Do not maintain the same rule in both systems. If a dbt test already provides
equivalent coverage, record that mapping in the control plane rather than
generating a duplicate DQX rule.

## Ownership

### Analytics Engineering

- owns dbt models and model-local tests
- responds to data quality findings in the normal workflow
- fixes source or transformation defects

### Data Governance

- owns business terms and enterprise controls
- sets applicability, severity and exception policy
- manages the rule lifecycle in the central registry

### Data Platform

- owns the shared post-dbt integration
- operates the reusable DQX job
- provides authentication, observability and execution SLOs

### Data Owners

- approve business meaning and acceptable risk
- resolve policy disputes and approve exceptions

## Production considerations

This repository is an executable starter, not a complete enterprise control
plane. Before production use, add:

- identity-aware rule approval and change history
- catalog/tag-based applicability beyond exact dbt `unique_id`
- retention and access controls for quarantined data
- alert routing and ownership escalation
- concurrency tests for your job retry policy
- explicit compatibility testing when upgrading DQX, dbt or the adapter
