import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "run_dbt_with_dqx.py"
SPEC = importlib.util.spec_from_file_location("run_dbt_with_dqx", SCRIPT)
assert SPEC and SPEC.loader
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def test_successful_models_only_returns_materialized_successes():
    manifest = {
        "nodes": {
            "model.demo.good": {
                "resource_type": "model",
                "relation_name": "`main`.`demo`.`good`",
            },
            "model.demo.failed": {
                "resource_type": "model",
                "relation_name": "`main`.`demo`.`failed`",
            },
            "test.demo.good": {"resource_type": "test"},
        }
    }
    run_results = {
        "metadata": {"invocation_id": "inv-123"},
        "results": [
            {"unique_id": "model.demo.good", "status": "success"},
            {"unique_id": "model.demo.failed", "status": "error"},
            {"unique_id": "test.demo.good", "status": "pass"},
        ],
    }

    models, invocation_id, statuses = runner.successful_models(manifest, run_results)

    assert models == [
        {
            "unique_id": "model.demo.good",
            "relation_name": "`main`.`demo`.`good`",
        }
    ]
    assert invocation_id == "inv-123"
    assert statuses == {"success": 1, "error": 1, "pass": 1}


def test_progress_line_is_dbt_like_and_color_optional():
    line = runner.progress_line(
        2,
        3,
        "email_format_valid",
        "dim_customer",
        "FAIL",
        7,
        colors=False,
    )

    assert "2 of 3 FAIL dqx_email_format_valid_on_dim_customer" in line
    assert line.endswith("[FAIL 7]")
    assert "\033[" not in line


def test_successful_test_results_resolve_the_tested_model():
    manifest = {
        "nodes": {
            "model.demo.customer": {
                "resource_type": "model",
                "relation_name": "`main`.`demo`.`customer`",
            },
            "test.demo.customer_not_null": {
                "resource_type": "test",
                "depends_on": {"nodes": ["model.demo.customer"]},
            },
            "test.demo.customer_unique": {
                "resource_type": "test",
                "depends_on": {"nodes": ["model.demo.customer"]},
            },
        }
    }
    run_results = {
        "metadata": {"invocation_id": "test-only-123"},
        "results": [
            {"unique_id": "test.demo.customer_not_null", "status": "pass"},
            {"unique_id": "test.demo.customer_unique", "status": "pass"},
        ],
    }

    models, invocation_id, statuses = runner.successful_models(manifest, run_results)

    assert models == [
        {
            "unique_id": "model.demo.customer",
            "relation_name": "`main`.`demo`.`customer`",
        }
    ]
    assert invocation_id == "test-only-123"
    assert statuses == {"pass": 2}
