{% macro run_dqx_governance_gate(results) %}
  {% set enabled = var('dqx_gate_enabled', false) | as_bool %}
  {% if not execute or not enabled or flags.WHICH not in ['test', 'build'] %}
    {{ return('select 1') }}
  {% endif %}

  {% set native_failures = [] %}
  {% set models = [] %}
  {% set model_ids = [] %}

  {% for result in results %}
    {% set status = result.status | string %}
    {% if status in ['error', 'fail'] %}
      {% do native_failures.append(result.node.unique_id) %}
    {% endif %}

    {% if result.node.resource_type == 'model' and status == 'success' %}
      {% if result.node.unique_id not in model_ids and result.node.relation_name %}
        {% do model_ids.append(result.node.unique_id) %}
        {% do models.append({
          'unique_id': result.node.unique_id,
          'relation_name': result.node.relation_name
        }) %}
      {% endif %}
    {% elif result.node.resource_type == 'test' and status in ['pass', 'warn'] %}
      {% for dependency_id in result.node.depends_on.nodes %}
        {% set dependency = graph.nodes.get(dependency_id) %}
        {% if dependency and dependency.resource_type == 'model' %}
          {% if dependency.unique_id not in model_ids and dependency.relation_name %}
            {% do model_ids.append(dependency.unique_id) %}
            {% do models.append({
              'unique_id': dependency.unique_id,
              'relation_name': dependency.relation_name
            }) %}
          {% endif %}
        {% endif %}
      {% endfor %}
    {% endif %}
  {% endfor %}

  {% if native_failures | length > 0 %}
    {% do log(
      'Enterprise DQX gate skipped because native dbt tests failed: '
      ~ (native_failures | join(', ')),
      info=true
    ) %}
    {{ return('select 1') }}
  {% endif %}

  {% if models | length == 0 %}
    {% do log('Enterprise DQX gate: no tested models were resolved', info=true) %}
    {{ return('select 1') }}
  {% endif %}

  {% set gate_catalog = env_var('DBT_DQX_CATALOG', target.database) %}
  {% set gate_schema = env_var('DBT_DQX_SCHEMA', target.schema) %}
  {% set function_name = gate_catalog ~ '.' ~ gate_schema ~ '.run_dqx_gate' %}
  {% set models_json = tojson(models) | replace("'", "''") %}

  {% do log('', info=true) %}
  {% do log('ENTERPRISE DQX GATE', info=true) %}
  {% do log(
    '  submitting ' ~ (models | length) ~ ' tested model(s) from dbt invocation '
    ~ invocation_id,
    info=true
  ) %}

  {% set orchestration_sql %}
    select {{ function_name }}(
      '{{ models_json }}',
      '{{ invocation_id }}',
      '{{ gate_catalog }}',
      '{{ gate_schema }}'
    ) as orchestration_result
  {% endset %}
  {% do run_query(orchestration_sql) %}

  {% set escaped_invocation = invocation_id | replace("'", "''") %}
  {% set gate_rows_sql %}
    select
      dbt_unique_id,
      status,
      input_row_count
    from {{ gate_catalog }}.{{ gate_schema }}.governance_gate_runs
    where invocation_id = '{{ escaped_invocation }}'
    order by dbt_unique_id
  {% endset %}
  {% set gate_rows = run_query(gate_rows_sql) %}

  {% if gate_rows | length == 0 %}
    {{ return(
      "select raise_error('The DQX job finished without recording a governance "
      ~ "result for invocation " ~ invocation_id ~ "')"
    ) }}
  {% endif %}

  {% set controls_sql %}
    with latest as (
      select
        run_name,
        metric_value,
        row_number() over (
          partition by run_name
          order by run_time desc
        ) as row_number
      from {{ gate_catalog }}.{{ gate_schema }}.dqx_metrics
      where run_name like '%:{{ escaped_invocation }}'
        and metric_name = 'check_metrics'
    )
    select
      regexp_extract(run_name, '^(.*):[^:]+$', 1) as dbt_unique_id,
      control.check_name,
      cast(control.error_count as bigint) as error_count,
      cast(control.warning_count as bigint) as warning_count
    from latest
    lateral view explode(
      from_json(
        metric_value,
        'array<struct<check_name:string,error_count:bigint,warning_count:bigint>>'
      )
    ) exploded as control
    where row_number = 1
    order by dbt_unique_id, control.check_name
  {% endset %}
  {% set controls = run_query(controls_sql) %}

  {% set passed = [] %}
  {% set warned = [] %}
  {% set failed = [] %}
  {% for control in controls %}
    {% set unique_id = control[0] %}
    {% set model_name = unique_id.split('.')[-1] %}
    {% set control_name = control[1] %}
    {% set error_count = control[2] | int %}
    {% set warning_count = control[3] | int %}
    {% if error_count > 0 %}
      {% set status = 'FAIL ' ~ error_count %}
      {% do failed.append(control_name ~ ' on ' ~ model_name) %}
    {% elif warning_count > 0 %}
      {% set status = 'WARN ' ~ warning_count %}
      {% do warned.append(control_name ~ ' on ' ~ model_name) %}
    {% else %}
      {% set status = 'PASS' %}
      {% do passed.append(control_name ~ ' on ' ~ model_name) %}
    {% endif %}
    {% do log(
      '  ' ~ loop.index ~ ' of ' ~ (controls | length)
      ~ ' ' ~ status ~ ' dqx_' ~ control_name ~ '_on_' ~ model_name
      ~ ' [' ~ status ~ ']',
      info=true
    ) %}
  {% endfor %}

  {% set skipped = [] %}
  {% for gate_row in gate_rows %}
    {% if gate_row[1] == 'SKIP' %}
      {% do skipped.append(gate_row[0]) %}
      {% do log('  SKIP ' ~ gate_row[0] ~ ': no governed controls', info=true) %}
    {% endif %}
  {% endfor %}

  {% do log(
    '  Done. PASS=' ~ (passed | length)
    ~ ' WARN=' ~ (warned | length)
    ~ ' ERROR=' ~ (failed | length)
    ~ ' SKIP=' ~ (skipped | length)
    ~ ' TOTAL=' ~ (controls | length),
    info=true
  ) %}

  {% for warning in warned %}
    {% do log('  Warning in governance control ' ~ warning, info=true) %}
  {% endfor %}

  {% if failed | length > 0 %}
    {% set failure_message = (
      'Enterprise DQX gate blocked this dbt invocation. Failed controls: '
      ~ (failed | join(', '))
    ) | replace("'", "''") %}
    {{ return("select raise_error('" ~ failure_message ~ "')") }}
  {% endif %}

  {{ return('select 1') }}
{% endmacro %}
