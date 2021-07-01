{% materialization incremental, adapter='clickhouse' -%}

  {% set unique_key = config.get('unique_key') %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}
  {% set distributed = config.get('distributed') %}}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {% if distributed %}
    {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
    {% elif existing_relation.is_view or should_full_refresh() %}

    {% endif %}
  {% endif %}

  {% if existing_relation is none %}
    {% if distributed %}
      {% set build_sql = create_distributed_table(target_relation, tmp_relation, sql) %}
      {% do to_drop.append(tmp_relation) %}
    {% else %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
    {% endif %}
  {% elif existing_relation.is_view or should_full_refresh() %}
    {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
    {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
    {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
    {% do adapter.drop_relation(backup_relation) %}
    {% do adapter.rename_relation(target_relation, backup_relation) %}
    {% if distributed %}
      {% set build_sql = create_distributed_table(target_relation, tmp_relation, sql) %}
      {% do to_drop.append(tmp_relation) %}
    {% else %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
    {% endif %}
    {% do to_drop.append(backup_relation) %}
  {% else %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql = incremental_upsert(tmp_relation, target_relation, unique_key=unique_key) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
