{% macro engine_clause(label) %}
  {%- set engine = config.get('engine', validator=validation.any[basestring]) -%}
  {%- if engine is not none %}
    {{ label }} = {{ engine }}
  {%- else %}
    {{ label }} = MergeTree()
  {%- endif %}
{%- endmacro -%}

{% macro partition_cols(label) %}
  {%- set cols = config.get('partition_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- endif %}
{%- endmacro -%}

{% macro order_cols(label) %}
  {%- set cols = config.get('order_by', validator=validation.any[list, basestring]) -%}
  {%- if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}
    {{ label }} (
    {%- for item in cols -%}
      {{ item }}
      {%- if not loop.last -%},{%- endif -%}
    {%- endfor -%}
    )
  {%- else %}
    {{ label }} (tuple())
  {%- endif %}
{%- endmacro -%}

{% macro on_cluster_clause(label) %}
  {%- set on_cluster = target.cluster -%}
  {%- if on_cluster is none -%}
    {%- set on_cluster = adapter.get_clickhouse_cluster_name() -%}
  {%- endif -%}
  {%- if on_cluster is not none -%}
    {{ label }} {{ on_cluster }}
  {%- endif -%}
{%- endmacro -%}

{% macro clickhouse__create_table_as(temporary, relation, sql) -%}
  {#-- distributed ddl (on cluster clause) will be executed on each node within the cluster, that is the same sql will be executed multiple times which could be a waste of processing power --#}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {%- set distributed_ddl=on_cluster_clause(label="on cluster") -%}
  
  {%- if temporary or distributed_ddl is none or distributed_ddl == '' -%}
    {{ create_table_as_sql(temporary, relation, sql) }}
  {%- else -%}
    {%- call statement('create_table') -%}
      {{ create_table_as_sql(Fales, relation, sql) }}
    {%- endcall -%}
    {%- set cols = adapter.get_columns_in_relation(relation) -%}
    create table if not exists {{ relation.include(database=False) }}
    {{ on_cluster_clause(label="on cluster") }}
    (
    {%- for n in range(cols|length) -%}
      {{ cols[n].column }} {{ cols[n].raw_type }} {{ '' if n+1 == cols|length else ',\n' }}
    {%- endfor -%}
    ) {{ engine_clause(label="engine") }}
    {{ order_cols(label="order by") }}
    {{ partition_cols(label="partition by") }}
  {%- endif -%}

{%- endmacro -%}

{%- macro clickhouse__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create view {{ relation.include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  as (
    {{ sql }}
  )
{%- endmacro -%}

{% macro clickhouse__list_schemas(database) %}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select name from system.databases
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro clickhouse__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ relation.without_identifier().include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ relation.without_identifier().include(database=False) }} {{ on_cluster_clause(label="on cluster") }}
  {%- endcall -%}
{% endmacro %}

{% macro clickhouse__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      null as db,
      name as name,
      database as schema,
      if(engine not in ('MaterializedView', 'View'), 'table', 'view') as type
    from system.tables as t
    where schema = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro clickhouse__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
      name,
      type,
      position
    from system.columns
    where
      "table" = '{{ relation.identifier }}'
    {% if relation.schema %}
      and database = '{{ relation.schema }}'
    {% endif %}
    order by position
  {% endcall %}
  {% do return(load_result('get_columns_in_relation').table) %}
{% endmacro %}

{% macro clickhouse__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop table if exists {{ relation }} {{ on_cluster_clause(label="on cluster") }}
  {%- endcall %}
{% endmacro %}

{% macro clickhouse__rename_relation(from_relation, to_relation) -%}
  {% call statement('drop_relation') %}
    drop table if exists {{ to_relation }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
  {% call statement('rename_relation') %}
    rename table {{ from_relation }} to {{ to_relation }} {{ on_cluster_clause(label="on cluster") }}
  {% endcall %}
{% endmacro %}

{% macro clickhouse__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}

{% macro clickhouse__make_temp_relation(base_relation, suffix) %}
  {% set tmp_identifier = base_relation.identifier ~ suffix %}
  {% set tmp_relation = base_relation.incorporate(
                              path={"identifier": tmp_identifier, "schema": base_relation.schema}) -%}
  {% do return(tmp_relation) %}
{% endmacro %}


{% macro clickhouse__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{% macro clickhouse__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro clickhouse__get_columns_in_query(select_sql) %}
  {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    limit 0
  {% endcall %}

  {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}

{% macro clickhouse__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    alter table {{ relation }} {{ on_cluster_clause(label="on cluster") }} modify column {{ adapter.quote(column_name) }} {{ new_column_type }} 
  {% endcall %}
{% endmacro %}

{% macro create_table_as_table(temporary, relation, as_relation) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {% if temporary -%}
    create table {{ relation.include(database=False) }}
    ENGINE = Memory
  {%- else %}
    create table {{ relation.include(database=False) }}
    {{ on_cluster_clause(label="on cluster") }}
    {{ engine_clause(label="engine") }}
    {{ order_cols(label="order by") }}
    {{ partition_cols(label="partition by") }}
  {%- endif %}
  as {{ as_relation.include(database=False) }}
{%- endmacro %}

{% macro create_distributed_table_as(relation, local_relation) -%}
  {%- set cluster_name = adapter.get_clickhouse_cluster_name() -%}
  {%- if cluster_name is none -%}
    {% do exceptions.raise_compiler_error("Invalid call of marco `create_distributed_table_as`. `cluster` is not specified in target") %}
  {%- endif -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {{ sql_header if sql_header is not none }}

  create table {{ relation.include(database=False) }}
    {{ on_cluster_clause(label="on cluster") }} as {{ local_relation.include(database=False) }}
    {{ distributed_engine_clause("engine", relation, local_relation) }}
{% endmacro %}

{% macro distributed_engine_clause(label, target_relation, target_local_relation) %}
  {%- set cluster_name = adapter.get_clickhouse_cluster_name() -%}
  {%- if cluster_name is none -%}
    {% do exceptions.raise_compiler_error("Invalid call of marco `distributed_engine_clause`. `cluster` is not specified in target") %}
  {%- endif -%}
  {%- set sharding_key = config.get('sharding_key', default='rand()') -%}
  {{ label }} = Distributed({{ cluster_name }}, {{ target_relation.schema }}, {{ target_local_relation.identifier }}, {{ sharding_key }})
{% endmacro %}

{% macro distributed_local_table_name(target_relation) %}
  {%- set suffix = target.local_suffix -%}
  {{ return (target_relation.identifier ~ '_' ~ suffix) }}
{% endmacro %}

{% macro create_distributed_table(target_relation, target_local_relation, tmp_relation, sql) -%}
  {# distributed engine model #}
	{%- set old_relation = adapter.get_relation(database=target_relation.database, schema=target_relation.schema, identifier=target_relation.identifier) -%}
	{%- set old_local_relation = adapter.get_relation(database=target_local_relation.database, schema=target_local_relation.schema, identifier=target_local_relation.identifier) -%}
	
  {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}

  {%- set backup_local_relation_type = 'table' if old_local_relation is none else old_local_relation.type -%}
  {%- set backup_local_relation = make_backup_relation(target_local_relation, backup_local_relation_type) -%}

	{%- do adapter.drop_relation(backup_local_relation) -%}
	{%- do run_query(create_table_as(False, tmp_relation, sql)) -%}
	
  {# create local relation #}
  {%- if old_relation is not none -%}
    {%- set backup_relation = make_backup_relation(target_relation, backup_relation_type) -%}
		{%- do adapter.rename_relation(target_relation, backup_relation) -%}
	{%- endif -%}
	{%- if old_local_relation is not none -%}
		{%- do adapter.rename_relation(target_local_relation, backup_local_relation) -%}
	{%- endif -%}
	{%- do run_query(create_table_as_table(False, target_local_relation, tmp_relation)) -%}
	
  {# create distributed relation #}
	{%- do run_query(create_distributed_table_as(target_relation, target_local_relation)) -%}
	
  {# insert data into distributed relation #}
	{%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
	{%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
	insert into {{ target_relation.include(database=False) }} ({{ dest_cols_csv }})
	select {{ dest_cols_csv }}
	from {{ tmp_relation }}

{% endmacro %}

{% macro make_backup_relation(base_relation, relation_type=None, suffix='__dbt_backup') %}
  {% set backup_identifier = base_relation.identifier ~ suffix %}
  {% set back_relation_type = 'table' %}
  {% if relation_type is none %}
    {% if base_relation.type is not none %}
      {% set back_relation_type = base_relation.type %}
    {% endif %}
  {% else%}
    {% set back_relation_type = relation_type %}
  {% endif %}
	{% set backup_relation = api.Relation.create(identifier=backup_identifier,
												schema=base_relation.schema,
												database=base_relation.database,
												type=back_relation_type) %}
  {% do return(backup_relation) %}
{% endmacro %}

{# create temp table without on cluster clause to avoid waste of processing power #}
{% macro create_table_as_sql(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  {%- if temporary -%}
    create table {{ relation.include(database=False) }}
    ENGINE = Memory
  {%- else -%}
    create table {{ relation.include(database=False) }}
    {{ engine_clause(label="engine") }}
    {{ order_cols(label="order by") }}
    {{ partition_cols(label="partition by") }}
  {%- endif %}
  as (
    {{ sql }}
  )
{%- endmacro -%}