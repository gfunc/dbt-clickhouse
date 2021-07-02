{% macro incremental_upsert(tmp_relation, target_relation, unique_key=none, statement_name='main') %}
  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
  {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
  {%- set distributed = config.get('distributed') -%}

  {%- if unique_key is not none -%}
    alter table 
    {%- if distributed -%}
      {%- set target_local_identifier=distributed_local_table_name(target_relation) -%}
      {%- set target_local_relation = api.Relation.create(identifier=target_local_identifier,
									                                          schema=schema,
									                                          database=database,
									                                          type='table') -%}
      {{ target_local_relation }} 
    {%- else -%}
      {{ target_relation }} 
    {%- endif -%}
    {{ on_cluster_clause(label="on cluster") }}
    delete where ({{ unique_key }}) in (
      select ({{ unique_key }})
      from {{ tmp_relation }}
    )
  {%- endif %}

  insert into {{ target_relation }} ({{ dest_cols_csv }})
    select {{ dest_cols_csv }}
    from {{ tmp_relation }}
{%- endmacro %}