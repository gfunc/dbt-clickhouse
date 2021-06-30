{% materialization table, adapter='clickhouse' %}
    {%- set identifier = model['alias'] -%}
    {%- set tmp_identifier = model['name'] + '__dbt_tmp' -%}
    {%- set backup_identifier = model['name'] + '__dbt_backup' -%}

    {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
    {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}
    {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema,
                                                      database=database,
                                                      type='table') -%}
    {%- set distributed = config.get('distributed') -%}}

  /*
      See ../view/view.sql for more information about this relation.
  */
    {%- set backup_relation_type = 'table' if old_relation is none else old_relation.type -%}
    {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema,
                                                database=database,
                                                type=backup_relation_type) -%}

    {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
    {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}


    -- drop the temp relations if they exists for some reason
    {{ adapter.drop_relation(intermediate_relation) }}
    {{ adapter.drop_relation(backup_relation) }}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}


    {% if distributed %}
    -- distributed engine model
        {%- set target_local_identifier = model['name'] + '_local' -%}
        {%- set target_local_relation = api.Relation.create(identifier=target_local_identifier,
                                                      schema=schema,
                                                      database=database,
                                                      type='table') -%}
        {%- set old_local_relation = adapter.get_relation(database=database, schema=schema, identifier=target_local_identifier) -%}
        
        {%- set backup_local_identifier = model['name'] + '_local__dbt_backup' -%}
        {%- set backup_local_relation_type = 'table' if old_local_relation is none else old_local_relation.type -%}
        {%- set backup_local_relation = api.Relation.create(identifier=backup_local_identifier,
                                                schema=schema,
                                                database=database,
                                                type=backup_local_relation_type) -%}
        {{ adapter.drop_relation(backup_local_relation) }}

        {% do run_query(create_table_as(False, intermediate_relation, sql)) %}
        -- cleanup
        {% if old_local_relation is not none %}
            {{ adapter.rename_relation(target_local_relation, backup_local_relation) }}
        {% endif %}
        {% if old_relation is not none %}
            {{ adapter.rename_relation(target_relation, backup_relation) }}
        {% endif %}

        -- create local relation
        {% do run_query(create_table_as_table(False, target_local_relation, intermediate_relation)) %}

        -- create distributed relation        
        {% do run_query(create_distributed_table(target_relation, target_local_relation)) %}

        -- insert data into distributed relation
        {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
        {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
        {% call statement("main") %}
            insert into {{ target_relation.include(database=False) }} ({{ dest_cols_csv }})
            select {{ dest_cols_csv }}
            from {{ intermediate_relation.include(database=False) }};
        {% endcall %}

        -- drop intermediate relation
        {{ adapter.drop_relation(intermediate_relation) }}
    {% else %}
        -- build model
        {% call statement("main") %}
            {{ create_table_as(False, intermediate_relation, sql) }}
        {% endcall %}
        -- cleanup
        {% if old_relation is not none %}
            {{ adapter.rename_relation(target_relation, backup_relation) }}
        {% endif %}

        {{ adapter.rename_relation(intermediate_relation, target_relation) }}
    {%- endif %}


    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {% do persist_docs(target_relation, model) %}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    -- finally, drop the existing/backup relation after the commit
    {{ drop_relation_if_exists(backup_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
