{% macro incremental_upsert(tmp_relation, target_relation, unique_key=none, statement_name='main') %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    {%- set distributed = config.get('distributed') -%}

    {%- if unique_key is not none -%}
        {%- set partition_by=config.get('partition_by') -%}
        {%- set table_name -%}
            {%- if distributed -%}
                {%- set target_local_identifier=distributed_local_table_name(target_relation) -%}
                {%- set target_local_relation = api.Relation.create(identifier=target_local_identifier,
                                                                                        schema=schema,
                                                                                        database=database,
                                                                                        type='table') -%}
                {{ target_local_relation }} {{ on_cluster_clause(label="on cluster") }}
            {%- else -%}
                {{ target_relation }}
            {%- endif -%}
        {%- endset -%}
        {%- set query -%}
            {%- if partition_by is not none and partition_by != '' -%}
                {%- set queries = ['SET replication_alter_partitions_sync=2'] -%}
                {%- set select_query -%}
                    WITH parts AS (SELECT DISTINCT {{ config.get('partition_by') }} AS _p
                        FROM {{ tmp_relation }}
                        ORDER BY {{ config.get('partition_by') }})
                    SELECT _p FROM parts
                    UNION ALL
                    SELECT min(_p) FROM parts
                {%- endset -%}
                {%- set parition_list = run_query(select_query) -%}
                {%- set partition_key -%}
                    {%- if '(' in  partition_by -%}
                        {{ partition_by.split(',')[0].rsplit('(', 1)[1] | replace(')','') | trim(' ') }}
                    {%- else -%}
                        {{ partition_by }}
                    {% endif %}
                {%- endset -%}
                {%- for part in parition_list.columns[0].values() -%}
                    {%- set query-%}
                    OPTIMIZE TABLE {{ table_name }} PARTITION {{part}} FINAL DEDUPLICATE BY {{ unique_key }}, {{ partition_key }}
                    {%- endset -%}
                    {%- do queries.append(query) -%}
                {%- endfor -%}
                {{ queries|join(';')|replace('\n','') }}
            {%- else -%}
                SET replication_alter_partitions_sync=2;OPTIMIZE TABLE {{ table_name }} FINAL DEDUPLICATE BY {{ unique_key }};
                OPTIMIZE TABLE {{ table_name }} FINAL DEDUPLICATE BY {{ unique_key }}
            {%- endif -%}
        {%- endset -%}
        {%- do post_hooks.append({'sql': query, 'transaction': True}) -%}
    {%- endif %}
    INSERT INTO {{ target_relation }} ({{ dest_cols_csv }})
    SELECT {{ dest_cols_csv }}
    FROM {{ tmp_relation }}
{%- endmacro %}