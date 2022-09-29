{% macro grant_select_on_table_hook(target_relation, role_user_list, distributed=False, cluster='') %}
  {% set grants = [] %}
  {%- if cluster == '' -%}
    {% set cluster = target.cluster %}
  {%- endif -%}
  {%- if target.name == 'prod' -%}
    {%- for role in role_user_list -%}
      {% do grants.append(table_grant(target_relation, role, cluster)) %}
      {%- if distributed-%}
        {%- if cluster is none -%}
          {% do exceptions.raise_compiler_error("Invalid call of macro grant_select_on_table_hook, `distributed=True` while `cluster` is not specified in target") %}
        {%- else -%}
          {%- do grants.append(table_local_grant(target_relation, role, cluster)) -%}
        {%- endif -%}
      {%- endif -%}
    {%- endfor -%}
  {% endif %}
  {{ grants|join(';')|replace('\n','') }}
{% endmacro %}

{% macro table_grant(target_relation, role, cluster) %}
  grant {{ on_cluster_clause(label="on cluster") }} select on {{ target_relation }} to {{ role }}
{% endmacro %}

{% macro table_local_grant(target_relation, role, cluster) %}
  {%- set target_local_relation_identifier=distributed_local_table_name(target_relation) -%}
  grant {{ on_cluster_clause(label="on cluster") }} select on {{ target_relation.schema }}.{{ target_local_relation_identifier }} to {{ role }}
{% endmacro %}

{% macro flush_distributed_table(relation) %}
  SYSTEM FLUSH DISTRIBUTED {{ relation.schema }}.{{ relation.identifier }}
{% endmacro %}