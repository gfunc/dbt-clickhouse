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
  	{%- if old_relation is not none -%}
			{{ adapter.rename_relation(target_relation, backup_relation) }}
		{%- endif -%}
    {%- set target_local_identifier=distributed_local_table_name(target_relation) -%}
    {%- set target_local_relation = api.Relation.create(identifier=target_local_identifier,
	  								  schema=schema,
	  								  database=database,
	  								  type='table') -%}
    {% call statement("main") %}
      {{ create_distributed_table(target_relation, intermediate_relation, target_local_relation, sql) }}
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
