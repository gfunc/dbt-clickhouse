{% macro run_hooks(hooks, inside_transaction=True) %}
  {% for hook in hooks | selectattr('transaction', 'equalto', inside_transaction)  %}
    {% if not inside_transaction and loop.first %}
      {% call statement(auto_begin=inside_transaction) %}
        commit;
      {% endcall %}
    {% endif %}
    {% set rendered = render(hook.get('sql')) | trim %}
     {% if (rendered | length) > 0 %}
      {% set sqls = rendered.split(';') %}
      {% for sql in sqls %}
        {% call statement(auto_begin=inside_transaction) %}
          {{ sql }}
        {% endcall %}
      {% endfor %}
    {% endif %}
  {% endfor %}
{% endmacro %}
