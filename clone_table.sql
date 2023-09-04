{#
clone_table

Clones a table from production to dev. Can be
used to create or refresh a table clone.

example:
    dbt run-operation clone_table --args '{schema: <insert>, table: <insert>, is_transient: <true/false>}'

note: create or replace is atomic -> 1 transaction
#}

{% macro clone_table(schema, table, is_transient) %}

    {% if is_transient == 'true' %}
        {% set clone_sql -%}
            create or replace transient table to_db.{{ schema }}.{{ table }} clone from_db.{{ schema }}.{{ table }}
        {%- endset %}
    {% else %}
        {% set clone_sql -%}
            create or replace table to_db.{{ schema }}.{{ table }} clone from_db.{{ schema }}.{{ table }}
        {%- endset %}
    {% endif %}

    {{ log("start: clone_table {0}.{1} to to_db".format(schema, table)) }}

    {% do run_query(clone_sql) %}

    {{ log("finish: clone_table {0}.{1} to to_db".format(schema, table)) }}

    {% set grant_to_role_sql -%}
        grant select on to_db.{{ schema }}.{{ table }} to role to_role
    {%- endset %}

    {% do run_query(grant_to_role_sql) %}

    {{ log("finish: granting select on to_db.{0}.{1} clone to to_role".format(schema, table)) }}

    {% set grant_to_db_sql -%}
        grant select on to_db.{{ schema }}.{{ table }} to role to_role
    {%- endset %}

    {% do run_query(grant_to_db_sql) %}

    {{ log("finish: granting select on to_db.{0}.{1} clone to to_role".format(schema, table)) }}

{% endmacro %}
