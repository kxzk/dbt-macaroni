{#
drop_personal_schema

This macro will drop your personal dev schema.
This is the `schema` value defined in your
profiles.yml.

When you run another model the schema will
automatically be re-created for you by DBT.
#}

{% macro drop_personal_schema() %}
    {% set query -%}
        drop schema {{ target.schema }} cascade
    {%- endset %}

    {{ log("start dropping schema: " ~ target.schema) }}

    {% do run_query(query) %}

    {{ log("finish dropping schema: " ~ target.schema) }}
{% endmacro %}
