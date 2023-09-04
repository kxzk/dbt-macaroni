{#
<insert_macro_name>
<insert_description>
usage:
    {{ insert_macro_name(some_value) }}
    (sql) -> <insert sql generated>
    {{ insert_macro_name(some_value, some_value) }}
    (sql) -> <insert sql generated>
    {{ insert_macro_name(arg_datatype=some_value, arg2_dataype=some_value) }}
    (sql) -> <insert sql generated>
#}

{% macro insert_macro_name(arg_datatype=none, arg2_datatype=none) -%}

    {% if arg_datatype is none %}
        {{ exceptions.raise_compiler_error("arg_datatype is required") }}
    {% endif %}

    {%- if arg2_datatype is not none -%}
    {%- else -%}
    {%- endif -%}

{%- endmacro %}
