{#
dev_limit

A macro to limit the number of rows
processed during development and staging.
In production, the limit is removed.

By default, the macro runs using
the bernoulli sampling method.
However, the system method is faster,
but it cannot be used on:
    - the result of a join
    - a view
    - sample size must be a decimal percentage (0.1, .2, etc.)

Thus, `is_system_bool=true` will invoke this behavior.


Slightly confusing, so refer to the link below:

https://docs.snowflake.com/en/sql-reference/constructs/sample.html#examples


usage:
    select id, name
    from {{ source('schema', 'table') }}
    {{ dev_limit(1000) }}
    (sql) -> sample bernoulli (1000 rows)
    {{ dev_limit(.01, is_system_bool=true) }}
    (sql) -> sample system (.01)

    dbt run --models my_model --vars '{skip_limit: true}'
    (sql) -> nothing, limit is skipped
#}

{%- macro dev_limit(sample_size=none, is_system_bool=false) -%}

    {%- if sample_size is none -%}
        {{ exceptions.raise_compiler_error("Invalid must provide a `sample_size` value.") }}
    {%- endif -%}

    {%- if var('skip_limit', default=false) -%}
    {%- else -%}
        {%- if target.name == 'prod' -%}
        {%- else -%}
            {%- if is_system_bool != false %}
                sample system ({{ sample_size }})
            {% else %}
                sample bernoulli ({{ sample_size }} rows)
            {% endif -%}
        {%- endif -%}
    {%- endif -%}

{%- endmacro -%}
