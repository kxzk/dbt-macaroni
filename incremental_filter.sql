{#
incremental_filter

This macro is a utility to help cut down on
repetitive code for incremental models. Also,
it should help to standardize our approach.

usage:
    {{
        config(
            materialized='incremental'
        )
    }}
    
    select event_id, event_date
    from {{ source('schema', 'table') }}
    where 1=1
        {{ incremental_filter('event_date') }}

basic example:
    {{ incremental_filter('event_date') }}
    (sql) -> and event_date > (select max(event_date) from {{ this }})
    {{ incremental_filter('event_date', -3) }}
    (sql) -> and event_date >= current_date() + -3

backfill example:
    dbt run --models my_model --vars '{backfill_start_date: 2022-11-01}'
    (sql) -> and event_date >= '2022-11-01'
    dbt run --models my_model --vars '{backfill_start_date: 2022-11-01, backfill_end_date: 2022-12-01}'
    (sql) -> and event_date >= '2022-11-01' and event_date <= '2022-12-01'
    dbt run --models my_model --vars '{backfill_start_int: -8, backfill_end_int: -5}'
    (sql) -> and event_date >= current_date() + -8 and event_date <= current_date() + -5
#}

{%- macro incremental_filter(column_str=none, lookback_int=none) -%}

    {%- if is_incremental() %}

        {%- if column_str is none -%}
            {{ exceptions.raise_compiler_error("Invalid must provide a `column_str` value.") }}
        {%- endif -%}

        {%- if var('backfill_start_date', default=false) -%}
            and {{ column_str }} >= '{{ var("backfill_start_date") }}'
            {%- if var('backfill_end_date', default=false) -%}
                and {{ column_str }} <= '{{ var("backfill_end_date") }}'
            {%- endif -%}

        {%- elif var('backfill_start_int', default=false) and var('backfill_end_int', default=false) -%}
            and {{ column_str }} >= current_date() + {{ var('backfill_start_int') }}
            and {{ column_str }} <= current_date() + {{ var('backfill_end_int') }}
        {%- else -%}

            {%- if lookback_int is not none -%}
                and {{ column_str }} >= current_date() + {{ lookback_int }}
            {%- else -%}
                {%- set sql -%}select max({{ column_str }}) from {{ this }}{%- endset -%}
                {%- if execute -%}
                    and {{ column_str }} > '{{ run_query(sql).columns[0].values()[0] }}'
                {%- endif -%}
            {%- endif -%}

        {%- endif -%}

    {% endif -%}

{%- endmacro -%}
