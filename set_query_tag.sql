{#
set_query_tag

Snowflake looks for this macro when determining
how to set the query tag and uses it to override
the default behavior.

The precedence of which tag to use is as follows:

1. tag added in model file
2. tag added in yml file
3. no tag -> use default (path to model)
#}

{% macro set_query_tag() -%}

    {% set run_user = target.user %}

    {% set has_yml_query_tag = get_current_query_tag() %}
    {% set has_model_query_tag = config.get('query_tag') %} 

    {% if has_model_query_tag %}
        {% set query_tag = has_model_query_tag %}
    {% elif has_yml_query_tag %}
        {% set query_tag = has_yml_query_tag %}
    {% else %}
        {% set query_tag = model.path %}
    {% endif %}

    {% set query_tag_w_metadata = query_tag ~ '|' ~ run_user %}

    {{ log("Setting query_tag to '{0}'".format(query_tag_w_metadata)) }}

    {% do run_query("alter session set query_tag = '{}'".format(query_tag_w_metadata)) %}

    {{ return(none) }}

{% endmacro %}
