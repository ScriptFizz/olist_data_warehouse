{% macro greatest_timestamp(expressions) -%}
    {{ return(
        adapter.dispatch(
            'greatest_timestamp',
            'olist_warehouse'
        )(expressions)
    ) }}
{%- endmacro %}


{% macro postgres__greatest_timestamp(expressions) -%}
    greatest(
        {% for expression in expressions %}
            {{ expression }}
            {% if not loop.last %}, {% endif %}
        {% endfor %}
    )
{%- endmacro %}


{% macro bigquery__greatest_timestamp(expressions) -%}
    (
        select max(candidate_timestamp)
        from unnest([
            {% for expression in expressions %}
                {{ expression }}
                {% if not loop.last %}, {% endif %}
            {% endfor %}
        ]) as candidate_timestamp
    )
{%- endmacro %}