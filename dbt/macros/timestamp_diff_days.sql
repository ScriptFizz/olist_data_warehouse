{% macro timestamp_diff_days(end_timestamp, start_timestamp) -%}
    {{ return(
        adapter.dispatch(
            'timestamp_diff_days',
            'olist_warehouse'
        )(end_timestamp, start_timestamp)
    ) }}
{%- endmacro %}

{% macro postgres__timestamp_diff_days(end_timestamp, start_timestamp) -%}
    cast(
        trunc(
            extract(
                epoch from (
                    {{ end_timestamp }} - {{ start_timestamp }}
                )
            ) / 86400.0
        )
        as bigint
    )
{%- endmacro %}


{% macro bigquery__timestamp_diff_days(end_timestamp, start_timestamp) -%}
    timestamp_diff(
        {{ end_timestamp }},
        {{ start_timestamp }},
        day
    )
{%- endmacro %}


{% macro default__timestamp_diff_days(end_timestamp, start_timestamp) -%}
{{ exceptions.raise_compiler_error(
    "timestamp_diff_days is not implemented for adapter "
    ~ target.type
) }}
{%- endmacro %}