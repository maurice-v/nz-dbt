{% macro netezza__safe_cast(field, type) %}
    {#-- Netezza requires VARCHAR to have a size. If casting to character varying without size, use default --#}
    {%- set type_lower = type | lower | trim -%}
    {%- if type_lower == 'character varying' or type_lower == 'varchar' -%}
        cast({{ field }} as varchar(256))
    {%- elif type_lower.startswith('character varying') and '(' not in type_lower -%}
        cast({{ field }} as varchar(256))
    {%- else -%}
        cast({{ field }} as {{ type }})
    {%- endif -%}
{% endmacro %}
