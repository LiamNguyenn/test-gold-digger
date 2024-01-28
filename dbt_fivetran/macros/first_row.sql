{% macro first_row(source_schema, source_table, grouping, order_by_date) %}

(
    select
        *
    from
        {{source(source_schema, source_table)}}
    where
        id in (
        select
            FIRST_VALUE(id) over(partition by {{ grouping }} order by {{ order_by_date }} asc rows between unbounded preceding and unbounded following)
        from
            {{source(source_schema, source_table)}}
        where
            not _fivetran_deleted
        )
)

{% endmacro %}