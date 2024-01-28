with source as (
    select *

    from {{ source("heroshop_db_public", "product_categories") }}
),

transformed as (
    select
        id::varchar                 as id, -- noqa: RF04
        country::varchar            as country,
        code::varchar               as code,
        logo_url::varchar           as logo_url,
        image_url::varchar          as image_url,
        description::varchar        as description,
        created_at::timestamp       as created_at,
        title::varchar              as title,
        display_on_ui::boolean      as display_on_ui,
        updated_at::timestamp       as updated_at,
        name::varchar               as name, -- noqa: RF04
        disabled::boolean           as disabled,
        _fivetran_synced::timestamp as fivetran_synced,
        _fivetran_deleted::boolean  as fivetran_deleted
    from source
)

select * from transformed
where not fivetran_deleted
