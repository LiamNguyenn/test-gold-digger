with source as (
    select *

    from {{ source("heroshop_db_public", "products") }}
),

transformed as (
    select
        id::varchar                   as id, -- noqa: RF04
        supplier_id::varchar          as supplier_id,
        product_code::varchar         as product_code,
        name::varchar                 as name, -- noqa: RF04
        title::varchar                as title,
        image_url::varchar            as image_url,
        email::varchar                as email,
        created_at::timestamp         as created_at,
        updated_at::timestamp         as updated_at,
        logo_url::varchar             as logo_url,
        usage::varchar                as usage, -- noqa: RF04
        participant::varchar          as participant,
        product_type::int             as product_type,
        disabled::boolean             as disabled,
        saving_category_id::varchar   as saving_category_id,
        product_category_id::varchar  as product_category_id,
        transaction_fee::float        as transaction_fee,
        _fivetran_synced::timestamp   as fivetran_synced,
        _fivetran_deleted::boolean    as fivetran_deleted,
        reviewed::boolean             as reviewed,
        handle::varchar               as handle,
        slug::varchar                 as slug,
        instapay_fee::float           as instapay_fee,
        local_id::int                 as local_id,
        storefront_image_url::varchar as storefront_image_url,
        image::varchar                as image,
        storefront_image::varchar     as storefront_image,
        logo::varchar                 as logo,
        how_it_works::varchar         as how_it_works,
        giftpay_balance::float        as giftpay_balance,
        terms_and_conditions::varchar as terms_and_conditions,
        description::varchar          as description,
        is_special::boolean           as is_special,
        country::varchar              as country

    from source
)

select * from transformed
where not fivetran_deleted
