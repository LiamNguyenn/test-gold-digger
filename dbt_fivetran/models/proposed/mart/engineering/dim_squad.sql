with all_squads as (
    select squad
    from {{ ref("int_cleansed_squad_names") }}
    union distinct
    select distinct squad from {{ ref("stg_eh_product__squad_board_ownership") }}
    union distinct
    select distinct squad from {{ ref("stg_eh_engineering__squad_members") }}
)

select
    {{ dbt_utils.generate_surrogate_key(['squad']) }} as dim_squad_sk,
    squad                                                                              as squad_name
from all_squads
where
    squad is not NULL
    and squad != ''
