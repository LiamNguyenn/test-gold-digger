with all_squads as (
    select squad
    from "dev"."intermediate"."int_cleansed_squad_names"
    union distinct
    select distinct squad from "dev"."staging"."stg_eh_product__squad_board_ownership"
    union distinct
    select distinct squad from "dev"."staging"."stg_eh_engineering__squad_members"
)

select
    md5(cast(coalesce(cast(squad as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as dim_squad_sk,
    squad                                                                              as squad_name
from all_squads
where
    squad is not NULL
    and squad != ''