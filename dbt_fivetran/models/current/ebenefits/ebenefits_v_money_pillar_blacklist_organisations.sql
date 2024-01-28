{{ config(materialized='view', alias='_v_money_pillar_blacklist_organisations') }}

-- The blacklisted orgs are currently stored in ONE target group condition field: tg.conditions 
        select distinct o.id as organisation_id                        
        from {{ source('feature_flag_public', 'features') }} f
        join {{ source('feature_flag_public', 'features_target_groups') }} ftg on ftg.feature_id = f.id 
        join {{ source('feature_flag_public', 'target_groups') }} tg on ftg.target_group_id = tg.id 
        join {{ source('postgres_public', 'organisations') }} o on tg.target_type = 'organisation' and tg.conditions like '%:' || o.id || '.%' 
        where f.code = 'eben_money_pillar_black_list'
            and not f._fivetran_deleted
            and not ftg._fivetran_deleted
            and not tg._fivetran_deleted
            and not o._fivetran_deleted        
 