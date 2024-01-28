{{ config(alias='zoom_info', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'zoom_info') }}

),

renamed as (

    select
        record_id::bigint                                 as record_id,
        _id::bigint                                       as _id,
        company_name::varchar                             as company_name,
        abn::bigint                                       as abn,
        country::varchar                                  as country,
        match_status::varchar                             as match_status,
        zoom_info_company_id::bigint                      as zoom_info_company_id,
        website::varchar                                  as website,
        founded_year::bigint                              as founded_year,
        company_hq_phone::varchar                         as company_hq_phone,
        revenue_in_000_s_usd_::bigint                     as revenue_in_000_s_usd_,
        revenue_range_in_usd_::varchar                    as revenue_range_in_usd_,
        est_hr_department_budget_in_000_s_usd_::bigint    as est_hr_department_budget_in_000_s_usd_,
        employees::bigint                                 as employees,
        employee_range::varchar                           as employee_range,
        past_1_year_employee_growth_rate::numeric         as past_1_year_employee_growth_rate,
        past_2_year_employee_growth_rate::numeric         as past_2_year_employee_growth_rate,
        sic_code_1::bigint                                as sic_code_1,
        sic_code_2::bigint                                as sic_code_2,
        sic_codes::varchar                                as sic_codes,
        naics_code_1::bigint                              as naics_code_1,
        naics_code_2::bigint                              as naics_code_2,
        naics_codes::varchar                              as naics_codes,
        primary_industry::varchar                         as primary_industry,
        primary_sub_industry::varchar                     as primary_sub_industry,
        all_industries::varchar                           as all_industries,
        all_sub_industries::varchar                       as all_sub_industries,
        industry_hierarchical_category::varchar           as industry_hierarchical_category,
        secondary_industry_hierarchical_category::varchar as secondary_industry_hierarchical_category,
        alexa_rank::bigint                                as alexa_rank,
        zoom_info_company_profile_url::varchar            as zoom_info_company_profile_url,
        linked_in_company_profile_url::varchar            as linked_in_company_profile_url,
        facebook_company_profile_url::varchar             as facebook_company_profile_url,
        twitter_company_profile_url::varchar              as twitter_company_profile_url,
        ownership_type::varchar                           as ownership_type,
        business_model::varchar                           as business_model,
        certified_active_company::varchar                 as certified_active_company,
        certification_date::varchar                       as certification_date,
        defunct_company::varchar                          as defunct_company,
        total_funding_amount_in_000_s_usd_::bigint        as total_funding_amount_in_000_s_usd_,
        recent_funding_amount_in_000_s_usd_::bigint       as recent_funding_amount_in_000_s_usd_,
        recent_funding_round::varchar                     as recent_funding_round,
        recent_funding_date::date                         as recent_funding_date,
        recent_investors::varchar                         as recent_investors,
        all_investors::varchar                            as all_investors,
        company_street_address::varchar                   as company_street_address,
        company_city::varchar                             as company_city,
        company_state::varchar                            as company_state,
        company_zip_code::varchar                         as company_zip_code,
        company_country::varchar                          as company_country,
        full_address::varchar                             as full_address,
        number_of_locations::bigint                       as number_of_locations,
        company_is_acquired::varchar                      as company_is_acquired,
        company_id_ultimate_parent_::bigint               as company_id_ultimate_parent_,
        entity_name_ultimate_parent_::varchar             as entity_name_ultimate_parent_,
        company_id_immediate_parent_::bigint              as company_id_immediate_parent_,
        entity_name_immediate_parent_::varchar            as entity_name_immediate_parent_,
        relationship_immediate_parent_::varchar           as relationship_immediate_parent_,
        _file::varchar                                    as _file,
        _transaction_date::date                           as _transaction_date,
        _etl_date::timestamp                              as _etl_date,
        _modified::timestamp                              as _modified
    from source

)

select * from renamed
