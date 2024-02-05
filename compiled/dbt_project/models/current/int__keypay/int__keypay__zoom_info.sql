
    with source as (

select * from "dev"."stg__keypay"."zoom_info"

),

renamed as (

select
        record_id,
        _id,
        company_name,
        abn,
        country,
        match_status,
        zoom_info_company_id,
        website,
        founded_year,
        company_hq_phone,
        revenue_in_000_s_usd_,
        revenue_range_in_usd_,
        est_hr_department_budget_in_000_s_usd_,
        employees,
        employee_range,
        past_1_year_employee_growth_rate,
        past_2_year_employee_growth_rate,
        sic_code_1,
        sic_code_2,
        sic_codes,
        naics_code_1,
        naics_code_2,
        naics_codes,
        primary_industry,
        primary_sub_industry,
        all_industries,
        all_sub_industries,
        industry_hierarchical_category,
        secondary_industry_hierarchical_category,
        alexa_rank,
        zoom_info_company_profile_url,
        linked_in_company_profile_url,
        facebook_company_profile_url,
        twitter_company_profile_url,
        ownership_type,
        business_model,
        certified_active_company,
        certification_date,
        defunct_company,
        total_funding_amount_in_000_s_usd_,
        recent_funding_amount_in_000_s_usd_,
        recent_funding_round,
        recent_funding_date,
        recent_investors,
        all_investors,
        company_street_address,
        company_city,
        company_state,
        company_zip_code,
        company_country,
        full_address,
        number_of_locations,
        company_is_acquired,
        company_id_ultimate_parent_,
        entity_name_ultimate_parent_,
        company_id_immediate_parent_,
        entity_name_immediate_parent_,
        relationship_immediate_parent_,
        _file,
        _transaction_date,
        _etl_date,
        _modified
from source

)

select *
from renamed