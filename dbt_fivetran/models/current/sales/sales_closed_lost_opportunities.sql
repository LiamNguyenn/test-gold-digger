{{
    config(
        materialized='incremental',
        alias='closed_lost_opportunities'
    )
}}

select distinct
  a.id as account_id
  , a.name as account_name
  , o.id as opportunity_id
  , o.name as opportunity_name
  , case o.record_type_id 
      when '0120o0000017euhAAA' then 'Global Teams'
      when '0120o000001JYSxAAO' then 'Direct Sales'
      when '0120o000001JYSyAAO' then 'Hero Referrer'
      when '0120o000001JYT1AAO' then 'Upsell'
    end as opportunity_type
  , o.close_date as lost_at
  , a.geo_code_c as country
  , a.industry_primary_c as industry
  , l.no_of_lost_features
  , c.iso_code as currency
  , sum(coalesce(
      hr_mrr.zqu_total_c * 12                                                     -- quote MRR
      , ol.arr_annuity_c                                                          -- product ARR
      , o.bizible_2_bizible_opportunity_amount_c                                  -- opportunity amount
      , case 
          when country = 'AU' then o.opportunity_employees_c * 12 * 13.36         -- salesforce backend calculation for AU from CommOps (check Tom Pyle & Maddy)
          when country = 'UK' then o.opportunity_employees_c * 12 * 6.65          -- salesforce backend calculation for UK from CommOps (check Tom Pyle & Maddy)
          when country = 'SG' then o.opportunity_employees_c * 12 * 5             -- salesforce backend calculation for SG from CommOps (check Tom Pyle & Maddy)
          when country = 'NZ' then o.opportunity_employees_c * 12 * 10.64         -- salesforce backend calculation for NZ from CommOps (check Tom Pyle & Maddy)
        end   
      , 0)) over(partition by o.id, q.id)::decimal(10,2) as arr
  , case 
      when ( hr.zqu_quote_product_name_c is null or hr.zqu_quote_product_name_c !~* 'eh hr software' ) 
          and ( p.name is null or p.name !~* 'employment hero software' ) then false
        else true 
    end as quoted
  , o.opp_loss_reason_detail_c as lost_reason_detail
from
  salesforce.opportunity o
  join salesforce.account a on
    o.account_id = a.id
  join (
      select opportunity_id, count(lost_reason) as no_of_lost_features
      from sales.closed_lost_opportunity_reasons
      group by opportunity_id
      ) as l on
    o.id = l.opportunity_id
  left join salesforce.opportunity_line_item ol on
    o.id = ol.opportunity_id
    and not ol.is_deleted
  left join salesforce.product_2 as p on 
    ol.product_2_id = p.id 
    and not p.is_deleted
  left join (
    select *
    from salesforce.zqu_quote_c 
    where id in (
      select first_value(id) 
        over (partition by zqu_opportunity_c order by zqu_valid_until_c desc, zqu_number_c desc
              rows between unbounded preceding and current row)
      from salesforce.zqu_quote_c 
      where not is_deleted )
      ) as q on
    o.id = q.zqu_opportunity_c
  left join salesforce.zqu_quote_rate_plan_c as hr on
    q.id = hr.zqu_quote_c
    and not hr.is_deleted
  left join salesforce.zqu_quote_rate_plan_charge_c as hr_mrr on
    hr.id = hr_mrr.zqu_quote_rate_plan_c
    and not hr_mrr.is_deleted
  left join salesforce.currency_type c on
    coalesce(q.zqu_currency_c, o.currency_iso_code) = c.iso_code
    and c.is_active = 't'
    and not c._fivetran_deleted
where
  not o.is_deleted
  and lower(o.stage_name) = 'lost'
  and o.lost_reason_c ilike 'product%' 
  and (p.name is null or p.name ~* 'employment hero software')
  and (hr.zqu_quote_product_name_c is null or hr.zqu_quote_product_name_c ~* 'eh hr software')
  and (hr_mrr.name is null or lower(hr_mrr.name) = 'contracted users')

{% if is_incremental() %}  
  and not exists (select 'x' from {{this}} lo where lo.opportunity_id = o.id)
{% endif %}      
order by 
  a.geo_code_c
  , opportunity_type
  , o.close_date desc