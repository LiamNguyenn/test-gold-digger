{{ config(alias='business_traits') }}

select b.id
    , b.name
    , b.date_created as created_at
    , b.industry_id
    , b.industry_name 
    , split_part(r.culture_name,'-',2) as country
    , b.commence_billing_from::date
    , b.White_Label_Id
    , wl.name as white_label_name
    , wl.Reseller_Id as partner_id
    , p.name as partner_name
from
    {{ref('int__keypay_dwh__business')}} b
    JOIN {{ref('int__keypay__white_label')}} AS wl ON b.White_Label_Id = wl.id
    left join {{ref('int__keypay__region')}}  r on wl.region_id = r.id
    left join {{ref('int__keypay__resellers')}} p on p.id = wl.Reseller_Id