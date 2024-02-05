

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
    "dev"."int__keypay_dwh"."business" b
    JOIN "dev"."int__keypay"."white_label" AS wl ON b.White_Label_Id = wl.id
    left join "dev"."int__keypay"."region"  r on wl.region_id = r.id
    left join "dev"."int__keypay"."resellers" p on p.id = wl.Reseller_Id