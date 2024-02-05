select distinct
    reason_type_key,
    case
        when reason_type_key = 0 then 'default'
        when reason_type_key = 1 then 'other'
        when reason_type_key = 2 then 'marketing'
        when reason_type_key = 3 then 'assisted_implementation'
        when reason_type_key = 4 then 'staff_rewards'
        when reason_type_key = 5 then 'commission'
        when reason_type_key = 6 then 'instapay_dev'
        when reason_type_key = 7 then 'refund'
        when reason_type_key = 8 then 'organisation_issuance'
        when reason_type_key = 9 then 'transaction_fee'
        when reason_type_key = 10 then 'points_compensation'
        else 'unknown'
    end as reason_type

from "dev"."staging"."stg_herodollar_service_public__tracking_infos"