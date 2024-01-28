{% snapshot proserv_payroll_settings_snapshot %}

{{
    config(
    alias='payroll_settings_snapshot',
    target_schema="proserv",
    strategy='check',
    unique_key='business_id',
    check_cols=['is_payroll_settings_completed'],
    invalidate_hard_deletes=True,
    )
}}

select distinct b.id as business_id
, true as is_payroll_settings_completed
    from {{ref('keypay_dwh_business')}} b
    join (select distinct businessid from {{ref('keypay_location')}} where not Is_Deleted) l on l.businessid = b.id
    join (select distinct business_id from {{ref('keypay_pay_cycle')}} where not Is_Deleted) pc on pc.business_id = b.id
    left join {{ref('keypay_aba_details')}} ad on b.id = ad.businessid  -- AU
    left join {{ref('keypay_bacs_details')}} bd on bd.businessid = b.id  -- UK
    left join {{ref('keypay_csn_details')}} cd on b.id = cd.business_id -- SG
    left join {{ref('keypay_statutory_settings')}} ss on b.id = ss.business_id -- MY
    left join {{ref('keypay_bank_payment_file_details')}} bp on b.id = bp.business_id --NZ
    where b.name is not null and b.abn is not null and (b.to_be_deleted = 0 or b.to_be_deleted is null)
    and (ad.businessid is not null 
        or bd.businessid is not null 
        or cd.business_id is not null 
        or (ss.income_tax_number_encrypted is not null and ss.e_number is not null and ss.epf_number is not null and ss.socso_number is not null and ss.hrdf_status is not null)
        or (bp.business_id is not null and bp.file_format is not null and bp.originating_account_number is not null and bp.originating_account_name is not null and bp.lodgement_reference is not null ))

{% endsnapshot %}
