select distinct
    payment_method_key,
    case
        when payment_method_key = 0 then 'credit_card'
        when payment_method_key = 1 then 'instapay'
        when payment_method_key = 2 then 'hero_dollars'
        when payment_method_key = 3 then 'hero_points'
        else 'unknown'
    end as payment_method


from "dev"."staging"."stg_heroshop_db_public__transactions"