

select * from "dev"."stg__keypay"."pay_cycle_frequency" where date_trunc('day', _transaction_date) = '2024-02-01'