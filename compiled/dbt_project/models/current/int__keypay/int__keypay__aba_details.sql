

select * from "dev"."stg__keypay"."aba_details" where date_trunc('day', _transaction_date) = '2024-02-02'