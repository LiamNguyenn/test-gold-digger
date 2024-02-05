

select * from "dev"."stg__keypay"."bank_payment_file_details" where date_trunc('day', _transaction_date) = '2024-02-02'