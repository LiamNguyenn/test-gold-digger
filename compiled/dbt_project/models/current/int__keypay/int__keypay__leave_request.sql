

select * from "dev"."stg__keypay"."leave_request" where date_trunc('day', _transaction_date) = '2024-02-01'