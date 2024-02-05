

select * from "dev"."stg__keypay"."timesheet_line" where date_trunc('day', _transaction_date) = '2023-11-08'