

select * from "dev"."stg__keypay"."invoice_line_item" where date_trunc('day', _transaction_date) = '2023-11-14'