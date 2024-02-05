

select * from "dev"."stg_checkly"."all_checks"
         where date_trunc('day', _transaction_date) = '2024-02-04'