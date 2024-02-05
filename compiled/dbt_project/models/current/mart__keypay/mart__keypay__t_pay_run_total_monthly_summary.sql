

select coalesce(prt.employee_id, prth.employee_id) as employee_id
  , pr.business_id
  , pr.invoice_id
  --, DATEADD(month, 1, DATEFROMPARTS(year(pr.DateFirstFinalised), month(pr.DateFirstFinalised), 1)) as BillingMonth
  , DATEADD('DAY', 1, LAST_DAY(pr.date_first_finalised)) as billing_month
  , coalesce(prt.is_excluded_from_billing, prth.is_excluded_from_billing) as is_excluded_from_billing
  , sum(coalesce(prt.gross_earnings, prth.gross_earnings)) as monthly_gross_earnings
  , sum(coalesce(prt.net_earnings, prth.net_earnings)) as monthly_net_earnings
  , sum(coalesce(prt.total_hours, prth.total_hours)) as total_hours
  --, sum(prt.PaygWithholdingAmount) as PaygWithholdingAmount
  --, sum(prt.HelpAmount) as HelpAmount
  --, sum(prt.SuperContribution) as SuperContribution
from  "dev"."int__keypay"."payrun" pr
  	join "dev"."int__keypay_dwh"."business" b on pr.business_id = b.id AND SPLIT_PART(b._file, 'Shard', 2) = SPLIT_PART(pr._file, 'Shard', 2)
	left join "dev"."int__keypay"."payrun_total" prt on prt.payrun_id = pr.id and SPLIT_PART(pr._file, 'Shard', 2) = SPLIT_PART(prt._file, 'Shard', 2) and pr.date_first_finalised::date >= '2022-01-01' and prt.is_excluded_from_billing = 0
  	left join "dev"."stg__keypay"."payrun_total_history" prth on prth.payrun_id = pr.id and SPLIT_PART(pr._file, 'Shard', 2) = SPLIT_PART(prth._file, 'Shard', 2) and pr.date_first_finalised::date < '2022-01-01' and prth.is_excluded_from_billing = 0
  where
      pr.date_first_finalised is not null
      --and pr.DateFirstFinalised >= ''''',@fromDate,'''''      and pr.DateFirstFinalised <= ''''',@toDate,'''''           
      and (b.to_be_deleted is null or not to_be_deleted) --ISNULL(b.to_be_deleted, 0) = 0
    group by 1,2,3,4,5