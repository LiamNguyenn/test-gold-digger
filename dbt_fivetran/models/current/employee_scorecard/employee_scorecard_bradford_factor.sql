{{ config(materialized="view", alias="bradford_factor") }}

with
	leave_details as (
	    select
	    	lr.id
	      	, lc.name as leave_name
	      	, lc.organisation_id
	      	, lr.member_id
	      	, lr.start_date
	      	, lr.end_date
	      	, lr.total_units
	      	, lr.unit_type
	      	, case 
		        when lr.unit_type = 'hours' then lr.total_units::float/8
		        when lr.unit_type = 'weeks' then lr.total_units*5
		        else lr.total_units::float
		        end as leave_days_by_units
	      	, datediff(days, lr.start_date, lr.end_date)+1 as max_possible_leave
	      	, case 
	          	when leave_days_by_units>max_possible_leave then max_possible_leave
	          	else leave_days_by_units
	        	end as leave_in_days
	      	, lr.comment
	      	, coalesce(lr.approved, True) as approved
	      	, case when (lr.approved or lr.approved is null) and leave_in_days>=0 then true else false end as leave_taken
	    from
	      	{{ source('postgres_public', 'leave_requests') }} as lr
	      	join {{ source('postgres_public', 'leave_categories') }} as lc on
	        	lr.leave_category_id = lc.id
	    where
	      	not lr._fivetran_deleted
	      	and not lc._fivetran_deleted
	      	and not lc.deleted
	      	-- and lc.name !~* '.*(Archive|DNU|Do Not Use).*'
	      	and lc.name !~* '.*(archive|Lieu|toil|DNU|Do Not Use|Maternity|Long Service Leave|holiday|voluntary|compassionate|bereavement).*'
      		and lc.name ~* '.*(sick|personal|carer|medical|emergency|absence).*'
	)
	, bradford_score as (
		select
	      	ld.member_id
	      	, count(
	      		case
	            	when not e.active and ld.start_date>=dateadd(days, -365, e.termination_date) and ld.end_date<=e.termination_date and ld.leave_taken then ld.id
	              	when e.active and ld.start_date>=dateadd(days, -365, getdate()) and ld.end_date<=getdate() and ld.leave_taken then ld.id
	            end) as frequency_within_year

	      	, case 
		        when coalesce(
		        	sum(
		        		case 
				            when not e.active and ld.start_date>=dateadd(days, -365, e.termination_date) and ld.end_date<=e.termination_date and ld.approved then ld.leave_in_days
				            when e.active and ld.start_date>=dateadd(days, -365, getdate()) and ld.end_date<=getdate() and ld.approved then ld.leave_in_days
		          		end), 0)<0 then 0
		        when coalesce(
		        	sum(
		        		case 
		            		when not e.active and ld.start_date>=dateadd(days, -365, e.termination_date) and ld.end_date<=e.termination_date and ld.approved then ld.leave_in_days
		            		when e.active and ld.start_date>=dateadd(days, -365, getdate()) and ld.end_date<=getdate() and ld.approved then ld.leave_in_days
		          		end), 0)>260 then 260 
		        else coalesce(
		        	sum(
		        		case 
		            		when not e.active and ld.start_date>=dateadd(days, -365, e.termination_date) and ld.end_date<=e.termination_date and ld.approved then ld.leave_in_days
		           			when e.active and ld.start_date>=dateadd(days, -365, getdate()) and ld.end_date<=getdate() and ld.approved then ld.leave_in_days
		          		end), 0) end as total_leave_days_within_year

	      	, POWER(frequency_within_year,2)*total_leave_days_within_year as bradford_score
	    from 
	      	leave_details as ld
		    join {{ref('employee_scorecard_cohort')}} as e on
		    	ld.member_id = e.member_id
	    group by 1
	)

select * from bradford_score