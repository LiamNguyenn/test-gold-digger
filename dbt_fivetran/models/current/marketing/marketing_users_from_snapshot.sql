{{ config(materialized="view", alias='users_from_snapshot') }}

select eh_platform_user_id
, first_name
, last_name
, email
, personal_mobile
, date_of_birth
, state
, country
, eh_Platform_Employment_Location
, EH_Platform_Role__c
, eh_Platform_Creation_Date
, eh_Platform_Join_Date
, eh_Platform_First_Mobile_Access_Date
, title
, Org_ID__c
, company
, eh_Platform_Industry
, eh_PLatform_SetUp_Mode
, EH_Platform_Connected_Payroll__c
, eh_Platform_Branded_Payroll
, EH_Platform_Subscription_Level__c
, eh_Platform_Last_LogIn_Date
, eh_Platform_swag_store_enabled
, eh_Platform_org_Instapay_Enabled
, Termination_Date__c
, EH_Platform_Manager__c
, Managed_by_EI__c
, eh_Platform_White_labelled_Payroll
--, Number_Of_Employees
, eh_Platform_Money_Enabled
, eh_Platform_Career_Enabled
, benefits_enabled
, eh_platform_contractor
, eh_platform_bussiness_portal_account
, eh_platform_marketing_consent
, dbt_updated_at as last_updated_date
, case when dbt_valid_to is not null then true else false end as is_deleted
from {{ref('marketing_users_snapshot')}}