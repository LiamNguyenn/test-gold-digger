







with a as (

    select 
      
      1 as id_dbtutils_test_fewer_rows_than,
      count(*) as count_our_model 
    from "dev"."exports"."exports_v_braze_email_unsubscriptions"
    group by id_dbtutils_test_fewer_rows_than

),
b as (

    select 
      
      1 as id_dbtutils_test_fewer_rows_than,
      count(*) as count_comparison_model 
    from "dev"."exports"."exports_braze_users"
    group by id_dbtutils_test_fewer_rows_than

),
counts as (

    select

        a.id_dbtutils_test_fewer_rows_than as id_dbtutils_test_fewer_rows_than_a,
          b.id_dbtutils_test_fewer_rows_than as id_dbtutils_test_fewer_rows_than_b,
        

        count_our_model,
        count_comparison_model
    from a
    full join b on 
    a.id_dbtutils_test_fewer_rows_than = b.id_dbtutils_test_fewer_rows_than
    

),
final as (

    select *,
        case
            -- fail the test if we have more rows than the reference model and return the row count delta
            when count_our_model > count_comparison_model then (count_our_model - count_comparison_model)
            -- fail the test if they are the same number
            when count_our_model = count_comparison_model then 1
            -- pass the test if the delta is positive (i.e. return the number 0)
            else 0
    end as row_count_delta
    from counts

)

select * from final

