

    with test_data as (

        select
            cast('POSTCODE' as TEXT) as column_name,
            7 as matching_column_index,
            True as column_index_matches

    )
    select *
    from test_data
    where
        not(matching_column_index >= 0 and column_index_matches)