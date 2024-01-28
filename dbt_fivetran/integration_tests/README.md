# Overview
This dbt project contains all integration tests for dbt_fivetran project.
Kindy refer to [dbt docs](https://docs.getdbt.com/blog/unit-testing-dbt-packages#unit-testing-vs-integration-testing) to know what is integration test and when we should create integration tests.
# Setup
Same as `dbt_fivetran` project.

## Prerequisites
- python3

# Run integration test
- cd into `dbt_fivetran/integration_tests`
- Run `dbt deps`
- Run `dbt seed`
- Create models: `dbt run --target {your_target} --model {your_testing_model}`
- Test models: `dbt test --target {your_target} --model {your_testing_model}`   



