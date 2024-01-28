# frozen_string_literal: true

require 'aws-sdk-athena'
require 'byebug'
require 'date'

CLIENT_EXECUTION_TIMEOUT = 100_000
ATHENA_OUTPUT_BUCKET = 's3://aws-athena-query-results-979797940137-ap-southeast-2/hieubui/'
ATHENA_DEFAULT_DATABASE = 'logdna'
SLEEP_AMOUNT_IN_MS = 1000
REGION = 'ap-southeast-2'

def full_date(date, delimiter)
  month = "%02d" % date.month
  day = "%02d" % date.day
  [date.year, month, day].join(delimiter)
end

def dash_date(date)
  full_date(date, '-')
end

def underscore_date(date)
  full_date(date, '_')
end

def splash_date(date)
  full_date(date, '/')
end

def exec(query_string)
  # puts query_string
  client = Aws::Athena::Client.new(region: REGION)
  resp = client.start_query_execution(
    query_string: query_string,
    query_execution_context: {
      database: ATHENA_DEFAULT_DATABASE,
      catalog: 'AwsDataCatalog'
    },
    result_configuration: {
      output_location: ATHENA_OUTPUT_BUCKET
    }
  )
  puts resp
end

def add_partition(date)
  exec(%{
    ALTER TABLE audits_tmp
    ADD IF NOT EXISTS PARTITION (dt='#{dash_date(date)}')
    LOCATION 's3://eh-audits-prod/tmp/#{splash_date(date)}/'
  })
end

def drop_partition(date)
  exec(%{
    ALTER TABLE audits_tmp
    DROP IF EXISTS PARTITION (dt='#{dash_date(date)}')
  })
end

def drop_table(date)
  exec(%{
    DROP TABLE audits_archived_#{underscore_date(date)}
  })
end

def copy_data_by_query(date)
  query_string = %{
  CREATE TABLE audits_archived_#{underscore_date(date)}
  WITH
  (
    format='PARQUET',
    write_compression='GZIP',
    external_location='s3://eh-audits-prod/#{splash_date(date)}/'
  ) AS
  SELECT
    id,
    auditable_id,
    auditable_type,
    auditor_id,
    auditor_uuid,
    auditor_type,
    group_id,
    group_uuid,
    group_type,
    event,
    object_changes,
    data,
    created_at,
    updated_at
  FROM
    audits_tmp
  WHERE
    dt = '#{dash_date(date)}'
  }
  exec(query_string)
end

def prepare_for_dates(dates)
  counter = 0
  # make sure partition available
  dates.each do |d|
    add_partition(d)
    counter += 1

    sleep(5) if (counter % 10).zero?
  end
end

def run_for_dates(dates)
  counter = 0
  dates.each do |d|
    copy_data_by_query(d)
    counter += 1

    sleep(5) if (counter % 10).zero?
  end
end

def clean_up_for_dates(dates)
  counter = 0
  # drop partitions
  dates.each do |d|
    drop_partition(d)
    counter += 1

    sleep(5) if (counter % 5).zero?
  end

  counter = 0
  # drop created tables
  dates.each do |d|
    drop_table(d)
    counter += 1

    sleep(5) if (counter % 5).zero?
  end
end

def main(from, to, command)
  from_date = Date.parse(from)
  to_date = Date.parse(to)

  dates = (from_date..to_date).to_a
  case command
  when 1
    prepare_for_dates(dates)
  when 2
    run_for_dates(dates)
  when 3
    clean_up_for_dates(dates)
  end
end

PREPARE = 1
RUN = 2
CLEAN_UP = 3

from = '2020-11-01'
to = '2020-11-30'

main(from, to, PREPARE)
main(from, to, RUN)
main(from, to, CLEAN_UP)
