# frozen_string_literal: true

def extract_old_data(date_str)
  data_date = Date.parse(date_str)
  next_date = data_date.tomorrow

  # count first
  count_sql = <<-SQL.squish
    SELECT COUNT(*)
    FROM audits
    WHERE created_at >= '#{data_date}' AND created_at < '#{next_date}'
  SQL
  count = ActiveRecord::Base.connection.execute(count_sql)
  puts "Date: #{date_str}. Count: #{count.first['count']}"

  sql = <<-SQL.squish
    SELECT *
    FROM audits
    WHERE created_at >= ''#{data_date}'' AND created_at < ''#{next_date}''
  SQL

  export_sql = <<-SQL.squish
    SELECT *
    FROM aws_s3.query_export_to_s3(
      '#{sql}',
      'eh-audits-prod',
      'tmp/#{data_date.strftime('%Y/%m/%d')}/audits.csv',
      'ap-southeast-2',
      options :='format CSV, delimiter $$;$$'
    )
  SQL

  # puts export_sql
  ActiveRecord::Base.connection.execute(export_sql)
end

def run_month_of_day(date)
  from_date = Date.parse(date)
  to_date = from_date.end_of_month

  (from_date..to_date).to_a.map(&:to_s).each do |dt|
    extract_old_data(dt)
    sleep 3
  end
end

run_month_of_day('2020-06-02')
## Missed
extract_old_data('2020-06-01')
run_month_of_day('2020-02-02')

run_month_of_day('2020-03-01')
run_month_of_day('2020-04-01')
run_month_of_day('2020-05-01')
run_month_of_day('2020-07-01')
run_month_of_day('2020-08-01')
run_month_of_day('2020-09-01')
run_month_of_day('2020-10-01')
run_month_of_day('2020-11-01')

count_sql = <<-SQL.squish
  SELECT COUNT(*)
  FROM audits
  WHERE created_at >= '2020-02-01' AND created_at < '2020-02-02'
SQL
count = ActiveRecord::Base.connection.execute(count_sql)
puts "Date: 2020-02-01. Count: #{count.first['count']}"
