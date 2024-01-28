# frozen_string_literal: true

require 'aws-sdk-dynamodb'
require 'byebug'
require 'date'

CLIENT_EXECUTION_TIMEOUT = 100_000
REGION = 'ap-southeast-2'
SLEEP_AMOUNT_IN_MS = 1000

client = Aws::DynamoDB::Client.new(region: REGION)
# client.operation_names

resp = client.query({
  table_name: "eh_audits",
  select: "ALL_ATTRIBUTES",
  expression_attribute_values: {
    ":v1" => {
      s: "No One You Know",
    },
  },
  key_condition_expression: "Artist = :v1",
  projection_expression: "SongTitle",
})
