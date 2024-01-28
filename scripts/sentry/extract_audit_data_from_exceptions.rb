require 'csv'
require 'json'
require 'time'
require 'httparty'
require 'byebug'

require './event/downloader'
require './event/saver'
require './event/audit_reporter'

sentry_token = ENV['SENTRY_TOKEN']

issue_id = '3353482890'

downloader = Event::Downloader.new(
  sentry_token,
  issue_id: issue_id,
  total_events: 300
)
event_data = downloader.download

saver = Event::Saver.new
saver.save_as_json("raw-#{issue_id}", event_data)

reporter = Event::AuditReporter.new(event_data)
reporting_data = reporter.prepare_report_data
saver.save_as_json("report-#{issue_id}", reporting_data)
