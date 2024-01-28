require_relative './reporter'

module Event
  class AuditReporter < Reporter
    def prepare_report_data
      data = []
      @events.each do |event|
        if event['context']['data'] != '[Filtered]'
          data << body(event)
        else
          puts event['eventID']
        end
      rescue Exception
        puts event['eventID']
      end
      data
    end

    private

    def body(event)
      {
        created_at: event['dateCreated'],
        message: JSON.parse(event['context']['data'])
      }
    end
  end
end
