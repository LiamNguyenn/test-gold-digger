module Event
  class Reporter
    def initialize(events)
      @events = events
    end

    def prepare_report_data
      raise 'Not implemented'
    end
  end
end
