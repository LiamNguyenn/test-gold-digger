module Event
  class Downloader
    ORGANISATION_SLUG = 'employment-hero'
    TOTAL_EXPECTED = 1000

    attr_reader :events

    def initialize(sentry_token, issue_id:, total_events: TOTAL_EXPECTED)
      @token = "Bearer #{sentry_token}"
      @issue_id = issue_id
      @total_events = total_events
    end

    def download
      @events = download_events
    end

    private

    def download_events
      headers = { headers: { Authorization: @token } }
      url = "#{event_url}?#{query_params}"
      data = []

      puts "[START] Download events of issue #{@issue_id}"

      while data.size < @total_events
        response = HTTParty.get(url, headers)
        response_data = response.parsed_response
        data += response_data

        puts "[IN PROGRESS] Downloaded #{response_data.size}. Total: #{data.size}"

        pagination = response.headers["link"].split(', ').last.split('; ')
        url = (pagination[2] == 'results="true"') ? pagination[0][1..-2] : nil

        break if url.nil?
      end

      puts "[DONE] Download events of issue #{@issue_id}. Downloaded #{data.count}"
      data
    end

    def event_url
      "https://sentry.io/api/0/issues/#{@issue_id}/events/"
    end

    def query_params
      'cursor=0:0:1&full=true'
    end
  end
end
