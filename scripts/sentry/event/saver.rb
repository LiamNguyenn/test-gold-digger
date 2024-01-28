module Event
  class Saver
    def save_as_json(filename, data)
      file = json_filename(filename)

      puts "[START] Saving JSON data as #{file}"
      File.write(file, JSON.dump(data))
      puts "[DONE]"
      file
    end

    def save_as_csv(filename, data)
      file = csv_filename(filename)

      puts "[START] Writting CSV data as #{file}"
      CSV.open(file, 'w') { |csv| data.each { |row| csv << row } }
      puts "[DONE]"
      file
    end

    private

    def json_filename(filename)
      "json/#{filename}-#{Time.now}.json".freeze
    end

    def csv_filename(filename)
      "csv/#{filename}-#{Time.now}.csv".freeze
    end
  end
end
