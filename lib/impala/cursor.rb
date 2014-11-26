module Impala
  # Cursors are used to iterate over result sets without loading them all
  # into memory at once. This can be useful if you're dealing with lots of
  # rows. It implements Enumerable, so you can use each/select/map/etc.
  class Cursor
    BUFFER_SIZE = 1024
    include Enumerable

    def initialize(handle, connection)
      @handle = handle
      @connection = connection

      @row_buffer = []
      @done = false
      @open = true

      fetch_more
    end

    def inspect
      "#<#{self.class}#{open? ? '' : ' (CLOSED)'}>"
    end

    def each
      while row = fetch_row
        yield row
      end
    end

    # Returns the next available row as a hash, or nil if there are none left.
    # @return [Hash, nil] the next available row, or nil if there are none
    #    left
    # @see #fetch_all
    def fetch_row
      raise CursorError.new("Cursor has expired or been closed") unless open? or @done

      if @row_buffer.empty?
        if @done
          return nil
        else
          fetch_more
        end
      end

      @row_buffer.shift
    end

    # Returns all the remaining rows in the result set.
    # @return [Array<Hash>] the remaining rows in the result set
    # @see #fetch_one
    def fetch_all
      self.to_a
    end

    # Close the cursor on the remote server. Once a cursor is closed, you
    # can no longer fetch any rows from it.
    def close
      @open = false
      
      @connection.use_service do |service|
        service.close(@handle)
      end
    end

    # Returns true if the cursor is still open.
    def open?
      @open
    end

    # Returns true if there are any more rows to fetch.
    def has_more?
      !@done || !@row_buffer.empty?
    end

    private

    def metadata
      @connection.use_service do |service|
        @metadata ||= service.get_results_metadata(@handle)
      end
    end

    def fetch_more
      fetch_batch until @done || @row_buffer.count >= BUFFER_SIZE
    end

    def fetch_batch
      return if @done

      res = nil
      @connection.use_service do |service|
        begin
          res = service.fetch(@handle, false, BUFFER_SIZE)
        rescue Protocol::Beeswax::BeeswaxException => e
          @open = false
          raise CursorError.new("Cursor has expired or been closed: #{e.to_s}, #{e.message}")
        end
      end

      rows = res.data.map { |raw| parse_row(raw) }
      @row_buffer.concat(rows)
      unless res.has_more
        close()
        @done = true
      end
    end

    def parse_row(raw)
      row = {}
      fields = raw.split(metadata.delim)

      fields.zip(metadata.schema.fieldSchemas).each do |raw_value, schema|
        if schema
          value = convert_raw_value(raw_value, schema)
          row[schema.name.to_sym] = value
        end
      end

      row
    end

    def convert_raw_value(value, schema)
      return nil if schema == nil or (value == 'NULL' && schema.type == 'null')
      case schema.type
      when 'string'
        value
      when 'boolean'
        if value == 'true'
          true
        elsif value == 'false'
          false
        else
          raise ParsingError.new("Invalid value for boolean: #{value}")
        end
      when 'tinyint', 'int', 'bigint'
        value.to_i
      when 'double', 'float'
        if value == 'Infinity'
          Float::INFINITY
        else
          value.to_f
        end
      when "timestamp"
        # Impala timestamps are time since epoch, so let's set the
        # time zone to UTC
        Time.parse(value + ' UTC')
      else
        raise ParsingError.new("Unknown type: #{schema.type}")
      end
    end
  end
end
