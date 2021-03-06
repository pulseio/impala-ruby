module Impala
  # This object represents a connection to an Impala server. It can be used to
  # perform queries on the database.
  class Connection
    SLEEP_INTERVAL = 0.1

    # Don't instantiate Connections directly; instead, use {Impala.connect}.
    def initialize(host, port)
      @host = host
      @port = port
      @connected = false
      @log_context = rand(10000).to_s # I believe this is used for debugging
      open
    end

    def inspect
      "#<#{self.class} #{@host}:#{@port}#{open? ? '' : ' (DISCONNECTED)'}>"
    end

    # Open the connection if it's currently closed.
    def open
      return if @connected

      socket = Thrift::Socket.new(@host, @port)

      @transport = Thrift::BufferedTransport.new(socket)
      @transport.open

      proto = Thrift::BinaryProtocol.new(@transport)
      @service = Protocol::ImpalaService::Client.new(proto)
      @connected = true
    end

    # Close this connection. It can still be reopened with {#open}.
    def close
      return unless @connected

      @connected = false
      @transport.close
    end

    # Returns true if the connection is currently open.
    def open?
      @connected
    end

    # Refresh the metadata store
    def refresh
      raise ConnectionError.new("Connection closed") unless open?

      use_service do |service|
        service.ResetCatalog
      end
    end

    # Perform a query and return all the results. This will
    # load the entire result set into memory, so if you're dealing with lots
    # of rows, {#execute} may work better.
    # @param [String] query the query you want to run
    # @param [Hash] Hash of execute optons that are passed along to
    #               underlying thrift call.  This can be used to
    #               customize your Impala query
    # @return [Array<Hash>] an array of hashes, one for each row.
    def query(raw_query, options = {})
      execute(raw_query, options).fetch_all
    end

    # Perform a query and return a cursor for iterating over the results.
    # @param [String] query the query you want to run
    # @param [Hash] Hash of execute optons that are passed along to
    #               underlying thrift call.  This can be used to
    #               customize your Impala query
    # @return [Cursor] a cursor for the result rows
    def execute(raw_query, options = {})
      raise ConnectionError.new("Connection closed") unless open?

      query = sanitize_query(raw_query)
      handle = send_query(query, options)

      Cursor.new(handle, self)
    end

    # Passes service to invoked block.  If block results in an
    # IOError, socket will be closed.  This allows connections to heal    
    def use_service &block
      
      begin
        yield @service
      rescue Thrift::TransportException, IOError => e
        close()
        raise e
      end

    end

    private

    def sanitize_query(raw_query)
      raw_query
    end

    def send_query(sanitized_query, options = {})
      query = Protocol::Beeswax::Query.new
      query.query = sanitized_query

      if options and options.length > 0
        query.configuration = options.map{|k,v| "#{k}=#{v.to_s}"}
      end
      
      use_service do |service|
        service.executeAndWait(query, @log_context)
      end
    end

    def close_handle(handle)
      use_service {|s| s.close(handle)}
    end
  end
end
