# frozen_string_literal: true

require 'duckdb'
require 'active_record'
require 'active_record/base'
require 'active_record/connection_adapters/abstract_adapter'
require 'fileutils'
require 'active_record/connection_adapters/duckdb/quoting'
require 'active_record/connection_adapters/duckdb/database_statements'
require 'active_record/connection_adapters/duckdb/schema_statements'
require 'active_record/connection_adapters/duckdb/explain_pretty_printer'
require 'active_record/connection_adapters/duckdb/tasks'

module ActiveRecord

  module ConnectionAdapters # :nodoc:
    class DuckdbAdapter < AbstractAdapter
      # = Active Record DuckDB Adapter
      #
      # The DuckDB adapter works with https://github.com/suketa/ruby-duckdb driver.
      #
      # Options:
      #
      # * <tt>:database</tt> - Path to the database file. Defaults to 'db/duckdb.db'.
      #                       Use ':memory:' for in-memory database.
      class << self
        ADAPTER_NAME = "DuckDB".freeze
        
        # @note DuckDB-specific client creation
        # @param [Hash, nil] config Configuration hash containing database path
        # @return [DuckDB::Connection] A new DuckDB connection
        def new_client(config = nil)
          # Handle different config formats (hash with string or symbol keys)
          database_path = if config
            config[:database] || config['database'] || 'db/duckdb.db'
          else
            'db/duckdb.db'
          end
          
          if database_path == ':memory:'
            DuckDB::Database.open.connect # in-memory database
          else
            # Ensure directory exists for file-based database
            dir = File.dirname(database_path)
            FileUtils.mkdir_p(dir) unless File.directory?(dir)
            DuckDB::Database.open(database_path).connect
          end
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [Hash] config Database configuration
        # @param [Hash] options Console options
        # @return [void]
        def dbconsole(config, options = {})
        end
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @param [Array] args Arguments passed to superclass
      # @return [DuckdbAdapter] New adapter instance
      def initialize(connection, logger = nil, config = {})
        # Create a mutable copy of the config to avoid FrozenError
        config = config.dup
        super(connection, logger, config)
        @max_identifier_length = nil
        @type_map = nil
        # Use the provided connection if it's a DuckDB connection, otherwise create a new one
        @raw_connection = connection.is_a?(DuckDB::Connection) ? connection : self.connect
        @notice_receiver_sql_warnings = []

        # Determine if we're using a memory database
        database_path = @config[:database] || 'db/duckdb.db'
        @memory_database = database_path == ':memory:'

        # Set up file path for file-based databases
        unless @memory_database
          case database_path
          when ""
            raise ArgumentError, "No database file specified. Missing argument: database"
          when /\Afile:/
            # Handle file:// URLs by extracting the path
            @config[:database] = database_path.sub(/\Afile:/, '')
          else
            # Handle relative paths - make them relative to Rails.root if in Rails
            if defined?(Rails.root) && !File.absolute_path?(database_path)
              @config[:database] = File.expand_path(database_path, Rails.root)
            else
              @config[:database] = File.expand_path(database_path)
            end
            
            # Ensure the directory exists
            dirname = File.dirname(@config[:database])
            unless File.directory?(dirname)
              begin
                FileUtils.mkdir_p(dirname)
              rescue SystemCallError
                raise ActiveRecord::NoDatabaseError.new(connection_pool: @pool)
              end
            end
          end
        end
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [Boolean] true if database exists, false otherwise
      def database_exists?
        if @memory_database
          true # Memory databases always "exist" once created
        else
          File.exist?(@config[:database].to_s)
        end
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @note Connects to a DuckDB database and sets up the adapter depending on the connected database's characteristics
      # @return [DuckDB::Connection] Raw database connection
      def connect
        @raw_connection = self.class.new_client(@config)
      rescue ConnectionNotEstablished => ex
        raise ex
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [DuckDB::Connection] Raw database connection
      def reconnect
        @raw_connection
      end

      include Duckdb::DatabaseStatements
      include Duckdb::SchemaStatements
      include Duckdb::Quoting

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [Hash] Hash of native database types
      def native_database_types # :nodoc:
        {
          primary_key: "BIGINT PRIMARY KEY",
          string: { name: "VARCHAR" },
          text: { name: "TEXT" },
          integer: { name: "INTEGER" },
          bigint: { name: "BIGINT" },
          float: { name: "REAL" },
          decimal: { name: "DECIMAL" },
          datetime: { name: "TIMESTAMP" },
          time: { name: "TIME" },
          date: { name: "DATE" },
          binary: { name: "BLOB" },
          boolean: { name: "BOOLEAN" },
          json: { name: "JSON" }
        }
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [String] The adapter name
      def adapter_name # :nodoc:
        "DuckDB"
      end

      # Capability flags - tell ActiveRecord what features DuckDB supports
      # These are used internally by ActiveRecord to decide how to handle various operations

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [Boolean] true if DuckDB supports savepoints
      def supports_savepoints? # :nodoc:
        true  # DuckDB can create savepoints within transactions (SAVEPOINT sp1, ROLLBACK TO sp1)
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [Boolean] true if DuckDB supports transaction isolation
      def supports_transaction_isolation? # :nodoc:
        true  # DuckDB supports transaction isolation using Snapshot Isolation (full ACID compliance)
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [Boolean] true if DuckDB supports index sort order
      def supports_index_sort_order? # :nodoc:
        true  # DuckDB can create indexes with sort order (CREATE INDEX idx ON table (col ASC/DESC))
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [Boolean] true if DuckDB supports partial indexes
      def supports_partial_index? # :nodoc:
        true  # DuckDB supports advanced indexing including zone maps and selective indexing
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @return [Boolean] true if adapter needs periodic reloading
      def requires_reloading? # :nodoc:
        true  # Adapter needs to reload connection info periodically due to DuckDB's file-based nature
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @param [String] table_name Name of the table
      # @return [Array<String>] Array of primary key column names
      def primary_keys(table_name) # :nodoc:
        raise ArgumentError unless table_name.present?

        # Query DuckDB's information_schema for primary key columns using parameterized query
        # Use constraint_type = 'PRIMARY KEY' for reliable identification
        sql = <<~SQL
          SELECT kcu.column_name
          FROM information_schema.key_column_usage kcu
          JOIN information_schema.table_constraints tc 
            ON kcu.constraint_name = tc.constraint_name 
            AND kcu.table_name = tc.table_name
          WHERE kcu.table_name = ?
            AND tc.constraint_type = 'PRIMARY KEY'
          ORDER BY kcu.ordinal_position
        SQL
        
        # Create bind parameter for the parameterized query
        binds = [
          ActiveRecord::Relation::QueryAttribute.new("table_name", table_name, ActiveRecord::Type::String.new)
        ]
        
        results = internal_exec_query(sql, "SCHEMA", binds)
        results.rows.map { |row| row[0] }
      end

      # @override
      # @note Implements AbstractAdapter interface method
      # @param [Symbol, nil] isolation Transaction isolation level
      # @param [Boolean] joinable Whether transaction is joinable
      # @param [Boolean] _lazy Whether transaction is lazy
      # @return [void]
      def begin_transaction(isolation: nil, joinable: true, _lazy: true); end

      # @override
      # @note Implements AbstractAdapter interface method
      # @param [String] table_name Name of the table
      # @return [Array<ActiveRecord::ConnectionAdapters::Column>] Array of column objects
      def columns(table_name) # :nodoc:
        column_definitions(table_name).map do |field|
          new_column_from_field(table_name, field)
        end
      end

      # @note Support for getting the next sequence value for auto-increment
      # @param [String] sequence_name Name of the sequence
      # @return [String] SQL expression for next sequence value
      def next_sequence_value(sequence_name)
        "nextval('#{sequence_name}')"
      end

      # @override
      # @note Implements AbstractAdapter interface method - ActiveRecord needs this to know we support INSERT...RETURNING
      # @return [Boolean] true if INSERT...RETURNING is supported
      def supports_insert_returning?
        true
      end

      # @override
      # @note Implements AbstractAdapter interface method - Tell ActiveRecord to return the primary key value after insert
      # @param [ActiveRecord::ConnectionAdapters::Column] column The column to check
      # @return [Boolean] true if should return value after insert
      def return_value_after_insert?(column)
        (column.type == :integer || column.type == :bigint) && column.name == 'id'
      end

      private
        # @note Simple implementation for now - just execute the SQL
        # @param [String] sql SQL to execute
        # @param [String] name Query name for logging
        # @param [Array] binds Bind parameters
        # @param [Boolean] prepare Whether to prepare statement
        # @param [Boolean] async Whether to execute asynchronously
        # @return [Object] Query result
        def execute_and_clear(sql, name, binds, prepare: false, async: false)
          log(sql, name, binds, async: async) do
            @raw_connection.query(sql)
          end
        end

                    # @note used by columns() method
      # @param [String] table_name Name of the table
      # @return [Array<Array>] Array of column definition arrays
      def column_definitions(table_name) # :nodoc:
        sql = <<~SQL
          SELECT column_name, data_type, is_nullable, column_default 
          FROM information_schema.columns 
          WHERE table_name = ?
          ORDER BY ordinal_position
        SQL
        
        # Create bind parameter for the parameterized query
        binds = [
          ActiveRecord::Relation::QueryAttribute.new("table_name", table_name, ActiveRecord::Type::String.new)
        ]
        
        result = internal_exec_query(sql, "SCHEMA", binds)
        
        # Convert DuckDB result to array format expected by new_column_from_field
        result.rows.map { |row| [row[0], row[1], row[2], row[3]] }
      end
    end
  end
end
