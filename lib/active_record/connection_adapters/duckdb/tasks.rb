# frozen_string_literal: true

require 'fileutils'

module ActiveRecord
  module Tasks # :nodoc:
    class DuckdbDatabaseTasks # :nodoc:
      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @return [Boolean] true if using database configurations
      def self.using_database_configurations?
        true
      end

      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @param [Object] db_config Database configuration object
      # @param [String, nil] root Root directory path
      # @return [DuckdbDatabaseTasks] New database tasks instance
      def initialize(db_config, root = nil)
        @db_config = db_config
        @root = root || determine_root_directory
      end

      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @raise [ArgumentError] if no database file specified
      # @raise [ActiveRecord::DatabaseAlreadyExists] if database already exists
      # @raise [ActiveRecord::DatabaseConnectionError] if connection fails
      # @return [void]
      def create
        database_path = db_config.respond_to?(:database) ? db_config.database : db_config[:database]
        
        # Handle in-memory databases
        if database_path == ":memory:"
          # In-memory databases are created when connected to
          establish_connection
          return
        end

        # Handle file-based databases
        unless database_path.present?
          raise ArgumentError, "No database file specified. Missing argument: database"
        end

        # Convert relative paths to absolute paths
        db_file_path = if File.absolute_path?(database_path)
          database_path
        else
          File.expand_path(database_path, root)
        end

        # Check if database already exists
        if File.exist?(db_file_path)
          raise ActiveRecord::DatabaseAlreadyExists
        end

        # Create directory if it doesn't exist
        dir = File.dirname(db_file_path)
        FileUtils.mkdir_p(dir) unless File.directory?(dir)

        # Create the database by establishing a connection
        # DuckDB will create the file when we connect to it
        begin
          establish_connection
          puts "Created database '#{database_path}'"
        rescue => e
          raise ActiveRecord::DatabaseConnectionError.new(e.message)
        end
      end

      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @raise [ArgumentError] if no database file specified
      # @raise [ActiveRecord::NoDatabaseError] if database file doesn't exist
      # @raise [ActiveRecord::DatabaseConnectionError] if operation fails
      # @return [void]
      def drop
        database_path = db_config.respond_to?(:database) ? db_config.database : db_config[:database]
        
        # Handle in-memory databases
        if database_path == ":memory:"
          # In-memory databases can't be "dropped" in the traditional sense
          # Just disconnect
          begin
            connection.disconnect! if connection&.active?
          rescue
            # Ignore errors during disconnect for in-memory databases
          end
          return
        end

        # Handle file-based databases
        unless database_path.present?
          raise ArgumentError, "No database file specified. Missing argument: database"
        end

        # Convert relative paths to absolute paths
        db_file_path = if File.absolute_path?(database_path)
          database_path
        else
          File.expand_path(database_path, root)
        end

        # Disconnect from database first
        begin
          connection.disconnect! if connection&.active?
        rescue
          # Continue even if disconnect fails
        end

        # Remove the database file
        begin
          if File.exist?(db_file_path)
            FileUtils.rm(db_file_path)
            puts "Dropped database '#{database_path}'"
          else
            puts "Database '#{database_path}' does not exist"
          end
          
          # Also remove any WAL files that might exist
          wal_file = "#{db_file_path}.wal"
          FileUtils.rm(wal_file) if File.exist?(wal_file)
          
        rescue Errno::ENOENT => error
          raise ActiveRecord::NoDatabaseError.new(error.message)
        rescue => error
          raise ActiveRecord::DatabaseConnectionError.new(error.message)
        end
      end

      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @return [void]
      def purge
        drop
        create
      end

      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @return [String] Database character set encoding
      def charset
        connection.encoding rescue 'UTF-8'
      end

      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @param [String] filename Output filename for structure dump
      # @param [Array] extra_flags Additional command line flags
      # @return [void]
      def structure_dump(filename, extra_flags)
        args = []
        args.concat(Array(extra_flags)) if extra_flags
        args << (db_config.respond_to?(:database) ? db_config.database : db_config[:database])

        ignore_tables = ActiveRecord::SchemaDumper.ignore_tables
        if ignore_tables.any?
          ignore_tables = connection.data_sources.select { |table| ignore_tables.any? { |pattern| pattern === table } }
          condition = ignore_tables.map { |table| connection.quote(table) }.join(", ")
          # DuckDB provides sqlite_master for SQLite compatibility
          args << "SELECT sql FROM sqlite_master WHERE tbl_name NOT IN (#{condition}) ORDER BY tbl_name, type DESC, name"
        else
          args << ".schema"
        end
        run_cmd("duckdb", args, filename)
      end

      # @override
      # @note Implements ActiveRecord::Tasks interface method
      # @param [String] filename Input filename for structure load
      # @param [Array] extra_flags Additional command line flags
      # @return [void]
      def structure_load(filename, extra_flags)
        database_path = db_config.respond_to?(:database) ? db_config.database : db_config[:database]
        flags = extra_flags.join(" ") if extra_flags
        `duckdb #{flags} #{database_path} < "#{filename}"`
      end

      private
        attr_reader :db_config, :root

        # @note get database connection for DuckDB
        # @return [ActiveRecord::ConnectionAdapters::DuckdbAdapter] Database connection
        def connection
          # Connection pooling is less critical for DuckDB since it's an embedded database
          # with lightweight connections (no network overhead), but we maintain ActiveRecord
          # compatibility by using lease_connection when available for thread safety
          if ActiveRecord::Base.respond_to?(:lease_connection)
            ActiveRecord::Base.lease_connection
          else
            ActiveRecord::Base.connection
          end
        end

        # @note establish connection to DuckDB database
        # @param [Object] config Database configuration (defaults to db_config)
        # @return [ActiveRecord::ConnectionAdapters::DuckdbAdapter] Database connection
        def establish_connection(config = db_config)
          ActiveRecord::Base.establish_connection(config)
          connection
        end

        # @note run shell command for DuckDB operations
        # @param [String] cmd Command to run
        # @param [Array] args Command arguments
        # @param [String] out Output file path
        # @return [void]
        # @raise [RuntimeError] if command fails
        def run_cmd(cmd, args, out)
          fail run_cmd_error(cmd, args) unless Kernel.system(cmd, *args, out: out)
        end

        # @note generate error message for failed shell commands
        # @param [String] cmd Command that failed
        # @param [Array] args Command arguments
        # @return [String] Error message
        def run_cmd_error(cmd, args)
          msg = +"failed to execute:\n"
          msg << "#{cmd} #{args.join(' ')}\n\n"
          msg << "Please check the output above for any errors and make sure that `#{cmd}` is installed in your PATH and has proper permissions.\n\n"
          msg
        end

        # @note determine root directory for database files
        # @return [String] Root directory path
        def determine_root_directory
          # Try different ways to determine the root directory
          if defined?(Rails) && Rails.respond_to?(:root) && Rails.root
            Rails.root.to_s
          elsif defined?(Rails) && Rails.respond_to?(:application) && Rails.application&.config&.root
            Rails.application.config.root.to_s
          elsif ENV['RAILS_ROOT']
            ENV['RAILS_ROOT']
          else
            # Fall back to current working directory
            Dir.pwd
          end
        end
    end
  end
end
