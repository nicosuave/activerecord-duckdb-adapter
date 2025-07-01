# frozen_string_literal: true

require 'active_record'
require "activerecord_duckdb_adapter/version"
require "active_record/connection_adapters/duckdb_adapter"

# Register the adapter with ActiveRecord
# Always define the connection method to ensure proper handling of configuration
module ActiveRecord
  module ConnectionHandling # :nodoc:
    def duckdb_connection(config)
      # Create the connection first using the adapter's new_client method
      connection = ActiveRecord::ConnectionAdapters::DuckdbAdapter.new_client(config)
      # Then create the adapter with the connection and config
      ActiveRecord::ConnectionAdapters::DuckdbAdapter.new(connection, nil, config)
    end
  end
end

# Also register with the new method if available
if ActiveRecord::ConnectionAdapters.respond_to?(:register)
  ActiveRecord::ConnectionAdapters.register("duckdb", "ActiveRecord::ConnectionAdapters::DuckdbAdapter", "active_record/connection_adapters/duckdb_adapter")
end

# Register database tasks (this might not be needed in newer versions)
begin
  ActiveRecord::Tasks::DatabaseTasks.register_task(/duckdb/, "ActiveRecord::Tasks::DuckdbDatabaseTasks")
rescue NoMethodError
  # Ignore if the method doesn't exist in this ActiveRecord version
end
