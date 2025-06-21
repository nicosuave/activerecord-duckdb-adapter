# frozen_string_literal: true

require 'active_record'
require "activerecord_duckdb_adapter/version"
require "active_record/connection_adapters/duckdb_adapter"

# Register the adapter with ActiveRecord
if ActiveRecord::ConnectionAdapters.respond_to?(:register)
  ActiveRecord::ConnectionAdapters.register("duckdb", "ActiveRecord::ConnectionAdapters::DuckdbAdapter", "active_record/connection_adapters/duckdb_adapter")
else
  # For older ActiveRecord versions, define the connection method manually
  module ActiveRecord
    module ConnectionHandling # :nodoc:
      def duckdb_connection(config)
        ActiveRecord::ConnectionAdapters::DuckdbAdapter.new(config)
      end
    end
  end
end

# Register database tasks (this might not be needed in newer versions)
begin
  ActiveRecord::Tasks::DatabaseTasks.register_task(/duckdb/, "ActiveRecord::Tasks::DuckdbDatabaseTasks")
rescue NoMethodError
  # Ignore if the method doesn't exist in this ActiveRecord version
end
