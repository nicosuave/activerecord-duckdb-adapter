# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module DatabaseStatements

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] sql SQL to execute
        # @param [String, nil] name Query name for logging
        # @param [Boolean] allow_retry Whether to allow retry on failure
        # @return [Object] Query result
        def execute(sql, name = nil, allow_retry: false)
          internal_execute(sql, name, allow_retry: allow_retry)
        end

        # @note internal execution wrapper for DuckDB
        # @param [String] sql SQL to execute
        # @param [String] name Query name for logging
        # @param [Array] binds Bind parameters
        # @param [Boolean] prepare Whether to prepare statement
        # @param [Boolean] async Whether to execute asynchronously
        # @param [Boolean] allow_retry Whether to allow retry on failure
        # @param [Boolean] materialize_transactions Whether to materialize transactions
        # @return [Object] Query result
        def internal_execute(sql, name = "SQL", binds = [], prepare: false, async: false, allow_retry: false, &block)
          raw_execute(sql, name, binds, prepare: prepare, async: async, allow_retry: allow_retry, &block)
        end

        # @override
        # @note Implements AbstractAdapter interface method - These methods need to return integers for update_all and delete_all
        # @param [Object] arel Arel object or SQL string
        # @param [String, nil] name Query name for logging
        # @param [Array] binds Bind parameters
        # @return [Integer] Number of affected rows
        def update(arel, name = nil, binds = [])
          sql, binds = to_sql_and_binds(arel, binds)
          result = internal_execute(sql, name, binds)
          extract_row_count(result, sql)
        end

        # @override
        # @note Implements AbstractAdapter interface method - These methods need to return integers for update_all and delete_all
        # @param [Object] arel Arel object or SQL string
        # @param [String, nil] name Query name for logging
        # @param [Array] binds Bind parameters
        # @return [Integer] Number of affected rows
        def delete(arel, name = nil, binds = [])
          sql, binds = to_sql_and_binds(arel, binds)
          result = internal_execute(sql, name, binds)
          extract_row_count(result, sql)
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] sql SQL to execute
        # @param [String] name Query name for logging
        # @param [Array] binds Bind parameters
        # @param [Boolean] prepare Whether to prepare statement
        # @param [Boolean] async Whether to execute asynchronously
        # @param [Boolean] allow_retry Whether to allow retry on failure
        # @param [Boolean] materialize_transactions Whether to materialize transactions
        # @return [ActiveRecord::Result] Query result as ActiveRecord::Result
        def internal_exec_query(sql, name = "SQL", binds = [], prepare: false, async: false, allow_retry: false)
          result = internal_execute(sql, name, binds, prepare: prepare, async: async, allow_retry: allow_retry)
          
          # Convert DuckDB result to ActiveRecord::Result
          raw_cols = result.columns || []
          cols = raw_cols.map { |col| col.respond_to?(:name) ? col.name : col.to_s }
          rows = result.to_a || []
          
          ActiveRecord::Result.new(cols, rows)
        end

        # @note raw execution for DuckDB
        # @param [String] sql SQL to execute
        # @param [String, nil] name Query name for logging
        # @param [Array] binds Bind parameters
        # @param [Boolean] prepare Whether to prepare statement
        # @param [Boolean] async Whether to execute asynchronously
        # @param [Boolean] allow_retry Whether to allow retry on failure
        # @param [Boolean] materialize_transactions Whether to materialize transactions
        # @param [Boolean] batch Whether to execute in batch mode
        # @return [Object] Query result
        def raw_execute(sql, name = nil, binds = [], prepare: false, async: false, allow_retry: false, batch: false)
          type_casted_binds = type_casted_binds(binds)
          log(sql, name, binds, type_casted_binds, async: async) do |notification_payload|
            # Rails 6.1 doesn't have with_raw_connection, use @raw_connection directly
            perform_query(@raw_connection, sql, binds, type_casted_binds, prepare: prepare, notification_payload: notification_payload, batch: batch)
          end
        end

        # @note DuckDB-specific query execution
        # @param [Object] raw_connection Raw database connection
        # @param [String] sql SQL to execute
        # @param [Array] binds Bind parameters
        # @param [Array] type_casted_binds Type-casted bind parameters
        # @param [Boolean] prepare Whether to prepare statement
        # @param [Object] notification_payload Notification payload for logging
        # @param [Boolean] batch Whether to execute in batch mode
        # @return [Object] Query result
        def perform_query(raw_connection, sql, binds, type_casted_binds, prepare:, notification_payload:, batch: false)
          # Use DuckDB's native parameter binding - clean and secure
          bind_values = extract_bind_values(type_casted_binds, binds)
          
          if bind_values&.any?
            @raw_connection.query(sql, *bind_values)
          else
            @raw_connection.query(sql)
          end
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] sql SQL to execute
        # @param [String, nil] name Query name for logging
        # @return [Object] Query result
        def query(sql, name = nil)
          result = internal_execute(sql, name)
          result
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] sql SQL to explain
        # @return [String] Pretty-printed explanation
        def explain(sql)
          result = internal_exec_query(sql, "EXPLAIN")
          Duckdb::ExplainPrettyPrinter.new.pp(result)
        end

        # @override
        # @note Implements AbstractAdapter interface method - Executes an INSERT statement and returns the ID of the newly inserted record
        # @param [String] sql INSERT SQL to execute
        # @param [String, nil] name Query name for logging
        # @param [Array] binds Bind parameters
        # @param [String, nil] pk Primary key column name
        # @param [String, nil] sequence_name Sequence name for auto-increment
        # @param [String, nil] returning RETURNING clause
        # @return [ActiveRecord::Result] Result containing inserted ID
        def exec_insert(sql, name = nil, binds = [], pk = nil, sequence_name = nil, returning: nil)
          if pk && supports_insert_returning?
            # Use INSERT...RETURNING to get the inserted ID
            returning_sql = sql.sub(/\bINSERT\b/i, "INSERT").concat(" RETURNING #{quote_column_name(pk)}")
            internal_exec_query(returning_sql, name, binds)
          else
            # Regular insert - return result from internal_execute
            internal_execute(sql, name, binds)
            # Return an empty result since we don't have the ID
            ActiveRecord::Result.new([], [])
          end
        end

        private

        # @note extract row count from DuckDB result
        # @param [Object] result Query result
        # @param [String] sql Original SQL query
        # @return [Integer] Number of affected rows
        def extract_row_count(result, sql)
          if result.respond_to?(:to_a) 
            rows = result.to_a
            if rows.length == 1 && rows[0].length == 1
              count = rows[0][0]
              return count.is_a?(Integer) ? count : count.to_i
            end
          end
          0
        end

        # @note convert Arel to SQL string
        # @param [Object] arel Arel object or SQL string
        # @param [Array] binds Bind parameters (unused)
        # @return [String] SQL string
        def to_sql(arel, binds = [])
          if arel.respond_to?(:to_sql)
            arel.to_sql
          else
            arel.to_s
          end
        end

        # @note Convert Arel to SQL and extract bind parameters
        # @param [Object] arel_or_sql_string Arel object or SQL string
        # @param [Array] binds Bind parameters
        # @param [Array] args Additional arguments
        # @return [Array] Array containing SQL string and bind parameters
        def to_sql_and_binds(arel_or_sql_string, binds = [], *args)
          # For simple cases, delegate to parent implementation if it exists
          if defined?(super)
            begin
              return super(arel_or_sql_string, binds, *args)
            rescue NoMethodError
              # Fall through to our implementation
            end
          end
          
          # Our simplified implementation for basic cases
          if arel_or_sql_string.respond_to?(:ast)
            # For Arel objects, visit the AST to get SQL and collect binds
            visitor = arel_visitor
            collector = Arel::Collectors::SQLString.new
            visitor.accept(arel_or_sql_string.ast, collector)
            sql = collector.value
            
            # Extract binds from the visitor if it collected them
            visitor_binds = if visitor.respond_to?(:binds)
              visitor.binds
            else
              []
            end
            
            result = [sql, binds + visitor_binds]
            # Add any additional args back to maintain signature compatibility
            args.each { |arg| result << arg }
            result
          elsif arel_or_sql_string.respond_to?(:to_sql)
            # For objects with to_sql method, use it directly
            result = [arel_or_sql_string.to_sql, binds]
            args.each { |arg| result << arg }
            result
          else
            # For plain strings, return as-is
            result = [arel_or_sql_string.to_s, binds]
            args.each { |arg| result << arg }
            result
          end
        end

        # @note get Arel visitor for SQL generation
        # @return [Object] Arel visitor instance
        def arel_visitor
          # Rails 6.1 accesses arel visitor differently
          if respond_to?(:schema_cache) && schema_cache
            schema_cache.arel_visitor
          else
            Arel::Visitors::ToSql.new(self)
          end
        end

        # @override
        # @note Implements AbstractAdapter interface method - ActiveRecord calls this method to get properly type-cast bind parameters
        # @param [Array] binds Array of bind parameters
        # @return [Array] Array of type-cast values
        def type_casted_binds(binds)
          if binds.empty?
            []
          else
            binds.map do |attr|
              if attr.respond_to?(:value_for_database)
                value = attr.value_for_database
                # Handle ActiveRecord timestamp value objects that DuckDB doesn't understand
                if value.class.name == 'ActiveRecord::Type::Time::Value'
                  # Convert to a proper Time object that DuckDB can handle
                  Time.parse(value.to_s)
                else
                  value
                end
              else
                type_cast(attr)
              end
            end
          end
        end

        # @note extract bind values for DuckDB parameter binding
        # @param [Array] type_casted_binds Type-casted bind parameters
        # @param [Array] binds Original bind parameters
        # @return [Array, nil] Array of bind values or nil if none
        def extract_bind_values(type_casted_binds, binds)
          # Prefer type_casted_binds as they are pre-processed by ActiveRecord
          return type_casted_binds if type_casted_binds&.any?
          
          # Extract values from bind objects if no type_casted_binds
          return nil unless binds&.any?
          
          binds.map do |bind|
            case bind
            when Array
              # [column, value] format
              bind[1]
            else
              # Extract value from attribute objects or use direct value
              bind.respond_to?(:value) ? bind.value : bind
            end
          end
        end

      end
    end
  end
end
