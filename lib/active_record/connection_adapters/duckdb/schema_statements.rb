# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module SchemaStatements # :nodoc:

        # @override
        # @note Implements AbstractAdapter interface method - Returns an array of indexes for the given table
        # @param [String] table_name Name of the table
        # @return [Array] Array of index objects (currently empty)
        def indexes(table_name)
          # DuckDB uses duckdb_indexes() function for index information
          # Since we may not have access to the duckdb_indexes() function in all contexts,
          # we'll return an empty array for now
          # TODO: Implement proper index querying when DuckDB Ruby driver supports it
          []
        end

        # @override
        # @note Implements AbstractAdapter interface method - Checks to see if the data source +name+ exists on the database
        # @example
        #   data_source_exists?(:ebooks)
        # @param [String, Symbol] name Name of the data source
        # @return [Boolean] true if data source exists, false otherwise
        def data_source_exists?(name)
          return false unless name.present?
          data_sources.include?(name.to_s)
        end

        # @note generates SQL for data source queries
        # @param [String, nil] name Data source name
        # @param [String, nil] type Data source type
        # @return [String] SQL query string
        def data_source_sql(name = nil, type: nil)
          scope = quoted_scope(name, type: type)

          sql = +"SELECT table_name FROM information_schema.tables"
          sql << " WHERE table_schema = #{scope[:schema]}"
          if scope[:type] || scope[:name]
            conditions = []
            conditions << "table_type = #{scope[:type]}" if scope[:type]
            conditions << "table_name = #{scope[:name]}" if scope[:name]
            sql << " AND #{conditions.join(" AND ")}"
          end
          sql
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] table_name Name of the table
        # @return [Boolean] true if table exists, false otherwise
        def table_exists?(table_name)
          return false unless table_name.present?
          
          sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = #{quote(table_name.to_s)} AND table_schema = 'main'"
          query_value(sql, "SCHEMA") > 0
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] table_name Name of the table to create
        # @param [Symbol, String, Boolean] id Primary key configuration
        # @param [String, nil] primary_key Primary key column name
        # @param [Boolean, nil] force Whether to drop existing table
        # @param [Hash] options Additional table options
        # @return [ActiveRecord::ConnectionAdapters::TableDefinition] Table definition
        def create_table(table_name, id: :primary_key, primary_key: nil, force: nil, **options)
          if force
            drop_table(table_name, if_exists: true, **options)
          end

          td = create_table_definition(table_name, **options)
          
          # Add primary key unless explicitly disabled
          if id != false
            case id
            when :primary_key, true
              # DuckDB native auto-increment: create sequence then use it as column default
              pk_name = primary_key || default_primary_key_name
              
              # Add primary key column with auto-increment via sequence
              # This follows DuckDB's documented pattern for auto-increment primary keys
              add_auto_increment_primary_key(td, table_name, pk_name)
            when Symbol, String
              # For other primary key types, delegate to parent behavior
              td.primary_key id, primary_key, **options
            end
          end

          yield td if block_given?

          if supports_comments? && !supports_comments_in_create?
            change_table_comment(table_name, options[:comment]) if options[:comment].present?
          end

          execute schema_creation.accept(td)
          td
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @return [Array<String>] Array of data source names
        def data_sources
          sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
          execute(sql).map { |row| row[0] }
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] table_name Name of the table to drop
        # @param [Boolean] if_exists Whether to use IF EXISTS clause
        # @param [Hash] options Additional drop options
        # @return [void]
        def drop_table(table_name, if_exists: false, **options)
          sql = +"DROP TABLE"
          sql << " IF EXISTS" if if_exists
          sql << " #{quote_table_name(table_name)}"
          execute sql
        end

        private
          # @note creates new column from DuckDB field information
          # @param [String] table_name Name of the table
          # @param [Array] field Field information array
          # @param [Object, nil] type_metadata Type metadata object
          # @return [ActiveRecord::ConnectionAdapters::Column] Column object
          def new_column_from_field(table_name, field, type_metadata = nil)
            # DuckDB information_schema returns: column_name, data_type, is_nullable, column_default
            column_name, data_type, is_nullable, column_default = field
            
            # For auto-increment columns, DuckDB might return internal default expressions
            # that we don't want to expose as ActiveRecord column defaults
            if column_default && (column_default.match?(/\Anextval\(/i) || column_default.match?(/\Aautoincrement/i))
              column_default = nil
            end
            
            # Convert DuckDB data types to ActiveRecord types
            sql_type_metadata = type_metadata || fetch_type_metadata(data_type)
            
            ActiveRecord::ConnectionAdapters::Column.new(
              column_name, 
              column_default, 
              sql_type_metadata, 
              is_nullable == 'YES'
            )
          end

          # @note converts DuckDB data types to ActiveRecord type metadata
          # @param [String] sql_type DuckDB SQL type string
          # @return [ActiveRecord::ConnectionAdapters::SqlTypeMetadata] Type metadata object
          def fetch_type_metadata(sql_type)
            # Convert DuckDB data types to ActiveRecord types
            cast_type = case sql_type.downcase
                       when /^integer/i
                         :integer
                       when /^bigint/i
                         :bigint
                       when /^varchar/i, /^text/i
                         :string
                       when /^decimal/i, /^numeric/i
                         :decimal
                       when /^real/i, /^double/i, /^float/i
                         :float
                       when /^boolean/i
                         :boolean
                       when /^date$/i
                         :date
                       when /^time/i
                         :time
                       when /^timestamp/i
                         :datetime
                       when /^blob/i
                         :binary
                       when /^uuid/i
                         :string # DuckDB UUID as string for now
                       else
                         :string # fallback
                       end
            
            # Create type metadata
            ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(
              sql_type: sql_type,
              type: cast_type
            )
          end

          # @note creates quoted scope for SQL queries
          # @param [String, nil] name Table or data source name
          # @param [String, nil] type Data source type
          # @return [Hash] Hash containing quoted scope elements
          def quoted_scope(name = nil, type: nil)
            schema, name = extract_schema_qualified_name(name)
            scope = {}
            scope[:schema] = schema ? quote(schema) : "'main'"
            scope[:name] = quote(name) if name
            scope[:type] = quote(type) if type
            scope
          end

          # @note extracts schema and name from qualified name string
          # @param [String, Symbol] string Qualified name string
          # @return [Array<String, nil>] Array containing schema and name
          def extract_schema_qualified_name(string)
            schema, name = string.to_s.scan(/[^`.\s]+|`[^`]*`/)
            schema, name = nil, schema unless name
            [schema, name]
          end

          # @note creates table definition for create_table
          # @param [String] table_name Name of the table
          # @param [Hash] options Table creation options
          # @return [ActiveRecord::ConnectionAdapters::TableDefinition] Table definition object
          def create_table_definition(table_name, **options)
            ActiveRecord::ConnectionAdapters::TableDefinition.new(
              self,
              table_name,
              **options
            )
          end

          # @note returns default primary key name
          # @return [String] Default primary key column name
          def default_primary_key_name
            "id"
          end

          # @note DuckDB doesn't support table comments yet
          # @return [Boolean] false, as DuckDB doesn't support table comments
          def supports_comments?
            false
          end

          # @note DuckDB doesn't support comments in CREATE statements
          # @return [Boolean] false, as DuckDB doesn't support comments in CREATE
          def supports_comments_in_create?
            false
          end

          # @note returns schema creation helper
          # @return [ActiveRecord::ConnectionAdapters::SchemaCreation] Schema creation helper
          def schema_creation
            ActiveRecord::ConnectionAdapters::SchemaCreation.new(self)
          end

          # @note adds auto-increment primary key using DuckDB's native sequence approach
          # @param [ActiveRecord::ConnectionAdapters::TableDefinition] td Table definition
          # @param [String] table_name Name of the table
          # @param [String] pk_name Primary key column name
          # @return [void]
          def add_auto_increment_primary_key(td, table_name, pk_name)
            sequence_name = "#{table_name}_#{pk_name}_seq"
            
            # Use DuckDB's native sequence approach - this is the official DuckDB pattern
            # Create sequence first, then reference it in the column default
            execute "CREATE SEQUENCE IF NOT EXISTS #{quote_table_name(sequence_name)}"
            
            # Add the column with nextval() as default - DuckDB's standard auto-increment pattern
            td.column pk_name, :bigint, primary_key: true, default: -> { "nextval('#{sequence_name}')" }
          end


      end
    end
  end
end
