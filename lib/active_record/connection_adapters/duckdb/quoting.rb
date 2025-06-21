# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Duckdb
      module Quoting # :nodoc:
        extend ActiveSupport::Concern

        QUOTED_COLUMN_NAMES = Concurrent::Map.new # :nodoc:
        QUOTED_TABLE_NAMES = Concurrent::Map.new # :nodoc:

        module ClassMethods # :nodoc:
          # @note regex pattern for column name matching
          # @return [Regexp] Regular expression for matching column names
          def column_name_matcher
            /
              \A
              (
                (?:
                  # "table_name"."column_name" | function(one or no argument)
                  ((?:\w+\.|"\w+"\.)?(?:\w+|"\w+") | \w+\((?:|\g<2>)\))
                )
                (?:(?:\s+AS)?\s+(?:\w+|"\w+"))?
              )
              (?:\s*,\s*\g<1>)*
              \z
            /ix
          end

          # @note regex pattern for column name with order matching
          # @return [Regexp] Regular expression for matching column names with order
          def column_name_with_order_matcher
            /
              \A
              (
                (?:
                  # "table_name"."column_name" | function(one or no argument)
                  ((?:\w+\.|"\w+"\.)?(?:\w+|"\w+") | \w+\((?:|\g<2>)\))
                )
                (?:\s+COLLATE\s+(?:\w+|"\w+"))?
                (?:\s+ASC|\s+DESC)?
              )
              (?:\s*,\s*\g<1>)*
              \z
            /ix
          end

          # @override
          # @note Implements AbstractAdapter interface method
          # @param [String, Symbol] name Column name to quote
          # @return [String] Quoted column name
          def quote_column_name(name)
            QUOTED_COLUMN_NAMES[name] ||= %Q("#{name.to_s.gsub('"', '""')}").freeze
          end

          # @override
          # @note Implements AbstractAdapter interface method
          # @param [String, Symbol] name Table name to quote
          # @return [String] Quoted table name
          def quote_table_name(name)
            QUOTED_TABLE_NAMES[name] ||= %Q("#{name.to_s.gsub('"', '""').gsub(".", "\".\"")}").freeze
          end
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] s String to quote
        # @return [String] Quoted string with escaped single quotes
        def quote_string(s)
          s.gsub("'", "''")  # Escape single quotes by doubling them
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] table Table name (unused)
        # @param [String] attr Attribute name
        # @return [String] Quoted column name
        def quote_table_name_for_assignment(table, attr)
          quote_column_name(attr)
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [Time] value Time value to quote
        # @return [String] Quoted time string
        def quoted_time(value)
          value = value.change(year: 2000, month: 1, day: 1)
          quoted_date(value).sub(/\A\d\d\d\d-\d\d-\d\d /, "2000-01-01 ")
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [String] value Binary value to quote
        # @return [String] Quoted binary string in hex format
        def quoted_binary(value)
          "x'#{value.hex}'"
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @return [String] Quoted true value for DuckDB
        def quoted_true
          "1"
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @return [Integer] Unquoted true value for DuckDB
        def unquoted_true
          1
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @return [String] Quoted false value for DuckDB
        def quoted_false
          "0"
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @return [Integer] Unquoted false value for DuckDB
        def unquoted_false
          0
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [Object] value Default value to quote
        # @param [ActiveRecord::ConnectionAdapters::Column] column Column object
        # @return [String] Quoted default expression
        def quote_default_expression(value, column) # :nodoc:
          if value.is_a?(Proc)
            value = value.call
            # Don't wrap nextval() calls in extra parentheses
            value
          elsif value.is_a?(String) && value.match?(/\Anextval\(/i)
            # Handle nextval function calls for sequences - don't quote them
            value
          else
            super
          end
        end

        # @override
        # @note Implements AbstractAdapter interface method
        # @param [Object] value Value to type cast
        # @return [Object] Type-cast value
        def type_cast(value) # :nodoc:
          case value
          when BigDecimal, Rational
            value.to_f
          when String
            if value.encoding == Encoding::ASCII_8BIT
              super(value.encode(Encoding::UTF_8))
            else
              super
            end
          else
            super
          end
        end
      end
    end
  end
end
