# frozen_string_literal: true

require "test_helper"

class ColumnsTest < TestCase
  def setup
    @connection = ActiveRecord::Base.connection
  end

  def test_columns_basic_functionality
    # Test with existing 'authors' table
    columns = @connection.columns('authors')
    
    assert_instance_of Array, columns
    assert columns.length > 0, "Should have at least one column"
    
    # Check that we get Column objects
    columns.each do |column|
      assert_instance_of ActiveRecord::ConnectionAdapters::Column, column
      assert_respond_to column, :name
      assert_respond_to column, :type
      assert_respond_to column, :sql_type
    end
  end

  def test_columns_authors_table
    columns = @connection.columns('authors')
    column_names = columns.map(&:name)
    
    # Authors table should have 'id' and 'name' columns
    assert_includes column_names, 'id'
    assert_includes column_names, 'name'
    
    # Find the ID column and check its properties
    id_column = columns.find { |col| col.name == 'id' }
    assert_not_nil id_column, "Should have id column"
    assert_equal :bigint, id_column.type
  end

  def test_columns_posts_table
    columns = @connection.columns('posts')
    column_names = columns.map(&:name)
    
    # Posts table should have expected columns
    expected_columns = ['id', 'author_id', 'title', 'body', 'count', 'enabled']
    expected_columns.each do |col_name|
      assert_includes column_names, col_name, "Should have #{col_name} column"
    end
  end

  def test_column_definitions_basic_functionality
    # Test the private column_definitions method through columns
    columns = @connection.columns('authors')
    
    # Should return proper column information
    assert columns.length >= 2, "Should have at least id and name columns"
    
    # Check column types are detected correctly
    id_column = columns.find { |col| col.name == 'id' }
    name_column = columns.find { |col| col.name == 'name' }
    
    assert_equal :bigint, id_column.type
    assert_equal :string, name_column.type
  end

  def test_columns_nonexistent_table
    # Test with a table that doesn't exist
    columns = @connection.columns('nonexistent_table')
    
    # Should return empty array for non-existent tables
    assert_equal [], columns
    assert_instance_of Array, columns
  end

  def test_columns_sql_injection_protection
    # Test that SQL injection is prevented in column_definitions
    malicious_table_name = "authors'; DROP TABLE authors; --"
    
    # Should not raise exception and should return empty array (table doesn't exist)
    columns = @connection.columns(malicious_table_name)
    assert_equal [], columns
    
    # Verify authors table still exists
    assert @connection.table_exists?('authors'), "Authors table should still exist after SQL injection attempt"
  end

  def test_columns_with_custom_table
    # Create a test table with various column types
    @connection.execute(<<~SQL)
      CREATE TABLE test_columns_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INTEGER,
        salary DECIMAL(10,2),
        is_active BOOLEAN DEFAULT true,
        created_at TIMESTAMP,
        notes TEXT
      )
    SQL

    columns = @connection.columns('test_columns_table')
    column_names = columns.map(&:name)
    
    # Check all columns are detected
    expected_columns = ['id', 'name', 'age', 'salary', 'is_active', 'created_at', 'notes']
    expected_columns.each do |col_name|
      assert_includes column_names, col_name, "Should have #{col_name} column"
    end
    
    # Check column types are correctly detected
    columns_by_name = columns.index_by(&:name)
    
    assert_equal :integer, columns_by_name['id'].type
    assert_equal :string, columns_by_name['name'].type
    assert_equal :integer, columns_by_name['age'].type
    assert_equal :decimal, columns_by_name['salary'].type
    assert_equal :boolean, columns_by_name['is_active'].type
    assert_equal :time, columns_by_name['created_at'].type  # DuckDB TIMESTAMP maps to :time
    assert_equal :string, columns_by_name['notes'].type  # TEXT maps to string
    
  ensure
    @connection.execute("DROP TABLE IF EXISTS test_columns_table")
  end

  def test_columns_null_and_default_values
    # Create a test table with various null and default constraints
    @connection.execute(<<~SQL)
      CREATE TABLE test_null_defaults (
        id INTEGER PRIMARY KEY,
        required_field VARCHAR(50) NOT NULL,
        optional_field VARCHAR(50),
        default_field VARCHAR(50) DEFAULT 'default_value',
        default_number INTEGER DEFAULT 42
      )
    SQL

    columns = @connection.columns('test_null_defaults')
    columns_by_name = columns.index_by(&:name)
    
    # Check null constraints
    assert_equal false, columns_by_name['required_field'].null, "required_field should not allow null"
    assert_equal true, columns_by_name['optional_field'].null, "optional_field should allow null"
    
    # Check default values (DuckDB returns defaults as strings)
    assert_equal "'default_value'", columns_by_name['default_field'].default
    assert_equal "42", columns_by_name['default_number'].default
    
  ensure
    @connection.execute("DROP TABLE IF EXISTS test_null_defaults")
  end

  def test_columns_ordering
    # Create a table with specific column order
    @connection.execute(<<~SQL)
      CREATE TABLE test_column_order (
        third_column INTEGER,
        first_column VARCHAR(50),
        second_column BOOLEAN
      )
    SQL

    columns = @connection.columns('test_column_order')
    column_names = columns.map(&:name)
    
    # Columns should be returned in the order they were defined
    expected_order = ['third_column', 'first_column', 'second_column']
    assert_equal expected_order, column_names, "Columns should be in definition order"
    
  ensure
    @connection.execute("DROP TABLE IF EXISTS test_column_order")
  end

  def test_columns_integration_with_activerecord
    # Test that ActiveRecord models can use the columns method
    require "models/author"
    require "models/post"
    
    # ActiveRecord should be able to get column information
    author_columns = Author.columns
    post_columns = Post.columns
    
    assert author_columns.length > 0, "Author should have columns"
    assert post_columns.length > 0, "Post should have columns"
    
    # Check that ActiveRecord can find specific columns
    assert Author.column_names.include?('id')
    assert Author.column_names.include?('name')
    assert Post.column_names.include?('id')
    assert Post.column_names.include?('title')
  end

  def test_columns_performance
    # Basic performance test - should complete quickly
    start_time = Time.now
    
    50.times do
      @connection.columns('authors')
    end
    
    end_time = Time.now
    duration = end_time - start_time
    
    # Should complete 50 calls in less than 1 second
    assert duration < 1.0, "columns method is too slow: #{duration} seconds for 50 calls"
  end

  def test_columns_case_sensitivity
    # DuckDB table names are case-sensitive
    columns_lower = @connection.columns('authors')
    columns_upper = @connection.columns('AUTHORS')
    
    # Lowercase should work (table exists)
    assert columns_lower.length > 0, "Should find columns for 'authors'"
    # Uppercase should return empty (table doesn't exist)
    assert_equal [], columns_upper, "Should not find columns for 'AUTHORS'"
  end

  def test_columns_with_schema_changes
    # Test that columns method works correctly after schema changes
    @connection.execute(<<~SQL)
      CREATE TABLE test_schema_evolution (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100)
      )
    SQL

    # Initial columns
    columns = @connection.columns('test_schema_evolution')
    assert_equal 2, columns.length
    assert_equal ['id', 'name'], columns.map(&:name)
    
    # Add a column
    @connection.execute("ALTER TABLE test_schema_evolution ADD COLUMN email VARCHAR(255)")
    
    # Should reflect new column
    columns = @connection.columns('test_schema_evolution')
    assert_equal 3, columns.length
    assert_equal ['id', 'name', 'email'], columns.map(&:name)
    
  ensure
    @connection.execute("DROP TABLE IF EXISTS test_schema_evolution")
  end
end 