# frozen_string_literal: true

require "test_helper"

class DatabaseTasksTest < TestCase
  def setup
    @test_db_path = 'test/test_database_tasks.duckdb'
    @test_config = {
      adapter: 'duckdb',
      database: @test_db_path
    }
    @tasks = ActiveRecord::Tasks::DuckdbDatabaseTasks.new(@test_config)
    
    # Store original connection config to restore later
    @original_config = ActiveRecord::Base.connection_db_config.configuration_hash
    
    # Clean up any existing test databases and directories
    cleanup_all_test_files
  end

  def teardown
    # Restore original connection to main test database
    ActiveRecord::Base.establish_connection(@original_config)
    cleanup_all_test_files
  end

  def test_create_file_database
    refute File.exist?(@test_db_path), "Test database should not exist initially"
    
    @tasks.create
    
    assert File.exist?(@test_db_path), "Database file should be created"
    assert File.size(@test_db_path) > 0, "Database file should not be empty"
  end

  def test_create_database_already_exists
    # Create database first
    @tasks.create
    assert File.exist?(@test_db_path), "Database should be created"
    
    # Try to create again - should raise exception
    assert_raises ActiveRecord::DatabaseAlreadyExists do
      @tasks.create
    end
  end

  def test_drop_existing_database
    # Create database first
    @tasks.create
    assert File.exist?(@test_db_path), "Database should be created"
    
    # Drop it
    @tasks.drop
    
    refute File.exist?(@test_db_path), "Database file should be removed"
  end

  def test_drop_nonexistent_database
    refute File.exist?(@test_db_path), "Test database should not exist"
    
    # Should not raise exception when dropping non-existent database
    assert_nothing_raised do
      @tasks.drop
    end
  end

  def test_purge_database
    # Store original connection config
    original_config = ActiveRecord::Base.connection_db_config.configuration_hash
    
    # Create database first
    @tasks.create
    assert File.exist?(@test_db_path), "Database should be created"
    
    # Add some data to make sure purge recreates fresh database
    ActiveRecord::Base.establish_connection(@test_config)
    ActiveRecord::Base.connection.execute('CREATE TABLE test_purge (id INTEGER)')
    ActiveRecord::Base.connection.execute('INSERT INTO test_purge VALUES (1)')
    
    # Purge should drop and recreate
    @tasks.purge
    
    assert File.exist?(@test_db_path), "Database should exist after purge"
    
    # Reconnect and verify table is gone
    ActiveRecord::Base.establish_connection(@test_config)
    tables = ActiveRecord::Base.connection.execute("SELECT name FROM sqlite_master WHERE type='table'").map { |row| row[0] }
    refute_includes tables, 'test_purge', "Table should not exist after purge"
    
  ensure
    # Restore original connection for other tests
    ActiveRecord::Base.establish_connection(original_config) if original_config
  end

  def test_create_in_memory_database
    # Store original connection config
    original_config = ActiveRecord::Base.connection_db_config.configuration_hash
    
    in_memory_config = {
      adapter: 'duckdb',
      database: ':memory:'
    }
    tasks = ActiveRecord::Tasks::DuckdbDatabaseTasks.new(in_memory_config)
    
    # Should not raise exception
    assert_nothing_raised do
      tasks.create
    end
    
    # Should be able to establish connection
    ActiveRecord::Base.establish_connection(in_memory_config)
    # Connection test - in-memory databases work differently
    assert_nothing_raised { ActiveRecord::Base.connection.execute("SELECT 1") }
    
  ensure
    # Restore original connection for other tests
    ActiveRecord::Base.establish_connection(original_config) if original_config
  end

  def test_drop_in_memory_database
    # Store original connection config
    original_config = ActiveRecord::Base.connection_db_config.configuration_hash
    
    in_memory_config = {
      adapter: 'duckdb',
      database: ':memory:'
    }
    tasks = ActiveRecord::Tasks::DuckdbDatabaseTasks.new(in_memory_config)
    
    # Create and connect
    tasks.create
    ActiveRecord::Base.establish_connection(in_memory_config)
    
    # Drop should not raise exception
    assert_nothing_raised do
      tasks.drop
    end
    
  ensure
    # Restore original connection for other tests
    ActiveRecord::Base.establish_connection(original_config) if original_config
  end

  def test_create_with_directory_creation
    nested_path = 'test/deep/nested/directory/test.duckdb'
    nested_config = {
      adapter: 'duckdb',
      database: nested_path
    }
    tasks = ActiveRecord::Tasks::DuckdbDatabaseTasks.new(nested_config)
    
    # Ensure clean state
    FileUtils.rm_rf('test/deep') if Dir.exist?('test/deep')
    
    begin
      refute File.exist?(nested_path), "Nested database should not exist initially"
      refute Dir.exist?('test/deep'), "Nested directory should not exist initially"
      
      tasks.create
      
      assert File.exist?(nested_path), "Nested database should be created"
      assert Dir.exist?('test/deep/nested/directory'), "Nested directories should be created"
      
    ensure
      # Cleanup
      FileUtils.rm_rf('test/deep') if Dir.exist?('test/deep')
    end
  end

  def test_database_tasks_with_relative_paths
    relative_config = {
      adapter: 'duckdb',
      database: './test/relative_test.duckdb'
    }
    tasks = ActiveRecord::Tasks::DuckdbDatabaseTasks.new(relative_config)
    
    # Ensure clean state
    FileUtils.rm_f('test/relative_test.duckdb')
    refute File.exist?('test/relative_test.duckdb'), "Relative path database should not exist initially"
    
    begin
      tasks.create
      assert File.exist?('test/relative_test.duckdb'), "Relative path database should be created"
      
      tasks.drop
      refute File.exist?('test/relative_test.duckdb'), "Relative path database should be dropped"
      
    ensure
      FileUtils.rm_f('test/relative_test.duckdb')
    end
  end

  private

  def cleanup_all_test_files
    # Clean up main test database
    FileUtils.rm_f(@test_db_path) if File.exist?(@test_db_path)
    FileUtils.rm_f("#{@test_db_path}.wal") if File.exist?("#{@test_db_path}.wal")
    
    # Clean up other test files
    FileUtils.rm_f('test/relative_test.duckdb')
    FileUtils.rm_rf('test/deep') if Dir.exist?('test/deep')
  end
end
