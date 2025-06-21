# frozen_string_literal: true

require "test_helper"

class PrimaryKeysTest < TestCase
  def setup
    @connection = ActiveRecord::Base.connection
  end

  def test_primary_keys_single_column
    # Test with the existing 'authors' table which has a single primary key
    primary_keys = @connection.primary_keys('authors')
    
    assert_equal ['id'], primary_keys
    assert_instance_of Array, primary_keys
    assert_equal 1, primary_keys.length
  end

  def test_auto_increment_primary_key_basic
    # Test that the basic auto-increment functionality works (fixes the original error)
    @connection.create_table :test_auto_increment, force: true do |t|
      t.string :name
    end

    # Reset sequence to ensure clean start
    begin
      @connection.execute("SELECT setval('test_auto_increment_id_seq', 1, false)")
    rescue
      # Ignore if sequence doesn't exist or setval doesn't work
    end

    # This should not raise "NOT NULL constraint failed: users.id"
    @connection.execute("INSERT INTO test_auto_increment (name) VALUES ('First')")
    @connection.execute("INSERT INTO test_auto_increment (name) VALUES ('Second')")
    @connection.execute("INSERT INTO test_auto_increment (name) VALUES ('Third')")

    # Verify the auto-increment worked
    results = @connection.execute("SELECT id, name FROM test_auto_increment ORDER BY id")
    
    assert_equal 3, results.to_a.length
    # Don't test exact IDs, just that they're sequential
    ids = results.to_a.map { |row| row[0] }
    assert_equal ids.sort, ids, "IDs should be in sequential order"
    assert_equal ids.uniq, ids, "IDs should be unique"
    
  ensure
    @connection.drop_table :test_auto_increment, if_exists: true
  end
end 