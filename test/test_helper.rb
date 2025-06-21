# frozen_string_literal: true

begin
  require 'debug'
rescue LoadError
  # Debug gem not available, continue without it
end

require_relative "config"
require "stringio"
require "active_record"
require "active_record/fixtures"
require "active_support/testing/autorun"
require "active_support/logger"

# Add the lib directory to load path
$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require 'activerecord-duckdb-adapter'

def connect
  ActiveRecord::Base.logger = ActiveSupport::Logger.new("log/debug.log", 0, 100 * 1024 * 1024)
  ActiveRecord::Base.configurations = {
      'duckdb' => {
          adapter: 'duckdb',
          database: 'test/test.duckdb',
          min_messages: 'warning'
      }
  }
  ActiveRecord::Base.establish_connection :duckdb
end

connect()

def load_schema
  # silence verbose schema loading
  original_stdout = $stdout
  $stdout = StringIO.new

  load SCHEMA_ROOT + "/schema.rb"

  ActiveRecord::FixtureSet.reset_cache
ensure
  $stdout = original_stdout
end

load_schema()

class TestCase < ActiveSupport::TestCase
  include ActiveRecord::TestFixtures
  self.fixture_paths = [::FIXTURE_ROOT]
  self.use_transactional_tests = true
  self.use_instantiated_fixtures = false
  
  # Add debugging for fixture loading issues
  def setup_fixtures(config = ActiveRecord::Base)
    puts "DEBUG: Setting up fixtures in #{self.class.name}" if ENV['DEBUG_FIXTURES']
    puts "DEBUG: Fixture paths: #{self.class.fixture_paths}" if ENV['DEBUG_FIXTURES']
    puts "DEBUG: Ruby version: #{RUBY_VERSION}" if ENV['DEBUG_FIXTURES']
    
    super
    
    if ENV['DEBUG_FIXTURES']
      puts "DEBUG: Fixtures loaded. Available fixture methods:"
      if respond_to?(:loaded_fixtures)
        puts "DEBUG: Loaded fixtures: #{loaded_fixtures.keys}"
      end
    end
  rescue => e
    puts "ERROR: Fixture setup failed: #{e.message}"
    puts "ERROR: Backtrace: #{e.backtrace.first(5).join(', ')}"
    raise
  end
end
