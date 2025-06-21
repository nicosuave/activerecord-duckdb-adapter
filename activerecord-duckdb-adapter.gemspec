# frozen_string_literal: true

require_relative "lib/activerecord_duckdb_adapter/version"

Gem::Specification.new do |spec|
  spec.name          = "activerecord-duckdb-adapter"
  spec.version       = ActiveRecordDuckdbAdapter::VERSION
  spec.authors       = ["okadakk", "Eddie A Tejeda"]
  spec.email         = ["k.suke.jp1990@gmail.com", "eddie.tejeda@gmail.com"]

  spec.summary       = "ActiveRecord adapter for DuckDB database"
  spec.description   = "A Ruby gem that provides an ActiveRecord adapter for DuckDB, enabling Ruby and Rails applications to use DuckDB as their database backend."
  spec.homepage      = "https://github.com/red-data-tools/activerecord-duckdb-adapter"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata = {
    "bug_tracker_uri" => "https://github.com/red-data-tools/activerecord-duckdb-adapter/issues",
    "changelog_uri"   => "https://github.com/red-data-tools/activerecord-duckdb-adapter/blob/main/CHANGELOG.md",
    "source_code_uri" => "https://github.com/red-data-tools/activerecord-duckdb-adapter",
    "rubygems_mfa_required" => "true"
  }

  # Specify files to include in the gem
  spec.files = Dir[
    "lib/**/*",
    "README.md",
    "LICENSE.txt",
    "CHANGELOG.md"
  ].select { |f| File.file?(f) }
  
  spec.require_paths = ["lib"]

  # Development dependencies with bounded versions
  spec.add_development_dependency "bundler", "~> 2.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rspec", "~> 3.13"

  # Runtime dependencies with bounded versions
  spec.add_dependency "activerecord", "~> 7.1"
  spec.add_dependency "duckdb", "~> 1.1"
end
