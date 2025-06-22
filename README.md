# Activerecord::Duckdb::Adapter

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/activerecord/duckdb/adapter`. To experiment with that code, run `bin/console` for an interactive prompt.


## Overview

DuckDB is an embeddable SQL OLAP database used analytical workloads, data science applications, and situations where you need fast analytical queries without the overhead of a separate database server. This adapter allows you to use DuckDB with ActiveRecord and Rails applications.

The adapter now defaults to **file-based databases** for data persistence, while still supporting in-memory databases as well.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'activerecord-duckdb-adapter'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install activerecord-duckdb-adapter

## Dependencies

This adapter depends on:
- [ruby-duckdb](https://github.com/suketa/ruby-duckdb) - Ruby bindings for DuckDB
- ActiveRecord 7.0+

## Usage

### Basic Configuration

In your `database.yml`:

```yaml
development:
  adapter: duckdb
  # File-based database (default)
  database: db/development.duckdb
  
  # For in-memory database (useful for testing)
  # database: ":memory:"
```

### Establishing Connection

```ruby
ActiveRecord::Base.establish_connection(
  adapter: 'duckdb',
  database: 'db/my_app.duckdb'  # or ':memory:' for in-memory database
)
```


## Development

After checking out the repo, run:

```bash
bin/setup
```

To run the test suite:

```bash
bundle exec rake test
```


To experiment with the adapter:

```bash
bin/console
```



## References
- [ruby-duckdb](https://github.com/suketa/ruby-duckdb): The underlying Ruby DuckDB driver (actively maintained)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [ActiveRecord Adapter Development](https://web.archive.org/web/20230326213337/https://eng.localytics.com/odbc-and-writing-your-own-activerecord-adapter/)
- [Rails Database Adapter Registration](https://github.com/rails/rails/commit/009c7e74117690f0dbe200188a929b345c9306c1)
- [Arel Query Building](https://www.cloudbees.com/blog/creating-advanced-active-record-db-queries-arel)


## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/red-data-tools/activerecord-duckdb-adapter. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/[USERNAME]/activerecord-duckdb-adapter/blob/master/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Activerecord::Duckdb::Adapter project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/red-data-tools/activerecord-duckdb-adapter/blob/master/CODE_OF_CONDUCT.md).
