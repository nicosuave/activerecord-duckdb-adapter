# # frozen_string_literal: true

require "test_helper"

require "models/author"
require "models/post"

class FinderTest < TestCase
  fixtures :posts

  def setup
    # Debug fixture loading
    puts "=== FinderTest Setup Debug ==="
    puts "Ruby version: #{RUBY_VERSION}"
    puts "ActiveRecord version: #{ActiveRecord.version}"
    puts "Posts table exists: #{ActiveRecord::Base.connection.table_exists?('posts')}"
    puts "Posts count: #{Post.count}"
    
    if Post.count > 0
      puts "First post: #{Post.first&.attributes}"
      puts "All posts: #{Post.all.map(&:attributes)}"
    else
      puts "No posts found - checking fixture loading..."
      puts "Fixture paths: #{self.class.fixture_paths}"
      puts "Loaded fixtures: #{loaded_fixtures.keys}" if respond_to?(:loaded_fixtures)
      
      # Try to manually check table contents
      begin
        result = ActiveRecord::Base.connection.execute("SELECT COUNT(*) as count FROM posts")
        puts "Direct SQL count: #{result.first['count'] rescue 'error'}"
        
        result = ActiveRecord::Base.connection.execute("SELECT * FROM posts LIMIT 3")
        puts "Direct SQL results: #{result.to_a rescue 'error'}"
      rescue => e
        puts "SQL error: #{e.message}"
      end
      
      # Try to reload fixtures manually
      begin
        puts "Attempting to reload fixtures..."
        self.class.fixture_paths.each do |fixture_path|
          puts "Fixture path: #{fixture_path}"
          puts "Posts fixture exists: #{File.exist?(File.join(fixture_path, 'posts.yml'))}"
        end
        
        # Force fixture reload
        ActiveRecord::FixtureSet.reset_cache
        setup_fixtures
        puts "After manual setup - Posts count: #{Post.count}"
      rescue => e
        puts "Fixture reload error: #{e.message}"
        puts "Backtrace: #{e.backtrace.first(3).join(', ')}"
      end
    end
    puts "=========================="
  end

  def test_find
    assert_equal(posts(:first).title, Post.find(1).title)
  end

  def skip_test_bigint
    # TODO: 多分だけど、DuckDBで返ってきたIDの値が、見た目上はInteger型だけど、実際は違う？？どっかでCastしないといけない？？
    # Primary KeyをBigIntにすると、in_order_ofで、うまく結果が返ってこない。これは、多分IntとBigIntが違うからだと思う。

    records = Post.where(enabled: true).where(id: [1, 2]).records
    p h = records.index_by(&:id)
    p h.keys
    p Post.find(h.keys)
    p [1, 2]
    p Post.find([1, 2])
    p h.keys.equal? [1, 2]
    p h.keys.map { |v| v.object_id }
    p [1, 2].map { |v| v.object_id }
    p h.keys.map { |v| v.class.ancestors }
    p [1, 2].map { |v| v.class.ancestors }
  end

  def test_find_where
    records = Post.where(enabled: true).find([2, 1, 3])
    assert_equal 3, records.size
    assert_equal posts(:second).title, records[0].title
    assert_equal posts(:first).title, records[1].title
    assert_equal posts(:third).title, records[2].title
  end

  def test_exists
    assert_equal true, Post.exists?(1)
    assert_equal true, Post.exists?("1")
    assert_equal true, Post.exists?(title: Post.find(1).title)
    assert_equal true, Post.exists?(id: [1, 9999])

    assert_equal false, Post.exists?(45)
    assert_equal false, Post.exists?(9999999999999999999999999999999)
    assert_equal false, Post.exists?(Post.new.id)
  end
end
