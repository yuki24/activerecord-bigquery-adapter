# frozen_string_literal: true
require "test_helper"

class BigqueryAdapterTest < BigqueryTestCase
  def setup
    @conn = ActiveRecord::Base.bigquery_connection(DEFAULT_CONFIG)
  end

  test "raises an error on incompatible timeout" do
    error = assert_raises ArgumentError do
      ActiveRecord::Base.bigquery_connection(**DEFAULT_CONFIG, timeout: "usa")
    end

    assert_equal 'Invalid timeout value: "usa".', error.message
  end

  test "raises an exception when dataset is not provided" do
    error = assert_raise ArgumentError do
      ActiveRecord::Base.bigquery_connection adapter: "bigquery"
    end

    assert_equal "No dataset is specified. Missing argument: dataset.", error.message
  end

  test "raises an exception when no service account credentials are provided" do
    error = assert_raise ArgumentError do
      ActiveRecord::Base.bigquery_connection dataset: "bigquery_adapter_test", adapter: "bigquery"
    end

    assert_equal "No service account credentials specified. Missing argument: service_account_credentials.", error.message
  end

  test "raises an exception when the dataset does not exist" do
    conn = ActiveRecord::Base.bigquery_connection(**DEFAULT_CONFIG, dataset: "does_not_exist")

    error = assert_raises ActiveRecord::NoDatabaseError do
      conn.execute("SELECT * FROM test")
    end

    assert_equal "Google::Cloud::NotFoundError: Not found: Dataset activerecord-bigquery-adapter:does_not_exist was not found in location US", error.message
  end

  test "#database_exists? returns false when the dataset does not exist" do
    assert_not ActiveRecord::ConnectionAdapters::BigQueryAdapter.database_exists?(**DEFAULT_CONFIG, dataset: "does_not_exist"),
               "expected does_not_exist to not exist"
  end

  test "#database_exists? returns true when the dataset does exist" do
    assert ActiveRecord::ConnectionAdapters::BigQueryAdapter.database_exists?(DEFAULT_CONFIG),
           "expected bigquery_adapter_test to exist"
  end

  test "#database_exists? takes the :database keyword argument for compatibility" do
    assert ActiveRecord::ConnectionAdapters::BigQueryAdapter.database_exists?(**DEFAULT_CONFIG.without(:dataset), database: DEFAULT_CONFIG[:dataset]),
           "expected bigquery_adapter_test to exist"
  end

  # BigQuery defaults to UTF-8 encoding
  test "#encoding" do
    assert_equal "UTF-8", @conn.encoding
  end

  test "#execute" do
    with_example_table "number integer" do
      @conn.execute "INSERT INTO #{default_table_name} (number) VALUES (10)"
      records = @conn.execute "SELECT * FROM #{default_table_name}"
      assert_equal 1, records.length

      record = records.first
      assert_equal 10, record["number"]
    end
  end

  test "#exec without binds" do
    with_example_table "id integer, data string" do
      result = @conn.exec_query("SELECT id, data FROM #{default_table_name}")
      assert_equal 0, result.rows.length
      assert_equal 2, result.columns.length
      assert_equal %w{ id data }, result.columns

      @conn.exec_query("INSERT INTO #{default_table_name} (id, data) VALUES (1, \"foo\")")
      result = @conn.exec_query("SELECT id, data FROM #{default_table_name}")
      assert_equal 1, result.rows.length
      assert_equal 2, result.columns.length

      assert_equal [[1, "foo"]], result.rows
    end
  end

  test "#exec_query with binds does not support prepared statements" do
    error = with_example_table "id int, data string" do
      assert_raises ActiveRecord::StatementInvalid do
        @conn.exec_query(
          "SELECT id, data FROM #{default_table_name} WHERE id = ?", nil, [ActiveRecord::Relation::QueryAttribute.new(nil, 1, ActiveRecord::Type::Value.new)])
      end
    end

    assert_match(/Google::Cloud::InvalidArgumentError: Positional parameters are not supported at \[\d+:\d+\]/, error.message)
  end

  test "#exec_query with typecasts does not support prepared statements" do
    error = with_example_table "id int, data string" do
      assert_raises ActiveRecord::StatementInvalid do
        @conn.exec_query(
          "SELECT id, data FROM #{default_table_name} WHERE id = ?", nil, [ActiveRecord::Relation::QueryAttribute.new("id", "1-fuu", ActiveRecord::Type::Integer.new)])
      end
    end

    assert_match(/Positional parameters are not supported/, error.message)
  end

  test "#quote_string" do
    assert_equal "''", @conn.quote_string("'")
  end

  test "#insert is logged" do
    with_example_table "number integer" do
      sql = "INSERT INTO #{default_table_name} (number) VALUES (10)"
      name = "foo"
      assert_logged [[sql, name, []]] do
        @conn.insert(sql, name)
      end
    end
  end

  test "#select_rows" do
    with_example_table "number integer" do
      2.times do |i|
        @conn.execute "INSERT INTO #{default_table_name} (number) VALUES (#{i})"
      end
      rows = @conn.select_rows "SELECT number FROM #{default_table_name} ORDER BY number ASC"
      assert_equal [[0], [1]], rows
    end
  end

  test "#exec_insert" do
    with_example_table "number integer" do
      @conn.exec_insert("insert into #{default_table_name} (number) VALUES (10)", "SQL")

      result = @conn.exec_query("select number from #{default_table_name} where number = 10", "SQL")

      assert_equal 1, result.rows.length
      assert_equal 10, result.rows.first.first
    end
  end

  test "#exec_insert does not support prepared statements" do
    error = with_example_table "number integer" do
      assert_raises ActiveRecord::StatementInvalid do
        vals = [ActiveRecord::Relation::QueryAttribute.new("number", 10, ActiveRecord::Type::Value.new)]
        @conn.exec_insert("insert into #{default_table_name} (number) VALUES (?)", "SQL", vals)
      end
    end

    assert_match(/Positional parameters are not supported/, error.message)
  end

  test "#select_rows logs" do
    with_example_table "number integer" do
      sql = "select * from #{default_table_name}"
      name = "foo"
      assert_logged [[sql, name, []]] do
        @conn.select_rows sql, name
      end
    end
  end

  # BigQuery does not support primary key.
  test "#primary_key always returns nil" do
    with_example_table "id int, data string" do
      assert_nil @conn.primary_key(default_table_name)
    end
  end

  # Transaction control statements are supported only in scripts or sessions.
  test "#transaction does nothing" do
    with_example_table "number integer" do
      count_sql = "select count(*) from #{default_table_name}"

      @conn.begin_db_transaction
      @conn.execute "INSERT INTO #{default_table_name} (number) VALUES (10)"

      assert_equal 1, @conn.select_rows(count_sql).first.first
      @conn.rollback_db_transaction
      assert_equal 1, @conn.select_rows(count_sql).first.first
    end
  end

  test "#tables" do
    with_example_table "number integer" do
      assert_equal [default_table_name], @conn.tables

      with_example_table "id integer", "people" do
        assert_equal [default_table_name, "people"].sort, @conn.tables.sort
      end
    end
  end

  test "#tables logs name" do
    sql = <<~SQL.squish
      SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_type = 'BASE TABLE'
    SQL

    assert_logged [[sql, "SCHEMA", []]] do
      @conn.tables
    end
  end

  test "#table_exists logs name" do
    with_example_table "number integer" do
      sql = <<~SQL.squish
        SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '#{default_table_name}' AND table_type = 'BASE TABLE'
      SQL

      assert_logged [[sql, "SCHEMA", []]] do
        assert @conn.table_exists?(default_table_name)
      end
    end
  end

  test "#columns" do
    with_example_table "id integer, number integer" do
      columns = @conn.columns(default_table_name).sort_by(&:name)

      assert_equal 2, columns.length
      assert_equal %w{ id number }.sort, columns.map(&:name).sort
      assert_equal [nil, nil], columns.map(&:default)
      assert_equal [true, true], columns.map(&:null)
    end
  end

  test "#columns with not null" do
    with_example_table "id integer, number integer not null" do
      column = @conn.columns(default_table_name).find { |x| x.name == "number" }
      assert_not column.null, "column should not be null"
    end
  end

  # BigQuery does not support indexes.
  test "#indexes always returns an empty array" do
    assert_equal [], @conn.indexes("items")
  end

  test "#supports_extensions returns false" do
    assert_not @conn.supports_extensions?, "does not support extensions"
  end

  test "#respond_to enable_extension" do
    assert_respond_to @conn, :enable_extension
  end

  test "#respond_to disable_extension" do
    assert_respond_to @conn, :disable_extension
  end

  test "db is not readonly when readonly option is false" do
    skip
    conn = ActiveRecord::Base.bigquery_connection(**DEFAULT_CONFIG, readonly: false)
    assert_not_predicate conn.raw_connection, :readonly?
  end

  test "db is not readonly when readonly option is unspecified" do
    skip
    conn = ActiveRecord::Base.bigquery_connection(**DEFAULT_CONFIG)
    assert_not_predicate conn.raw_connection, :readonly?
  end

  test "db is readonly when readonly option is true" do
    skip
    conn = ActiveRecord::Base.bigquery_connection(**DEFAULT_CONFIG, readonly: true)
    assert_not_predicate conn.raw_connection, :readonly?
  end

  test "writes are not permitted to readonly databases" do
    skip
    conn = ActiveRecord::Base.bigquery_connection(**DEFAULT_CONFIG, readonly: true)

    assert_raises(ActiveRecord::StatementInvalid, /SQLite3::ReadOnlyException/) do
      conn.execute("CREATE TABLE test(id integer)")
    end
  end
end
