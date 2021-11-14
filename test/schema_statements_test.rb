# frozen_string_literal: true
require "test_helper"

class SchemaStatementsTest < BigqueryTestCase
  def setup
    @conn = ActiveRecord::Base.bigquery_connection(DEFAULT_CONFIG)
  end

  test "#create_table can create a table with all supported types" do
    @conn.create_table(:users, id: false, force: :cascade) do |t|
      t.string :name, null: false
      t.text :description
      t.integer :age
      t.float :height
      t.decimal :net_worth, precision: 13, scale: 2
      t.datetime :last_logged_in_at
      t.time :wakes_up_at
      t.date :birthday
      t.binary :avatar
      t.boolean :covid19_vaccinated
    end

    assert_includes @conn.tables, "users"

    columns = @conn.columns(:users).to_h { [_1.name, _1] }

    assert_equal :string, columns["name"].type
    assert_equal "STRING", columns["name"].sql_type
    assert_equal :string, columns["description"].type
    assert_equal "STRING", columns["description"].sql_type
    assert_equal :integer, columns["age"].type
    assert_equal "INT64", columns["age"].sql_type
    assert_equal :float, columns["height"].type
    assert_equal "FLOAT64", columns["height"].sql_type
    assert_equal :decimal, columns["net_worth"].type
    assert_equal "NUMERIC(13, 2)", columns["net_worth"].sql_type
    assert_equal :datetime, columns["last_logged_in_at"].type
    assert_equal "DATETIME", columns["last_logged_in_at"].sql_type
    assert_equal :time, columns["wakes_up_at"].type
    assert_equal "TIME", columns["wakes_up_at"].sql_type
    assert_equal :date, columns["birthday"].type
    assert_equal "DATE", columns["birthday"].sql_type
    assert_nil  columns["avatar"].type # TODO: the type is nil????
    assert_equal "BYTES", columns["avatar"].sql_type
    assert_equal :boolean, columns["covid19_vaccinated"].type
    assert_equal "BOOL", columns["covid19_vaccinated"].sql_type
  ensure
    @conn.execute("DROP TABLE users") rescue Google::Cloud::NotFoundError
  end

  test "#add_index raises an exception" do
    with_example_table "id integer, number integer" do
      error = assert_raises ActiveRecord::StatementInvalid do
        @conn.add_index default_table_name, "id"
      end

      assert_equal Google::Cloud::InvalidArgumentError, error.cause.class
    end
  end

  test "#rename_table rename the table" do
    skip "Bigquery returns an error 'notFound: Not found: Table project:dataset.table' even though it " \
         "successfully renames a table."

    with_example_table "id integer, number integer" do
      @conn.rename_table default_table_name, "other"

      assert_equal ["other"], @conn.tables
    ensure
      @conn.rename_table "other", default_table_name rescue Google::Cloud::NotFoundError
    end
  end

  test "#remove_column removes a column" do
    with_example_table "id integer, number integer" do
      assert_logged [["ALTER TABLE #{default_table_name} DROP COLUMN number", nil, []]] do
        @conn.remove_column default_table_name, :number
      end

      # BigQuery has a few min of delay to update its own table info, so skipping this assertion:
      # assert_not_includes @conn.columns(default_table_name).map(&:name), "number"
    end
  end

  test "#remove_columns removes multiple columns" do
    with_example_table "id integer, number integer, name string" do
      assert_logged [["ALTER TABLE #{default_table_name} DROP COLUMN number", nil, []], ["ALTER TABLE #{default_table_name} DROP COLUMN name", nil, []]] do
        @conn.remove_columns default_table_name, :number, :name
      end

      # BigQuery has a few min of delay to update its own table info, so skipping this assertion:
      # assert_not_includes @conn.columns(default_table_name).map(&:name), "number"
      # assert_not_includes @conn.columns(default_table_name).map(&:name), "name"
    end
  end

  test "#change_column_null can remove a non-null constraint but can not add it" do
    with_example_table "id integer not null, number integer" do
      assert_logged [["ALTER TABLE #{default_table_name} ALTER COLUMN id DROP NOT NULL", nil, []]] do
        @conn.change_column_null default_table_name, :id, true
      end

      assert_includes @conn.columns(default_table_name).select(&:null).map(&:name), "id"

      error = assert_raises NotImplementedError do
        @conn.change_column_null default_table_name, :id, false
      end

      assert_equal "Adding a non-null constraint is supported in BigQuery.", error.message
    end
  end

  test "#change_column changes the column type" do
    with_example_table "id integer, number integer" do
      assert_logged [["ALTER TABLE #{default_table_name} ALTER COLUMN id SET DATA TYPE FLOAT64", nil, []]] do
        @conn.change_column default_table_name, :id, :float
      end

      id_column = @conn.columns(default_table_name).find {|column| column.name == 'id' }

      assert_equal :float, id_column.type
    end
  end

  test "#add_column with null" do
    with_example_table "id integer, number integer not null" do
      assert_nothing_raised do
        @conn.add_column default_table_name.to_sym, :name, :string
      end

      column = @conn.columns(default_table_name).find { |x| x.name == "name" }
      assert column.null, "column should be null"
    end
  end
end
