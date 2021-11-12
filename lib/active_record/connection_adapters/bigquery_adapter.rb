require "google/cloud/bigquery"

require_relative 'bigquery/schema_dumper'
require_relative 'bigquery/schema_statements'
require_relative 'bigquery/database_statements'

module ActiveRecord
  class Base
    def self.bigquery_connection(config) # :nodoc:
      config = config.symbolize_keys
      service_account_credentials, *remainder_config = config.values_at(:service_account_credentials, :debug)

      if service_account_credentials.blank?
        raise ArgumentError, "No service account credentials specified. Missing argument: service_account_credentials."
      end

      ConnectionAdapters::BigQueryAdapter.new(nil, logger, [service_account_credentials, *remainder_config], config)
    end
  end

  module ConnectionAdapters
    class BigQueryAdapter < AbstractAdapter
      ADAPTER_NAME = 'BigQuery'.freeze

      NATIVE_DATABASE_TYPES = {
        string:   { name: "varchar" },
        text:     { name: "text" },
        integer:  { name: "integer" },
        float:    { name: "float" },
        decimal:  { name: "decimal" },
        datetime: { name: "datetime" },
        time:     { name: "time" },
        date:     { name: "date" },
        binary:   { name: "blob" },
        boolean:  { name: "boolean" },
        json:     { name: "json" },
      }

      include Bigquery::SchemaStatements
      include Bigquery::DatabaseStatements

      attr_reader :service_account_credentials_json, :debug

      def initialize(connection, logger, connection_parameters, config)
        super(connection, logger)
        @config = config
        @service_account_credentials_json, @debug = connection_parameters
        @prepared_statements = false

        configure_logger
        connect
      end

      def self.database_exists?(config)
        ActiveRecord::Base.bigquery_connection(config).database_exists?
      end

      def database_exists?
        !! connection.dataset(config[:dataset])
      end

      def adapter_name
        ADAPTER_NAME
      end

      def supports_ddl_transactions?
        false
      end

      def supports_savepoints?
        false
      end

      def supports_transaction_isolation?
        false
      end

      def supports_partial_index?
        false
      end

      def supports_expression_index?
        false
      end

      def requires_reloading?
        true
      end

      def supports_foreign_keys?
        true
      end

      def supports_check_constraints?
        false
      end

      def supports_views?
        false
      end

      def supports_datetime_with_precision?
        false
      end

      def supports_json?
        false
      end

      def supports_common_table_expressions?
        true
      end

      def supports_insert_on_conflict?
        false
      end
      alias supports_insert_on_duplicate_skip? supports_insert_on_conflict?
      alias supports_insert_on_duplicate_update? supports_insert_on_conflict?
      alias supports_insert_conflict_target? supports_insert_on_conflict?

      def supports_concurrent_connections?
        false
      end

      def active?
        !! @connection&.service
      end

      def reconnect!
        super

        @connection = nil
        connect
      end

      # Disconnects from the database if already connected. Otherwise, this
      # method does nothing.
      def disconnect!
        super
        connection.connection.service.service.client.reset_all
      end

      def supports_index_sort_order?
        false
      end

      def native_database_types # :nodoc:
        NATIVE_DATABASE_TYPES
      end

      # Returns the current database encoding format as a string, e.g. 'UTF-8'
      def encoding
        @connection.encoding.to_s
      end

      def supports_explain?
        true
      end

      def supports_lazy_transactions?
        true
      end

      def execute(sql, name = nil)
        log(sql, name) do
          connection.query(sql, dataset: @config[:dataset])
        end
      end

      # REFERENTIAL INTEGRITY ====================================

      # SCHEMA STATEMENTS ========================================

      def primary_keys(table_name) # :nodoc:
        [] # no-op: BigQuery does not support primary keys.
      end

      def remove_index(table_name, column_name = nil, **options) # :nodoc:
        # no-op: BigQuery does not support index.
      end

      # Renames a table.
      #
      # Example:
      #   rename_table('octopuses', 'octopi')
      def rename_table(table_name, new_name)
        schema_cache.clear_data_source_cache!(table_name.to_s)
        schema_cache.clear_data_source_cache!(new_name.to_s)
        exec_query "ALTER TABLE #{quote_table_name(table_name)} RENAME TO #{quote_table_name(new_name)}"
        rename_table_indexes(table_name, new_name)
      end

      def add_column(table_name, column_name, type, **options) # :nodoc:
        if invalid_alter_table_type?(type, options)
          alter_table(table_name) do |definition|
            definition.column(column_name, type, **options)
          end
        else
          super
        end
      end

      def remove_column(table_name, column_name, type = nil, **options) # :nodoc:
        alter_table(table_name) do |definition|
          definition.remove_column column_name
          definition.foreign_keys.delete_if { |fk| fk.column == column_name.to_s }
        end
      end

      def remove_columns(table_name, *column_names, type: nil, **options) # :nodoc:
        alter_table(table_name) do |definition|
          column_names.each do |column_name|
            definition.remove_column column_name
          end
          column_names = column_names.map(&:to_s)
          definition.foreign_keys.delete_if { |fk| column_names.include?(fk.column) }
        end
      end

      def change_column_default(table_name, column_name, default_or_changes) # :nodoc:
        default = extract_new_default_value(default_or_changes)

        alter_table(table_name) do |definition|
          definition[column_name].default = default
        end
      end

      def change_column_null(table_name, column_name, null, default = nil) # :nodoc:
        unless null || default.nil?
          exec_query("UPDATE #{quote_table_name(table_name)} SET #{quote_column_name(column_name)}=#{quote(default)} WHERE #{quote_column_name(column_name)} IS NULL")
        end
        alter_table(table_name) do |definition|
          definition[column_name].null = null
        end
      end

      def change_column(table_name, column_name, type, **options) # :nodoc:
        alter_table(table_name) do |definition|
          definition[column_name].instance_eval do
            self.type = aliased_types(type.to_s, type)
            self.options.merge!(options)
          end
        end
      end

      def rename_column(table_name, column_name, new_column_name) # :nodoc:
        column = column_for(table_name, column_name)
        alter_table(table_name, rename: { column.name => new_column_name.to_s })
        rename_column_indexes(table_name, column.name, new_column_name)
      end

      def add_reference(table_name, ref_name, **options) # :nodoc:
        super(table_name, ref_name, type: :integer, **options)
      end
      alias :add_belongs_to :add_reference

      def foreign_keys(table_name)
        []
      end

      def connection
        @connection ||= Google::Cloud::Bigquery.new(credentials: service_account_credentials)
      end
      alias connect connection

      private

      # Returns the list of a table's column names, data types, and default values.
      def column_definitions(table_name)
        (@column_definitions ||= execute("SELECT * FROM analytics.INFORMATION_SCHEMA.COLUMNS"))
          .select { _1[:table_name] == table_name }
      end

      def initialize_type_map(m = type_map)
        super
        register_class_with_limit m, %r(STRING)i, Type::String
        register_class_with_limit m, %r(BOOL)i, Type::Boolean
      end

      def build_statement_pool
        StatementPool.new(self.class.type_cast_config_to_integer(@config[:statement_limit]))
      end

      def configure_logger
        Google::Apis.logger = debug ? Rails.logger : Logger.new('/dev/null')
      end

      def service_account_credentials
        @service_account_credentials ||=
          Google::Auth::ServiceAccountCredentials.make_creds(scope: 'https://www.googleapis.com/auth/bigquery', json_key_io: json_key_io)
      end

      def json_key_io
        @json_key_io ||= StringIO.new(service_account_credentials_json)
      end
    end
  end
end
