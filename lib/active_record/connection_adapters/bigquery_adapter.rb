# frozen_string_literal: true

require "google/cloud/bigquery"

require "active_record/connection_adapters/statement_pool"

require_relative 'bigquery/schema_dumper'
require_relative 'bigquery/schema_statements'
require_relative 'bigquery/database_statements'

module ActiveRecord
  class Base
    def self.bigquery_connection(config) # :nodoc:
      config = config.symbolize_keys
      config[:replica] = config[:readonly]

      unless config[:dataset]
        raise ArgumentError, "No dataset is specified. Missing argument: dataset."
      end

      service_account_credentials, *remainder_config = config.values_at(:service_account_credentials, :debug)

      if service_account_credentials.blank?
        raise ArgumentError, "No service account credentials specified. Missing argument: service_account_credentials."
      end

      if config[:timeout] && !config[:timeout].is_a?(Numeric)
        raise ArgumentError, "Invalid timeout value: #{config[:timeout].inspect}."
      end

      ConnectionAdapters::BigQueryAdapter.new(nil, logger, [service_account_credentials, *remainder_config], config)
    end
  end

  module ConnectionAdapters
    class BigQueryAdapter < AbstractAdapter
      ADAPTER_NAME = 'BigQuery'.freeze

      NATIVE_DATABASE_TYPES = {
        string:   { name: "STRING" },
        text:     { name: "STRING" },
        integer:  { name: "INTEGER" },
        float:    { name: "FLOAT64" },
        decimal:  { name: "DECIMAL" },
        datetime: { name: "DATETIME" },
        time:     { name: "TIME" },
        date:     { name: "DATE" },
        binary:   { name: "BYTES" },
        boolean:  { name: "BOOL" },
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
        config[:dataset] ||= config.delete(:database)
        ActiveRecord::Base.bigquery_connection(config).database_exists?
      end

      def database_exists?
        !! connection.dataset(@config[:dataset])
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
        'UTF-8'
      end

      def supports_explain?
        true
      end

      def supports_lazy_transactions?
        true
      end

      def connection
        @connection ||= Google::Cloud::Bigquery.new(credentials: service_account_credentials, timeout: @config[:timeout])
      end
      alias connect connection

      private

      def translate_exception(exception, message:, sql:, binds:)
        if exception.message.start_with?('Not found: Dataset')
          NoDatabaseError.new(message)
        else
          super
        end
      end

      # Returns the list of a table's column names, data types, and default values.
      def column_definitions(table_name)
        (@column_definitions ||= execute("SELECT * FROM INFORMATION_SCHEMA.COLUMNS"))
          .select { _1['table_name'] == table_name }
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
