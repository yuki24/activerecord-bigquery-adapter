# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Bigquery
      module DatabaseStatements
        READ_QUERY = ActiveRecord::ConnectionAdapters::AbstractAdapter.build_read_query_regexp # :nodoc:
        private_constant :READ_QUERY

        def write_query?(sql) # :nodoc:
          !READ_QUERY.match?(sql)
        end

        def execute(sql, name = nil) #:nodoc:
          if preventing_writes? && write_query?(sql)
            raise ActiveRecord::ReadOnlyError, "Write query attempted while in readonly mode: #{sql}"
          end

          log(sql, name) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              @connection.query(sql, dataset: @config[:dataset]).map!(&:stringify_keys!)
            end
          end
        end

        def exec_query(sql, name = nil, binds = [], prepare: false)
          if preventing_writes? && write_query?(sql)
            raise ActiveRecord::ReadOnlyError, "Write query attempted while in readonly mode: #{sql}"
          end

          type_casted_binds = type_casted_binds(binds)

          log(sql, name, binds, type_casted_binds) do
            ActiveSupport::Dependencies.interlock.permit_concurrent_loads do
              stmt = @connection.query(sql, dataset: @config[:dataset])
              cols = stmt.schema&.fields&.map(&:name)
              records = stmt.map!(&:values)

              ActiveRecord::Result.new(cols, records)
            end
          end
        end

        # Transaction control statements are supported only in scripts or sessions and we can not use that in
        # a way ActiveRecord works.
        def begin_db_transaction #:nodoc:
          # no-op...
        end

        # Transaction control statements are supported only in scripts or sessions and we can not use that in
        # a way ActiveRecord works.
        def commit_db_transaction #:nodoc:
          # no-op...
        end

        # Transaction control statements are supported only in scripts or sessions and we can not use that in
        # a way ActiveRecord works.
        def exec_rollback_db_transaction #:nodoc:
          # no-op...
        end

        # BigQuery does not return any value on INSERT.
        def last_inserted_id(*)
          nil
        end

        private

        def build_truncate_statement(table_name)
          "DROP TABLE #{quote_table_name(table_name)}"
        end
      end
    end
  end
end
