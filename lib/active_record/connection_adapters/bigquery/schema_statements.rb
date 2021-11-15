# frozen_string_literal: true

require_relative 'schema_creation'
require_relative 'table_definitions'

module ActiveRecord
  module ConnectionAdapters
    module Bigquery
      module SchemaStatements # :nodoc:
        def primary_keys(*) # :nodoc:
          [] # no-op: BigQuery does not support primary keys.
        end

        def indexes(*) # :nodoc:
          []
        end

        def remove_index(*) # :nodoc:
          # no-op: BigQuery does not support index.
        end

        def foreign_keys(table_name) # :nodoc:
          []
        end

        def remove_foreign_key(*) # :nodoc:
          # no-op: BigQuery does not support foreign keys.
        end

        def rename_table(table_name, new_name) # :nodoc:
          schema_cache.clear_data_source_cache!(table_name.to_s)
          schema_cache.clear_data_source_cache!(new_name.to_s)

          exec_query "ALTER TABLE #{quote_table_name(table_name)} RENAME TO #{quote_table_name(new_name)}"
        end

        def change_column_default(*) # :nodoc:
          raise NotImplementedError, "Default values are not supported in BigQuery."
        end

        def change_column_null(table_name, column_name, null, default = nil) # :nodoc:
          if null
            exec_query "ALTER TABLE #{quote_table_name(table_name)} ALTER COLUMN #{quote_column_name(column_name)} DROP NOT NULL"
          else
            raise NotImplementedError, "Adding a non-null constraint is supported in BigQuery."
          end
        end

        def change_column(table_name, column_name, type, **options) # :nodoc:
          exec_query "ALTER TABLE #{quote_table_name(table_name)} ALTER COLUMN #{quote_column_name(column_name)} SET DATA TYPE #{type_to_sql(type)}"
        end

        def rename_column(*) # :nodoc:
          raise NotImplementedError, "Renaming a table with a single SQL is not supported in BigQuery. You " \
                                     "will have to create a new table with a different name and drop the old one."
        end

        def check_constraints(*) # :nodoc:
          # BigQuery does not support check constraints.
          []
        end

        def remove_check_constraint(*) # :nodoc:
          # no-op: BigQuery does not support check constraints.
        end

        def create_schema_dumper(options)
          Bigquery::SchemaDumper.create(self, options)
        end

        private

        def schema_creation
          Bigquery::SchemaCreation.new(self)
        end

        def create_table_definition(name, **options)
          Bigquery::TableDefinition.new(self, name, **options)
        end

        def validate_index_length!(*)
          # no-op: BigQuery does not support check constraints.
        end

        def new_column_from_field(_table_name, field)
          sql_type_metadata = fetch_type_metadata(field['column_name'], field['data_type'])
          Column.new(field['column_name'], nil, sql_type_metadata, field['is_nullable'] == 'YES')
        end

        def fetch_type_metadata(_column_name, sql_type)
          cast_type = type_map.lookup(sql_type)
          SqlTypeMetadata.new(sql_type: sql_type, type: cast_type.type, limit: cast_type.limit, precision: cast_type.precision, scale: cast_type.scale)
        end

        def data_source_sql(name = nil, type: nil)
          scope = quoted_scope(name, type: type)
          select_from = +"SELECT table_name FROM INFORMATION_SCHEMA.TABLES"

          if scope.present?
            wheres = { table_name: scope[:name], table_type: scope[:type] }
                       .compact
                       .map { |column_name, value| "#{column_name} = #{value}" }
                       .join(" AND ")

            "#{select_from} WHERE #{wheres}"
          else
            select_from
          end
        end

        def quoted_scope(name = nil, type: nil)
          { name: name&.then { quote(_1) }, type: type&.then { quote(_1) } }.compact
        end
      end
    end
  end
end
