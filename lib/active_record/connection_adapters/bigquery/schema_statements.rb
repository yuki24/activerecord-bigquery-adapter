# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Bigquery
      module SchemaStatements # :nodoc:
        # Returns an empty array as BigQuery does not support indexes.
        def indexes(table_name)
          []
        end

        def add_foreign_key(from_table, to_table, **options)
          alter_table(from_table) do |definition|
            to_table = strip_table_name_prefix_and_suffix(to_table)
            definition.foreign_key(to_table, **options)
          end
        end

        def remove_foreign_key(from_table, to_table = nil, **options)
          return if options[:if_exists] == true && !foreign_key_exists?(from_table, to_table)

          to_table ||= options[:to_table]
          options = options.except(:name, :to_table, :validate)
          foreign_keys = foreign_keys(from_table)

          fkey = foreign_keys.detect do |fk|
            table = to_table || begin
                                  table = options[:column].to_s.delete_suffix("_id")
                                  Base.pluralize_table_names ? table.pluralize : table
                                end
            table = strip_table_name_prefix_and_suffix(table)
            fk_to_table = strip_table_name_prefix_and_suffix(fk.to_table)
            fk_to_table == table && options.all? { |k, v| fk.options[k].to_s == v.to_s }
          end || raise(ArgumentError, "Table '#{from_table}' has no foreign key for #{to_table || options}")

          foreign_keys.delete(fkey)
          alter_table(from_table, foreign_keys)
        end

        def check_constraints(table_name)
          raise NotImplementedError
        end

        def add_check_constraint(table_name, expression, **options)
          raise NotImplementedError
        end

        def remove_check_constraint(table_name, expression = nil, **options)
          raise NotImplementedError
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

        def validate_index_length!(table_name, new_name, internal = false)
          raise NotImplementedError
        end

        def new_column_from_field(table_name, field)
          sql_type_metadata = fetch_type_metadata(field[:column_name], field[:data_type])
          Column.new(field[:column_name], nil, sql_type_metadata, field[:is_nullable] == 'YES')
        end

        def fetch_type_metadata(column_name, sql_type)
          cast_type = type_map.lookup(sql_type)
          SqlTypeMetadata.new(sql_type: sql_type, type: cast_type.type, limit: cast_type.limit, precision: cast_type.precision, scale: cast_type.scale)
        end

        def data_source_sql(name = nil, type: nil)
          scope = quoted_scope(name, type: type)
          select_from = +"SELECT table_name FROM #{@config[:dataset]}.INFORMATION_SCHEMA.TABLES"

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
