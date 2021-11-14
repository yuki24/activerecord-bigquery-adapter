# frozen_string_literal: true

require "active_record/connection_adapters/abstract/schema_creation"

module ActiveRecord
  module ConnectionAdapters
    module Bigquery
      class SchemaCreation < SchemaCreation # :nodoc:
        private

        def supports_index_using?
          false
        end

        def visit_AddColumnDefinition(o)
          +"ADD COLUMN #{accept(o.column)}"
        end
      end
    end
  end
end
