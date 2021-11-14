# frozen_string_literal: true

module ActiveRecord
  module ConnectionAdapters
    module Bigquery
      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition
        # Always returns nil as BigQuery does not support primary keys.
        def primary_keys(name = nil) # :nodoc:
          nil
        end
      end
    end
  end
end
