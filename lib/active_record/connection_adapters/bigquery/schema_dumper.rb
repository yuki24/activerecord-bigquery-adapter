# frozen_string_literal: true

require "active_record/connection_adapters/abstract/schema_dumper"

module ActiveRecord
  module ConnectionAdapters
    module Bigquery
      class SchemaDumper < ConnectionAdapters::SchemaDumper # :nodoc:
        private

        def default_primary_key?(column)
          false
        end

        def explicit_primary_key_default?(column)
          false
        end

        # The scenic-view gem adds this method so this needs to be ignored.
        def views(*); end

        # The fx gem adds this method so this needs to be ignored.
        def functions(*); end
        def empty_line(*); end
        def triggers(*); end
      end
    end
  end
end
