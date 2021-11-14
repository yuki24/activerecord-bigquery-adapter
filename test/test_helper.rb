# frozen_string_literal: true

$LOAD_PATH.unshift File.expand_path("../lib", __dir__)
require 'active_record'
require 'pry-byebug'
require "active_record/connection_adapters/bigquery_adapter"

require "minitest/autorun"
require "minitest/focus"

class SQLSubscriber
  attr_reader :logged
  attr_reader :payloads

  def initialize
    @logged = []
    @payloads = []
  end

  def start(name, id, payload)
    @payloads << payload
    @logged << [payload[:sql].squish, payload[:name], payload[:binds]]
  end

  def finish(name, id, payload); end
end

class BigqueryTestCase < ActiveSupport::TestCase
  DEFAULT_CONFIG = {
    dataset: "bigquery_adapter_test",
    adapter: "bigquery",
    service_account_credentials: ENV.fetch('GOOGLE_CREDENTIALS')
  }

  private

  def with_example_table(definition = nil, table_name = self.default_table_name)
    @conn.execute("CREATE TABLE #{table_name}(#{definition})")
    yield
  ensure
    @conn.execute("DROP TABLE #{table_name}")
  end

  def assert_logged(logs)
    subscriber = SQLSubscriber.new
    subscription = ActiveSupport::Notifications.subscribe("sql.active_record", subscriber)
    yield
    assert_equal logs, subscriber.logged
  ensure
    ActiveSupport::Notifications.unsubscribe(subscription)
  end

  def default_table_name
    @default_table_name ||= "ex_#{object_id}"
  end
end
