# Activerecord::Bigquery::Adapter

The `activerecord-bigquery-adapter` offers a way to use ActiveRecord with BigQuery.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'activerecord-bigquery-adapter'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install activerecord-bigquery-adapter

## Set up the adapter

Once you install the gem, the `bigquery` adapter will be available in the `config/database.yml`:

```yaml
development:
  adapter: bigquery
  service_account_credentials: '<%= ENV["GOOGLE_CREDENTIALS"] %>'
  dataset: name_of_your_dataset
```

## Configuration options

| Name                          | Default  | Description |
|-------------------------------|----------|-------------|
| `service_account_credentials` | Required | Your service account credentials for Google Cloud. See [Creating a service account](https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account) for how to create one. |
| `dataset`                     | Required | The [dataset](https://cloud.google.com/bigquery/docs/datasets-intro) you would like to retrieve data from. |
| `timeout`                     | `nil`    | timeout to use in requests in seconds. |
| `readonly`                    | `false`  | Prevents any write queries from being run. Raises an `ActiveRecord::ReadOnlyRecord` error in such a case. |

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/retailzipline/activerecord-bigquery-adapter. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/retailzipline/activerecord-bigquery-adapter/blob/master/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Activerecord::Bigquery::Adapter project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/[USERNAME]/activerecord-bigquery-adapter/blob/master/CODE_OF_CONDUCT.md).
