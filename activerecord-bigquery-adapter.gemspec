# frozen_string_literal: true

require_relative "lib/active_record/connection_adapters/bigquery/version"

Gem::Specification.new do |spec|
  spec.name          = "activerecord-bigquery-adapter"
  spec.version       = Activerecord::Bigquery::Adapter::VERSION
  spec.authors       = ["Yuki Nishijima"]
  spec.email         = ["yuki24@hey.com"]

  spec.summary       = "BigQuery adapter for ActiveRecord."
  spec.description   = "Finally, ActiveRecord BigQuery adapter that is maintained."
  spec.homepage      = "https://github.com/retailzipline/activerecord-bigquery-adapter"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 2.5.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/retailzipline/activerecord-bigquery-adapter"
  spec.metadata["changelog_uri"] = "https://github.com/retailzipline/activerecord-bigquery-adapter/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject do |f|
      (f == __FILE__) || f.match(%r{\A(?:(?:test)/|\.(?:git|travis|circleci)|appveyor)})
    end
  end

  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_dependency "activerecord", ">= 5.2.0"
  spec.add_dependency "google-cloud-bigquery"

  spec.add_development_dependency "appraisal"
end
