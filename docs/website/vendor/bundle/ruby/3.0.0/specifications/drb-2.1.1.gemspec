# -*- encoding: utf-8 -*-
# stub: drb 2.1.1 ruby lib

Gem::Specification.new do |s|
  s.name = "drb".freeze
  s.version = "2.1.1"

  s.required_rubygems_version = Gem::Requirement.new(">= 0".freeze) if s.respond_to? :required_rubygems_version=
  s.metadata = { "homepage_uri" => "https://github.com/ruby/drb", "source_code_uri" => "https://github.com/ruby/drb" } if s.respond_to? :metadata=
  s.require_paths = ["lib".freeze]
  s.authors = ["Masatoshi SEKI".freeze]
  s.date = "2022-12-05"
  s.description = "Distributed object system for Ruby".freeze
  s.email = ["seki@ruby-lang.org".freeze]
  s.homepage = "https://github.com/ruby/drb".freeze
  s.licenses = ["Ruby".freeze, "BSD-2-Clause".freeze]
  s.required_ruby_version = Gem::Requirement.new(">= 2.7.0".freeze)
  s.rubygems_version = "3.3.5".freeze
  s.summary = "Distributed object system for Ruby".freeze

  s.installed_by_version = "3.3.5" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4
  end

  if s.respond_to? :add_runtime_dependency then
    s.add_runtime_dependency(%q<ruby2_keywords>.freeze, [">= 0"])
  else
    s.add_dependency(%q<ruby2_keywords>.freeze, [">= 0"])
  end
end
