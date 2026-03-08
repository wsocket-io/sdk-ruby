Gem::Specification.new do |s|
  s.name        = 'wsocket-io'
  s.version     = '0.2.0'
  s.summary     = 'wSocket SDK for Ruby'
  s.description = 'Official Ruby SDK for wSocket — realtime pub/sub, presence, history, and push notifications.'
  s.authors     = ['wSocket']
  s.email       = 'sdk@wsocket.io'
  s.homepage    = 'https://github.com/wsocket-io/sdk-ruby'
  s.license     = 'MIT'

  s.files       = Dir['lib/**/*.rb'] + ['LICENSE', 'README.md']
  s.require_paths = ['lib']

  s.required_ruby_version = '>= 3.0'

  s.add_dependency 'websocket-client-simple', '~> 0.8'
  s.add_dependency 'json', '~> 2.6'
end
