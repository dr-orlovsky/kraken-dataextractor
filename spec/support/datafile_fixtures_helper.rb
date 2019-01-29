def load_fixture_text(fn)
  File.open([Environment::APP_ROOT, 'spec', 'support', 'fixtures', fn].join('/'), &:read)
end
