require 'active_support/logger'

# App environment boilerplate code
class Environment

  APP_ROOT = File.expand_path(File.dirname(__FILE__) + '/..')
  class << self

    def app_config
      @app_config ||= YAML.load_file("#{File.expand_path(APP_ROOT + '/config')}/config.yml")
    end

    def logger
      return @logger if @logger
      l = Logger.new("#{File.expand_path(APP_ROOT + '/log')}/app.log")
      formatter = proc do |_severity, datetime, _progname, msg|
        "#{datetime}: #{msg}\n"
      end

      l.formatter = formatter
      @logger = l
    end

    def console_logger
      return @console_logger if @console_logger
      l = Logger.new(STDERR)
      l.formatter = logger.formatter
      @console_logger = l
    end

    def report_files_dir
      "#{Environment::APP_ROOT}/out"
    end

  end

end

def logger
  Environment.logger
end

def console_logger
  Environment.console_logger
end

Dir["#{Environment::APP_ROOT}/config/initializers/**/*.rb"].sort.each { |f| require f }
Dir["#{Environment::APP_ROOT}/lib/**/*.rb"].sort.each { |f| require f }
Dir["#{Environment::APP_ROOT}/app/**/*.rb"].sort.each { |f| require f }
