# Test mode environment tweaks
class Environment

  class << self

    alias app_logger logger

    def logger
      return @test_logger if @test_logger
      l = Logger.new("#{File.expand_path(APP_ROOT + '/log')}/test.log")
      l.formatter = app_logger.formatter
      @test_logger = l
    end

    def report_files_dir
      "#{Environment::APP_ROOT}/spec/out"
    end

  end

end
