#!/usr/bin/env ruby

require 'json'

class Logger
  def self.checkpoint(msg)
    puts ""
    puts "\e[95m##### #{msg} #####\e[0m"
  end

  def self.error(msg)
    puts "\e[91m#{msg}\e[0m"
  end

  def self.warning(msg)
    puts "\e[93m#{msg}\e[0m"
  end

  def self.success(msg)
    puts "\e[92m#{msg}\e[0m"
  end
end

class Config
  private

  def load_or_raise(name)
    value = ENV[name]
    if value.nil?
      raise MissingRequiredEnvironmentVariable.new(name)
    end

    value
  end

  def load_or_default(name, default)
    ENV[name].nil? ? default : ENV[name]
  end
end

class SmokesConfig < Config
  attr_reader :job_name, :drain_type, :drain_version, :cycles, :delay_us,
    :datadog_api_key

  def initialize
    @job_name = load_or_raise('JOB_NAME')
    @drain_type = load_or_raise('DRAIN_TYPE')
    @drain_version = load_or_raise('DRAIN_VERSION')
    @cycles = load_or_raise('CYCLES')
    @delay_us = load_or_raise('DELAY_US')
    @datadog_api_key = load_or_raise('DATADOG_API_KEY')
  end
end

class CFConfig < Config
  attr_reader :system_domain, :username, :password, :space, :org, :app_domain

  def initialize
    @system_domain = load_or_raise('CF_SYSTEM_DOMAIN')
    @username = load_or_raise('CF_USERNAME')
    @password = load_or_raise('CF_PASSWORD')
    @space = load_or_raise('CF_SPACE')
    @org = load_or_raise('CF_ORG')
    @app_domain = load_or_raise('APP_DOMAIN')
  end
end

class CFClient
  def initialize(config)
    @config = config

    login!
  end

  def app_exists?(name)
    cmd = ["cf", "app", name]

    process, _ = exec(cmd)

    if process.exitstatus != 0
      false
    else
      true
    end
  end

  def app_url(name)
    process, guid = exec(["cf", "app", name, "--guid"])
    if process.exitstatus != 0
      raise "Failed to get app guid"
    end

    process, routes = exec_json(["cf", "curl", "/v2/apps/#{guid}/routes"])
    if process.exitstatus != 0
      raise "Failed to get app routes"
    end

    domain_url = routes["resources"].first["entity"]["domain_url"]

    process, domain_data = exec_json(["cf", "curl", domain_url])
    if process.exitstatus != 0
      raise "Failed to get domain name"
    end

    domain_name = domain_data["entity"]["name"]
    port = routes["resources"].first["entity"]["port"]
    if port
      host = routes["resources"].first["entity"]["host"]
      return "#{host}.#{domain_name}"
    else
      return "#{domain_name}:#{port}"
    end
  end

  def push(name)

  end

  private

  attr_reader :config

  def login!
    Logger.checkpoint("Logging into CF")

    cmd = ["cf", "login",
           "-a", "api.#{config.system_domain}",
           "-u", config.username,
           "-p", config.password,
           "-s", config.space,
           "-o", config.org,
           "--skip-ssl-validation"]

    process, _ = exec(cmd)

    if process.exitstatus != 0
      raise "Failed to login to CF"
    end
  end

  def exec(cmd)
    output = ""
    IO.popen(cmd, err: [:child, :out]) do |ls_io|
      line = ls_io.gets
      output << line
      puts line
    end

    [$?, output]
  end

  def exec_json(cmd)
    process, data = exec(cmd)

    if process.exitstatus != 0
      data = JSON.parse(data)
    end

    [process, data]
  end
end

class Smokes
  def self.run!
    cf_config = CFConfig.new
    smokes_config = SmokesConfig.new

    cf_client = CFClient.new(cf_config)

    new(cf_client, smokes_config).run!
  end

  def initialize(cf_client, config)
    @cf_client = cf_client
    @config = config
  end

  def run!
    ensure_counter_app!
  end

  private

  attr_reader :cf_client, :config

  def ensure_counter_app!
    Logger.checkpoint("Ensuring Counter App is Pushed")

    if cf_client.app_exists?(counter_app_name)
      # TODO: restart
    else
      # TODO: push
    end
  end

  def drain_app_name
    "ss-smoke-drain-#{config.job_name}"
  end

  def dripspinner_app_name
    "ss-smoke-dripspinner-#{config.job_name}"
  end

  def counter_app_name
    "ss-smoke-counter-#{config.job_name}"
  end

  def syslog_drain_service_name
    "ss-smoke-#{config.job_name}-#{config.drain_version}"
  end

  def syslog_drain_service_url
    "#{config.drain_type}://#{cf_client.app_url(drain_app_name)}/drain?drain-version=#{config.drain_version}"
  end
end

Smokes.run!
