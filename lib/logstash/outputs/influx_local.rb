
# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "date"

# An influx-local output that does nothing.
class LogStash::Outputs::InfluxLocal < LogStash::Outputs::Base
  config_name "influx_local"

  public
  def register
  end # def register

  public
  def receive(event)
    cmd = 'insert commander_log,source=%s,log_level=%s,host=%s operation_context_id="%s",loging_class="%s",thread="%s",message="%s",operation_names="%s",timestamp=%s,job_id="%s",info="%s"'
    cmd = cmd % [
      event.get('source') || "",
      event.get('log_level') || "",
      event.get('host') || "",
      event.get('operation_context_id') || "",
      event.get('loging_class') || "",
      event.get('thread') || "",
      event.get('message') ? event.get('message').gsub('"', '\"').gsub("\n", " ") : "", # Escape quotes in message
      event.get('operation_names') || "",
      event.get('timestamp') ? Date.parse(event.get('timestamp')).strftime('%Q') : "", # Writting date in miliseconds
      event.get('job_id') || "",
      event.get('info') || ""
    ]

    # Escape quotes in command
    cmd.gsub!('"', '\"')
    # Complete escape in message
    cmd.gsub!('\\"', '\\\"')

    cmd = "influx -database \"commander_log\" -execute \"#{cmd}\""

    @logger.debug? and @logger.debug("influxdb_local: executing command: #{cmd}")
    out = `#{cmd}`
    if(out == "")
      @logger.debug? and @logger.debug("influxdb_local: command completed")
    else
      @logger.warn("influxdb_local: Non empty response: #{out}")
    end

  end # def event
end # class LogStash::Outputs::InfluxLocal
