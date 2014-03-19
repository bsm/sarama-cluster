require 'fileutils'
require 'pathname'

module Scenario
  extend self

  ROOT   = Pathname.new(File.expand_path("../", __FILE__))
  SERVER = ROOT.join "kafka_2.10-0.8.1"

  TOPIC_NAME = "my-topic"
  KAFKA_BIN  = SERVER.join("bin", "kafka-server-start.sh")
  KAFKA_CFG  = SERVER.join("config", "server-poseidon.properties")
  KAFKA_TMP  = "/tmp/kafka-logs-poseidon"
  ZOOKP_BIN  = SERVER.join("bin", "zookeeper-server-start.sh")
  ZOOKP_CFG  = SERVER.join("config", "zookeeper-poseidon.properties")
  ZOOKP_TMP  = "/tmp/zookeeper-poseidon"
  LOG4J_CFG  = SERVER.join("config", "log4j.properties")
  OUTPUT     = Scenario::ROOT.join("output.txt")

  @@pids     = {}
  @@total    = 0

  def run(&block)
    setup
    instance_eval(&block)
  rescue => e
    abort [e, *e.backtrace[0,20]].join("\n")
  ensure
    teardown
  end

  def setup
    FileUtils.rm_rf OUTPUT.to_s
    configure

    # Ensure all required files are present
    [KAFKA_BIN, ZOOKP_BIN, KAFKA_CFG, ZOOKP_CFG].each do |path|
      abort "Unable to locate #{path}. File does not exist!" unless path.file?
    end

    Signal.trap("INT") { teardown }

    spawn KAFKA_BIN, KAFKA_CFG
    spawn ZOOKP_BIN, ZOOKP_CFG
    sleep(2)
  end

  def teardown
    @@pids.each do |_, pid|
      Process.kill :TERM, pid
    end
    sleep(1)
    FileUtils.rm_rf KAFKA_TMP.to_s
    FileUtils.rm_rf ZOOKP_TMP.to_s

    fail! unless numlines == @@total
  end

  def configure
    download

    KAFKA_CFG.open("w") do |f|
      f.write SERVER.join("config", "server.properties").read.
        sub("=9092", "=29092").
        sub(":2181", ":22181").
        sub("num.partitions=2", "num.partitions=12").
        sub("log.flush.interval.ms=1000", "log.flush.interval.ms=10").
        sub("/tmp/kafka-logs", KAFKA_TMP)
    end
    ZOOKP_CFG.open("w") do |f|
      f.write SERVER.join("config", "zookeeper.properties").read.
        sub("/tmp/zookeeper", ZOOKP_TMP).
        sub("=2181", "=22181")
    end
    content = LOG4J_CFG.read
    LOG4J_CFG.open("w") do |f|
      f.write content.gsub("INFO", "FATAL")
    end if content.include?("INFO")
  end

  def download
    return if SERVER.directory?
    sh "cd #{ROOT} && curl http://www.mirrorservice.org/sites/ftp.apache.org/kafka/0.8.1/kafka_2.10-0.8.1.tgz | tar xz"
  end

  def checkpoint!(timeout = 10)
    puts "--> Verifying #{@@total}"
    timeout.times do
      if numlines > @@total
        break
      elsif numlines < @@total
        sleep(1)
      else
        return
      end
    end
    fail!
  end

  def consume(name)
    puts "--> Launching consumer #{name}"
    spawn "go run", ROOT.join("consumer.go"), name, OUTPUT
  end

  def produce(count)
    puts "--> Producing messages #{@@total}-#{@@total+count-1}"
    sh "go run", ROOT.join("producer.go"), count, @@total
    @@total += count
  end

  def numlines
    `wc -l #{OUTPUT} 2> /dev/null`.to_i
  end

  def abort(message)
    Kernel.abort "ERROR: #{message}"
  end

  def fail!
    Kernel.abort "FAILED: expected #{@@total} but was #{numlines}"
  end

  def sh(*bits)
    cmd = bits.join(" ")
    system(cmd) || abort(cmd)
  end

  def spawn(*args)
    cmd = args.join(" ")
    @@pids[cmd] = Process.spawn(cmd)
  end

end
