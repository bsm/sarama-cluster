#!/usr/bin/env ruby

require 'bundler/setup'
require 'timeout'
require File.expand_path("../scenario", __FILE__)

# Start Zookeeper & Kafka
Scenario.run do
  5.times do
    produce 1000
  end
  consume "A"
  consume "B"
  consume "C"
  checkpoint!

  15.times { produce 1000 }
  consume "D"
  10.times { produce 1000 }
  consume "X"
  10.times { produce 1000 }
  checkpoint!

  20.times { produce 1000 }
  consume "E"
  consume "F"
  15.times { produce 1000 }
  consume "Y"
  50.times { produce 100 }
  20.times { produce 1000 }

  checkpoint!
end


