#!/usr/bin/env ruby
require 'bundler/setup'
require 'poseidon_cluster'

name     = ARGV[0].to_s
output   = File.open(ARGV[1], "a")
output.sync = true

total    = 0
consumer = Poseidon::ConsumerGroup.new "my-group", ["localhost:29092"], ["localhost:22181"], "my-topic", max_bytes: 256*1024
consumer.fetch_loop do |n, messages|
  break if name[0] > 'Q' && total > 0
  messages.each do |m|
    output.write "#{name},#{n},#{m.value}\n"
  end
  total += messages.size
end
