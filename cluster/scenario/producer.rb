#!/usr/bin/env ruby
require 'bundler/setup'
require 'poseidon'

limit, offset = ARGV[0].to_i, ARGV[1].to_i
producer = Poseidon::Producer.new ["localhost:29092"], "poseidon-producer"

while limit > 0 do
  batch  = limit > 10000 ? 10000 : limit
  limit -= batch

  messages = (0...batch).map do
    num = offset.to_s.rjust(8, "0")
    offset += 1
    Poseidon::MessageToSend.new "my-topic", num, Time.now.to_s+num
  end

  10.times do
    ok = producer.send_messages messages
    break if ok
    sleep(1)
  end
end
