# Sarama Cluster [![Build Status](https://travis-ci.org/bsm/sarama-cluster.png)](https://travis-ci.org/bsm/sarama-cluster)

Cluster extensions for [Sarama](https://github.com/Shopify/sarama), the Go client library for Apache Kafka 0.9 (and later).

## Documentation

Documentation and example are available via godoc at http://godoc.org/github.com/bsm/sarama-cluster

## Running tests

You need to install Ginkgo & Gomega to run tests. Please see
http://onsi.github.io/ginkgo for more details.

To run tests, call:

    $ make test

## Troubleshooting

### Consumer not receiving any messages?
By default, sarama's `Config.Consumer.Offsets.Initial` is set to `sarama.OffsetNewest`. This means that in the event that a brand new consumer is created, and it has never committed any offsets to kafka, it will only receive messages starting from the message after the current one that was written.

If you wish to receive all messages (from the start of all messages in the topic) in the event that a consumer does not have any offsets committed to kafka, you need to set `Config.Consumer.Offsets.Initial` to `sarama.OffsetOldest.

## Licence

    (The MIT License)

    Copyright (c) 2016 Black Square Media Ltd

    Permission is hereby granted, free of charge, to any person obtaining
    a copy of this software and associated documentation files (the
    'Software'), to deal in the Software without restriction, including
    without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to
    permit persons to whom the Software is furnished to do so, subject to
    the following conditions:

    The above copyright notice and this permission notice shall be
    included in all copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
    EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
    MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
    IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
    CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
    TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
    SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
