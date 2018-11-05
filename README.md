# Sarama Cluster

[![GoDoc](https://godoc.org/github.com/bsm/sarama-cluster?status.svg)](https://godoc.org/github.com/bsm/sarama-cluster)
[![Build Status](https://travis-ci.org/bsm/sarama-cluster.svg?branch=master)](https://travis-ci.org/bsm/sarama-cluster)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/sarama-cluster)](https://goreportcard.com/report/github.com/bsm/sarama-cluster)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Cluster extensions for [Sarama](https://github.com/Shopify/sarama), the Go client library for Apache Kafka 0.9 (and later).

## Documentation

Documentation and example are available via godoc at http://godoc.org/github.com/bsm/sarama-cluster

## Examples

Consumers have two modes of operation. In the default multiplexed mode messages (and errors) of multiple
topics and partitions are all passed to the single channel:

```go
package main

import (
    "fmt"
    "log"
    "os"
    "os/signal"

    cluster "github.com/bsm/sarama-cluster"
)

func main() {

    // init (custom) config, enable errors and notifications
    config := cluster.NewConfig()
    config.Consumer.Return.Errors = true
    config.Group.Return.Notifications = true

    // init consumer
    brokers := []string{"127.0.0.1:9092"}
    topics := []string{"my_topic", "other_topic"}
    consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    // trap SIGINT to trigger a shutdown.
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    // consume errors
    go func() {
        for err := range consumer.Errors() {
            log.Printf("Error: %s\n", err.Error())
        }
    }()

    // consume notifications
    go func() {
        for ntf := range consumer.Notifications() {
            log.Printf("Rebalanced: %+v\n", ntf)
        }
    }()

    // consume messages, watch signals
    for {
        select {
        case msg, ok := <-consumer.Messages():
            if ok {
                fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
                consumer.MarkOffset(msg, "")    // mark message as processed
            }
        case <-signals:
            return
        }
    }
}
```

Users who require access to individual partitions can use the partitioned mode which exposes access to partition-level
consumers:

```go
package main

import (
  "fmt"
  "log"
  "os"
  "os/signal"

  cluster "github.com/bsm/sarama-cluster"
)

func main() {

    // init (custom) config, set mode to ConsumerModePartitions
    config := cluster.NewConfig()
    config.Group.Mode = cluster.ConsumerModePartitions

    // init consumer
    brokers := []string{"127.0.0.1:9092"}
    topics := []string{"my_topic", "other_topic"}
    consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)
    if err != nil {
        panic(err)
    }
    defer consumer.Close()

    // trap SIGINT to trigger a shutdown.
    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    // consume partitions
    for {
        select {
        case part, ok := <-consumer.Partitions():
            if !ok {
                return
            }

            // start a separate goroutine to consume messages
            go func(pc cluster.PartitionConsumer) {
                for msg := range pc.Messages() {
                    fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
                    consumer.MarkOffset(msg, "")    // mark message as processed
                }
            }(part)
        case <-signals:
            return
        }
    }
}
```

if you want to receive message from `Producer created by sarama`, you should init your consumer like this:

```go
package main

import (
    "fmt"

    "github.com/Shopify/sarama"
    "github.com/bsm/sarama-cluster"
)

func main() {
    config := cluster.NewConfig()

    config.Group.Return.Notifications = true
    brokers := []string{"127.0.0.1:9092"}
    topic := "test"

    topics := []string{topic}
    consumer, err := cluster.NewConsumer(brokers, "test-consumer-group", topics, config)
    if err != nil {
        panic(err)
    }

    // consumer really init successful when the notifications is received and `ntf.Type==cluster.RebalanceOK`
    for{
        ntf, ok := <-consumer.Notifications()
        if !ok {
            panic("notification is close before consumer init")
        }
        if ntf.Type == cluster.RebalanceOK{
            break
        } else if ntf.Type == cluster.RebalanceError{
            panic("consumer init failed")
        }
    }
    defer consumer.Close()

    producer, err := sarama.NewAsyncProducer(brokers, &config.Config)
    if err != nil {
        panic(err)
    }
    defer producer.Close()

    var testData = []string{"The", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"}
    for _, word := range testData {
       producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(word)}
       // get msg from kafka, and do something with msg.
       msg := <-consumer.Messages()
    }
}
```

kafka will return a notifications via `consumer.Notifications()` when consumer init successfully in kafka. you should wait this notifications and confirm field `Type` of it is equal to `cluster.RebalanceOK`.

if you don't wait the notifications, the msg inputted by `producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(word)}` maybe regard as an old msg before your new consumer, and new consumer will never get this msg unless you set `config.Consumer.Offsets.Initial` to `sarama.OffsetOldest`, but it will get all old msg.

## Running tests

You need to install Ginkgo & Gomega to run tests. Please see
http://onsi.github.io/ginkgo for more details.

To run tests, call:

    $ make test

## Troubleshooting

### Consumer not receiving any messages?

By default, sarama's `Config.Consumer.Offsets.Initial` is set to `sarama.OffsetNewest`. This means that in the event that a brand new consumer is created, and it has never committed any offsets to kafka, it will only receive messages starting from the message after the current one that was written.

If you wish to receive all messages (from the start of all messages in the topic) in the event that a consumer does not have any offsets committed to kafka, you need to set `Config.Consumer.Offsets.Initial` to `sarama.OffsetOldest`.
