/*
This package provides cluster extensions for Sarama, enabing users
to consume topics across from multiple, balanced nodes.

It follows the distribution recommendation and algorithm, described
in: http://kafka.apache.org/documentation.html#distributionimpl

Usage example:

  consumer, err := cluster.NewConsumer([]string{"127.0.0.1:9092"}, []string{"localhost:2181"}, "my-group", "my-topic", &cluster.ConsumerConfig{
    CommitEvery: time.Second, // Enable periodic auto-commits
  })
  if err != nil {
    log.Fatal(err)
  }

  // Don't forget to close the consumer.
  // This will also trigger a commit.
  defer consumer.Close()

  // You MUST consume errors (in a background goroutine) to avoid deadlocks.
  go func() {
    for msg := range consumer.Errors() {
      fmt.Println("ERROR:", msg.Error())
    }
  }()

  // Consume messages.
  for event := range consumer.Messages() {
    fmt.Println("MESSAGE:", string(event.Value))  // Print to STDOUT
    consumer.Ack(event)                           // Mark event as processed
  }
*/
package cluster
