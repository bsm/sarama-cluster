/*
This package provides cluster extensions for Sarama, enabing users
to consume topics across from multiple, balanced nodes.

It follows the distribution recommendation and algorithm, described
in: http://kafka.apache.org/documentation.html#distributionimpl

Usage example:

  client, err := sarama.NewClient("my-client", []string{"127.0.0.1:29092"}, nil)
  if err != nil {
    log.Fatal(err)
  }
  defer client.Close()

  consumer, err := cluster.NewConsumer(client, []string{"localhost:22181"}, "my-group", "my-topic", &cluster.ConsumerConfig{
    CommitEvery: time.Second, // Enable periodic auto-commits
  })
  if err != nil {
    log.Fatal(err)
  }

  // Don't forget to close the consumer.
  // This will also trigger a commit.
  defer consumer.Close()

  for event := range consumer.Events() {
    fmt.Println(string(event.Value))  // Print to STDOUT
    consumer.Ack(event)               // Mark event as processed
  }
*/
package cluster
