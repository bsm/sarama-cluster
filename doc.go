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

  consumer, err := cluster.NewConsumer(client, []string{"localhost:22181"}, "my-group", "my-topic", nil, nil)
  if err != nil {
    log.Fatal(err)
  }
  defer consumer.Close()

  // Auto-commit offset every second
  stopper := make(chan struct{})
  go func() {
    for {
      select {
      case <-stopper:
        return
      case <-time.After(time.Second):
      }
      consumer.Commit()
    }
  }()

  for event := range consumer.Events() {
    fmt.Println(string(event.Value))  // Print to STDOUT
    consumer.Ack(event)               // Mark event as processed
  }
  close(stopper)
*/
package http
