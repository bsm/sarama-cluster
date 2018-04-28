package cluster_test

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

func Example() {
	config := sarama.NewConfig()
	config.ClientID = "my-client"
	config.Version = sarama.V0_10_2_0
	config.Consumer.Return.Errors = true

	// define a (thread-safe) handler
	handler := cluster.HandlerFunc(func(pc cluster.PartitionConsumer) error {
		for msg := range pc.Messages() {
			fmt.Fprintf(os.Stdout, "%s-%d:%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
			pc.MarkMessage(msg, "custom metadata") // mark message as processed
		}
		return nil
	})

	// init consumer
	brokers := []string{"127.0.0.1:9092"}
	topics := []string{"my_topic", "other_topic"}
	consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config, handler)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %v\n", err)
		}
	}()

	// consume claims
	go func() {
		for claim := range consumer.Claims() {
			log.Printf("Claimed: %+v\n", claim.Topics)
		}
	}()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
}
