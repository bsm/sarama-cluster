package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

func main() {
	client, err := sarama.NewClient([]string{"127.0.0.1:9092"}, nil)
	abortOn(err)
	defer client.Close()

	err = produce(client, 10000)
	abortOn(err)
	log.Println("Produced 10000 messages.")

	err = consume(client, 1000, func(message *sarama.ConsumerMessage) {
		fmt.Println(string(message.Value))
	})
	abortOn(err)
	log.Println("Consumed 1000 messages.")

	log.Println("Press ^C to exit.")
	select {}
}

func produce(client *sarama.Client, n int) error {
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 0; i < n; i++ {
		kv := sarama.StringEncoder(fmt.Sprintf("MESSAGE-%08d", i))
		if _, _, err := producer.SendMessage("my_topic", kv, kv); err != nil {
			return err
		}
	}
	return nil
}

func consume(client *sarama.Client, n int, cb func(*sarama.ConsumerMessage)) error {
	// Connect consumer
	config := cluster.Config{CommitEvery: time.Second}
	consumer, err := cluster.NewConsumerFromClient(client, []string{"localhost:2181"}, "my-group", "my-topic", &config)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Consume and print out errors in a separate goroutine
	go func() {
		for msg := range consumer.Errors() {
			fmt.Println("ERROR:", msg.Error())
		}
	}()

	// Process and ack messages
	counter := 0
	for msg := range consumer.Messages() {
		cb(msg)
		consumer.Ack(msg)

		if counter++; counter >= n {
			return nil
		}
	}
	return nil
}

func abortOn(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
