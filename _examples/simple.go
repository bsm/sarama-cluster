package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

func main() {
	client, err := sarama.NewClient("my-client", []string{"127.0.0.1:29092"}, nil)
	abortOn(err)
	defer client.Close()

	err = produce(client, 10000)
	abortOn(err)
	log.Println("Produced 10000 events.")

	err = consume(client, 1000, func(event *sarama.ConsumerEvent) {
		fmt.Println(string(event.Value))
	})
	abortOn(err)
	log.Println("Consumed 1000 events.")

	log.Println("Press ^C to exit.")
	select {}
}

func produce(client *sarama.Client, n int) error {
	producer, err := sarama.NewSimpleProducer(client, "my-topic", nil)
	if err != nil {
		return err
	}
	defer producer.Close()

	for i := 0; i < n; i++ {
		kv := sarama.StringEncoder(fmt.Sprintf("MESSAGE-%08d", i))
		if err := producer.SendMessage(kv, kv); err != nil {
			return err
		}
	}
	return nil
}

func consume(client *sarama.Client, n int, cb func(*sarama.ConsumerEvent)) error {
	config := cluster.ConsumerConfig{CommitEvery: time.Second}
	consumer, err := cluster.NewConsumer(client, []string{"localhost:22181"}, "my-group", "my-topic", &config)
	if err != nil {
		return err
	}
	defer consumer.Close()

	counter := 0
	for event := range consumer.Events() {
		cb(event)
		consumer.Ack(event)

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
