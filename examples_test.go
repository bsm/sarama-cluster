package cluster_test

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"

	"github.com/bsm/sarama-cluster"
    "github.com/Shopify/sarama"
)

// This example shows how to use the consumer to read messages
// from a multiple topics through a multiplexed channel.
func ExampleConsumer() {

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
				consumer.MarkOffset(msg, "") // mark message as processed
			}
		case <-signals:
			return
		}
	}
}

// This example shows how to use the consumer to read messages
// through individual partitions.
func ExampleConsumer_Partitions() {

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
					consumer.MarkOffset(msg, "") // mark message as processed
				}
			}(part)
		case <-signals:
			return
		}
	}
}

// This example shows how to use the consumer with
// topic whitelists.
func ExampleConfig_whitelist() {

	// init (custom) config, enable errors and notifications
	config := cluster.NewConfig()
	config.Group.Topics.Whitelist = regexp.MustCompile(`myservice.*`)

	// init consumer
	consumer, err := cluster.NewConsumer([]string{"127.0.0.1:9092"}, "my-consumer-group", nil, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// consume messages
	msg := <-consumer.Messages()
	fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
}

// This example shows how to use the consumer to
// consume msg from saram.Producer
func ExampleConsumer_SaramProducer(){
    config := cluster.NewConfig()

    config.Group.Return.Notifications = true
    brokers := []string{"127.0.0.1:9092"}
    topic := "my_topic"

    topics := []string{topic}
    consumer, err := cluster.NewConsumer(brokers, "my-consumer-group", topics, config)
    if err != nil {
        panic(err)
    }

    // consumer really init successful when the notifications is received and `ntf.Type==cluster.RebalanceOK`
    for {
        ntf, ok := <-consumer.Notifications()
        if !ok {
            panic("notification is close before consumer init")
        }
        if ntf.Type == cluster.RebalanceOK {
            log.Printf("Rebalanced: %+v\n", ntf)
            break
        } else if ntf.Type == cluster.RebalanceError {
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
        // input msg to kafka by saram.Producer
        producer.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(word)}

        // get msg from kafka, and do something with msg.
        msg := <-consumer.Messages()
        log.Printf("%+v\n", msg)
    }
}