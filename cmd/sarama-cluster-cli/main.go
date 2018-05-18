package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

var (
	groupID    = flag.String("group", "", "REQUIRED: The shared consumer group name")
	brokerList = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster")
	topicList  = flag.String("topics", "", "REQUIRED: The comma separated list of topics to consume")
	offset     = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	verbose    = flag.Bool("verbose", false, "Whether to turn on sarama logging")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *groupID == "" {
		printUsageErrorAndExit("You have to provide a -group name.")
	} else if *brokerList == "" {
		printUsageErrorAndExit("You have to provide -brokers as a comma-separated list, or set the KAFKA_PEERS environment variable.")
	} else if *topicList == "" {
		printUsageErrorAndExit("You have to provide -topics as a comma-separated list.")
	}

	// Init config
	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	if *verbose {
		sarama.Logger = logger
	} else {
		config.Consumer.Return.Errors = true
	}

	switch *offset {
	case "oldest":
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case "newest":
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	brokers := strings.Split(*brokerList, ",")
	topics := strings.Split(*topicList, ",")

	// Init consumer, consume errors & messages
	consumer, err := cluster.NewConsumer(brokers, *groupID, topics, config, cluster.HandlerFunc(handler))
	if err != nil {
		printErrorAndExit(69, "Failed to start consumer: %s", err)
	}
	defer consumer.Close()

	// Consume (and ignore) claims channel
	go func() {
		for range consumer.Claims() {
		}
	}()

	// Consume errors channel and log
	go func() {
		for err := range consumer.Errors() {
			logger.Printf("Error: %v\n", err)
		}
	}()

	// Create signal channel, wait for shutdown
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	<-shutdown
}

func handler(pc cluster.PartitionConsumer) error {
	for msg := range pc.Messages() {
		fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		pc.MarkMessage(msg, "")
	}
	return nil
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n\n", values...)
	os.Exit(code)
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n\n", values...)
	flag.Usage()
	os.Exit(64)
}
