package cluster

import "github.com/dim/sarama"

// Config borrows many options from sarama.ConsumerConfig
type Config struct {
	// The default (maximum) amount of data to fetch from the broker in each request. The default of 0 is treated as 1024 bytes.
	DefaultFetchSize int32
	// The minimum amount of data to fetch in a request - the broker will wait until at least this many bytes are available.
	// The default of 0 is treated as 'at least one' to prevent the consumer from spinning when no messages are available.
	MinFetchSize int32
	// The maximum permittable message size - messages larger than this will return MessageTooLarge. The default of 0 is
	// treated as no limit.
	MaxMessageSize int32
	// The maximum amount of time (in ms) the broker will wait for MinFetchSize bytes to become available before it
	// returns fewer than that anyways. The default of 0 causes Kafka to return immediately, which is rarely desirable
	// as it causes the Consumer to spin when no events are available. 100-500ms is a reasonable range for most cases.
	// The default of 0 is treated as 100ms.
	MaxWaitTime int32

	// The method used to determine at which offset to begin consuming messages.
	OffsetMethod sarama.OffsetMethod
	// Interpreted differently according to the value of OffsetMethod.
	OffsetValue int64

	// The number of events to buffer in the Events channel. Setting this can let the
	// consumer continue fetching messages in the background while local code consumes events,
	// greatly improving throughput.
	EventBufferSize int

	// The minimum number of events to fetch - the consumer will wait until at least this many events are available
	EventMinCount int32
	// The maximum amount of time (in ms) the consumer will wait for EventMinCount to become available.
	// 100-500ms is a reasonable range for most cases. The default of 0 is treated as 100ms.
	EventWaitTime int32
}

// Validates configuration
func (c *Config) validate() error {
	if c.DefaultFetchSize < 0 {
		return sarama.ConfigurationError("Invalid DefaultFetchSize")
	} else if c.DefaultFetchSize == 0 {
		c.DefaultFetchSize = 1024
	}

	if c.MinFetchSize < 0 {
		return sarama.ConfigurationError("Invalid MinFetchSize")
	} else if c.MinFetchSize == 0 {
		c.MinFetchSize = 1
	}

	if c.EventMinCount < 0 {
		return sarama.ConfigurationError("Invalid EventMinCount")
	} else if c.EventMinCount == 0 {
		c.EventMinCount = 1
	}

	if c.MaxWaitTime < 0 {
		return sarama.ConfigurationError("Invalid MaxWaitTime")
	} else if c.MaxWaitTime == 0 {
		c.MaxWaitTime = 100
	} else if c.MaxWaitTime < 100 {
		sarama.Logger.Println("ConsumerConfig.MaxWaitTime is very low, which can cause high CPU and network usage. See sarama documentation for details.")
	}

	if c.EventWaitTime < 0 {
		return sarama.ConfigurationError("Invalid EventWaitTime")
	} else if c.EventWaitTime == 0 {
		c.EventWaitTime = 100
	} else if c.EventWaitTime < 100 {
		sarama.Logger.Println("Config.EventWaitTime is very low, which can cause high CPU and network usage. See sarama documentation for details.")
	}

	if c.MaxMessageSize < 0 {
		return sarama.ConfigurationError("Invalid MaxMessageSize")
	} else if c.EventBufferSize < 0 {
		return sarama.ConfigurationError("Invalid EventBufferSize")
	}

	return nil
}
