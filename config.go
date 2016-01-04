package cluster

import (
	"time"

	"github.com/Shopify/sarama"
)

// Config extends sarama.Config with Group specific namespace
type Config struct {
	sarama.Config

	// Group is the namespace for group management properties
	Group struct {
		// The strategy to use for the allocation of partitions to consumers (defaults to StrategyRange)
		PartitionStrategy Strategy
		Offsets           struct {
			Retry struct {
				// The numer retries when comitting offsets (defaults to 3).
				Max int
			}
		}
		Session struct {
			// The allowed session timeout for registered consumers (defaults to 30s).
			// Must be within the allowed server range.
			Timeout time.Duration
		}
		Heartbeat struct {
			// Interval between each heartbeat (defaults to 3s). It should be no more
			// than 1/3rd of the Group.Session.Timout setting
			Interval time.Duration
		}
	}
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig() *Config {
	c := &Config{
		Config: *sarama.NewConfig(),
	}
	c.Group.PartitionStrategy = StrategyRange
	c.Group.Offsets.Retry.Max = 3
	c.Group.Session.Timeout = 30 * time.Second
	c.Group.Heartbeat.Interval = 3 * time.Second
	return c
}

// Validate checks a Config instance. It will return a
// sarama.ConfigurationError if the specified values don't make sense.
func (c *Config) Validate() error {
	if c.Group.Heartbeat.Interval%time.Millisecond != 0 {
		sarama.Logger.Println("Group.Heartbeat.Interval only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Group.Session.Timeout%time.Millisecond != 0 {
		sarama.Logger.Println("Group.Session.Timeout only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Group.PartitionStrategy != StrategyRange && c.Group.PartitionStrategy != StrategyRoundRobin {
		sarama.Logger.Println("Group.PartitionStrategy is not supported; range will be assumed.")
	}
	if err := c.Config.Validate(); err != nil {
		return err
	}

	// validate the Group values
	switch {
	case c.Group.Offsets.Retry.Max < 0:
		return sarama.ConfigurationError("Group.Offsets.Retry.Max must be >= 0")
	case c.Group.Heartbeat.Interval <= 0:
		return sarama.ConfigurationError("Group.Heartbeat.Interval must be > 0")
	case c.Group.Session.Timeout <= 0:
		return sarama.ConfigurationError("Group.Session.Timeout must be > 0")
	}

	// ensure offset is correct
	switch c.Consumer.Offsets.Initial {
	case sarama.OffsetOldest, sarama.OffsetNewest:
	default:
		return sarama.ConfigurationError("Consumer.Offsets.Initial must be either OffsetOldest or OffsetNewest")
	}

	return nil
}
