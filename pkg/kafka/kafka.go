package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"

	"github.com/diyliv/read/config"
)

func NewKafkaConn(cfg *config.Config) (*kafka.Conn, error) {
	return kafka.DialLeader(
		context.Background(),
		"tcp",
		cfg.Kafka.Brokers[0],
		cfg.Kafka.Topic,
		cfg.Kafka.Partition)
}
