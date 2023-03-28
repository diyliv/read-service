package main

import (
	"context"

	"github.com/diyliv/read/config"
	"github.com/diyliv/read/internal/consumer"
	"github.com/diyliv/read/pkg/kafka"
	"github.com/diyliv/read/pkg/logger"
)

func main() {
	ctx := context.Background()
	cfg := config.ReadConfig("config", "yaml", "./config")
	logger := logger.InitLogger()
	kafkaConn, err := kafka.NewKafkaConn(cfg)
	if err != nil {
		panic(err)
	}
	defer kafkaConn.Close()
	cons := consumer.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID, logger, cfg)
	cons.ConsumeProduce(ctx)
}
