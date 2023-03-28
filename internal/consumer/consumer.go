package consumer

import (
	"context"
	"crypto/sha256"
	"encoding/json"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"go.uber.org/zap"

	"github.com/diyliv/read/config"
	"github.com/diyliv/read/internal/models"
)

type consumer struct {
	logger *zap.Logger
	reader *kafka.Reader
	writer *kafka.Writer
}

func NewConsumer(kafkaURL []string, topic, groupID string, logger *zap.Logger, cfg *config.Config) *consumer {
	return &consumer{
		logger: logger,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:                kafkaURL,
			GroupID:                groupID,
			Topic:                  topic,
			MinBytes:               minBytes,
			MaxBytes:               maxBytes,
			QueueCapacity:          queueCapacity,
			HeartbeatInterval:      heartbeatInterval,
			CommitInterval:         commitInterval,
			PartitionWatchInterval: partitionWatchInterval,
			Logger:                 kafka.LoggerFunc(logger.Sugar().Debugf),
			ErrorLogger:            kafka.LoggerFunc(logger.Sugar().Errorf),
			MaxAttempts:            maxAttempts,
			Dialer: &kafka.Dialer{
				Timeout: dialTimeout,
			},
		}),
		writer: &kafka.Writer{
			Addr:         kafka.TCP(cfg.Kafka.Brokers...),
			Topic:        cfg.Kafka.WriteTo,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: writerRequiredAcks,
			MaxAttempts:  writerMaxAttempts,
			Logger:       kafka.LoggerFunc(logger.Sugar().Debugf),
			ErrorLogger:  kafka.LoggerFunc(logger.Sugar().Errorf),
			Compression:  compress.Snappy,
			ReadTimeout:  writerReadTimeout,
			WriteTimeout: writerWriteTimeout,
		},
	}
}

func (c *consumer) ConsumeProduce(ctx context.Context) error {
	var (
		scan     models.OPCDA
		respScan models.Response
	)

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			c.logger.Error("Error while reading messages: " + err.Error())
			return err
		}
		if err := json.Unmarshal(msg.Value, &scan); err != nil {
			c.logger.Error("Error while unmarshalling: " + err.Error())
			return err
		}

		ch := make(chan models.Response)
		go func() {
			guid := uuid.NewHash(sha256.New(), uuid.New(), []byte(scan.ServerName+scan.TagName), 4)
			respScan.AssetId = guid.String()
			respScan.TagType = scan.TagType
			respScan.TagValue = scan.TagValue
			respScan.TagQuality = scan.TagQuality
			respScan.ReadAt = scan.ReadAt
			ch <- respScan
		}()

		go func() {
			respBytes, err := marshal(<-ch)
			if err != nil {
				c.logger.Error("Error while marshalling: " + err.Error())
			}
			if err := c.writer.WriteMessages(ctx, kafka.Message{Value: respBytes}); err != nil {
				c.logger.Error("Error while writing message: " + err.Error())
			}
		}()
	}
}

func marshal(data interface{}) ([]byte, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return dataBytes, err
}
