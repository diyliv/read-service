package interfaces

import "context"

type Consumer interface {
	ConsumeProduce(ctx context.Context) error
}
