package errs

import (
	"errors"
	"fmt"
)

var (
	ErrPipelineNotFound                = errors.New("no pipeline was found")
	ErrPipelineAlreadyInTransition     = errors.New("pipeline already in transition")
	ErrPipelineInvalidStatusTransition = errors.New("invalid status transition")
	ErrUnknownStatus                   = errors.New("unknown status")
)

// ConsumerPendingMessagesError represents an error when a consumer has pending messages
type ConsumerPendingMessagesError struct {
	ConsumerName string
	PendingCount int
	UnackedCount int
}

func (e *ConsumerPendingMessagesError) Error() string {
	return fmt.Sprintf("consumer %s has %d pending and %d unacknowledged messages",
		e.ConsumerName, e.PendingCount, e.UnackedCount)
}

// NewConsumerPendingMessagesError creates a new ConsumerPendingMessagesError
func NewConsumerPendingMessagesError(consumerName string, pending, unacked int) *ConsumerPendingMessagesError {
	return &ConsumerPendingMessagesError{
		ConsumerName: consumerName,
		PendingCount: pending,
		UnackedCount: unacked,
	}
}

// IsConsumerPendingMessagesError checks if an error is a ConsumerPendingMessagesError
func IsConsumerPendingMessagesError(err error) bool {
	var pendingErr *ConsumerPendingMessagesError
	return errors.As(err, &pendingErr)
}
