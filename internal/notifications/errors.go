package notifications

import "fmt"

// ErrInvalidConfig represents an invalid configuration error
type ErrInvalidConfig struct {
	Field  string
	Reason string
}

func (e ErrInvalidConfig) Error() string {
	return fmt.Sprintf("invalid config field %s: %s", e.Field, e.Reason)
}

// ErrNotificationDisabled indicates that notifications are disabled
var ErrNotificationDisabled = fmt.Errorf("notifications are disabled")

// ErrInvalidNotification indicates that a notification is invalid
type ErrInvalidNotification struct {
	Reason string
}

func (e ErrInvalidNotification) Error() string {
	return fmt.Sprintf("invalid notification: %s", e.Reason)
}
