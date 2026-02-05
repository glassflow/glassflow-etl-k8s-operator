package notifications

import "context"

// Storage defines the interface for storing notifications
type Storage interface {
	StoreNotification(ctx context.Context, notification *Notification) error
}
