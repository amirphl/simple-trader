// Package notifier
package notifier

// Notifier interface for sending notifications (e.g., Telegram, email).
type Notifier interface {
	Send(msg string) error
	SendWithRetry(msg string) error
	RetryWithNotification(action func() error, description string) error
}
