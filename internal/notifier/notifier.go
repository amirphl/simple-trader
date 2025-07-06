// Package notifier
package notifier

// Notifier interface for sending notifications (e.g., Telegram, email).
type Notifier interface {
	Send(message string) error
}

