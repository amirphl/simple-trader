// Package journal
package journal

import (
	"context"
	"time"
)

// Event represents a journaled event.
type Event struct {
	Time        time.Time
	Type        string // e.g., "order", "signal", "error", etc.
	Description string
	Data        map[string]any
}

// Journaler interface for journaling events.
type Journaler interface {
	LogEvent(ctx context.Context, event Event) error
	GetEvents(ctx context.Context, eventType string, start, end time.Time) ([]Event, error)
}
