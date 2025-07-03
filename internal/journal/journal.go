package journal

import "time"

// Event represents a journaled event.
type Event struct {
	Time        time.Time
	Type        string // e.g., "order", "signal", "error", etc.
	Description string
	Data        map[string]any
}

// Journaler interface for journaling events.
type Journaler interface {
	LogEvent(event Event) error
	GetEvents(eventType string, start, end time.Time) ([]Event, error)
}

