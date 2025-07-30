package tfutils

import (
	"errors"
	"time"
)

// ParseTimeframe parses timeframe string (e.g., "5m", "1h") to time.Duration
func ParseTimeframe(timeframe string) (time.Duration, error) {
	switch timeframe {
	case "1m":
		return time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, errors.New("unsupported timeframe")
	}
}

// GetTimeframeDuration returns the duration for a given timeframe
func GetTimeframeDuration(timeframe string) time.Duration {
	switch timeframe {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "4h":
		return 4 * time.Hour
	case "1d":
		return 24 * time.Hour
	default:
		return 0
	}
}

func TimeframeMinutes(timeframe string) int {
	switch timeframe {
	case "1m":
		return 1
	case "5m":
		return 5
	case "15m":
		return 15
	case "30m":
		return 30
	case "1h":
		return 60
	case "4h":
		return 4 * 60
	case "1d":
		return 24 * 4 * 60
	default:
		return 0
	}
}

// GetSupportedTimeframes returns all supported timeframes
func GetSupportedTimeframes() []string {
	return []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
}

func GetAggregationTimeframes() []string {
	return []string{"5m", "15m", "30m", "1h", "4h", "1d"}
}

// IsValidTimeframe checks if a timeframe is supported
func IsValidTimeframe(timeframe string) bool {
	return GetTimeframeDuration(timeframe) > 0
}
