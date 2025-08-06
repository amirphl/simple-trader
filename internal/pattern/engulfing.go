package pattern

import (
	"fmt"
)

// EngulfingPattern detects bullish and bearish engulfing patterns
type EngulfingPattern struct {
	name        string
	description string
}

// NewEngulfingPattern creates a new engulfing pattern detector
func NewEngulfingPattern() *EngulfingPattern {
	return &EngulfingPattern{
		name:        "Engulfing",
		description: "Detects bullish and bearish engulfing patterns where one candle completely engulfs the previous candle",
	}
}

// Name returns the pattern name
func (e *EngulfingPattern) Name() string {
	return e.name
}

// Description returns the pattern description
func (e *EngulfingPattern) Description() string {
	return e.description
}

// Detect finds engulfing patterns in the given candles
func (e *EngulfingPattern) Detect(candles []Candle) ([]PatternMatch, error) {
	if len(candles) < 2 {
		return nil, fmt.Errorf("need at least 2 candles to detect engulfing patterns")
	}

	var matches []PatternMatch

	for i := 1; i < len(candles); i++ {
		current := candles[i]
		previous := candles[i-1]

		// Validate candles
		if err := ValidateCandle(current); err != nil {
			continue
		}
		if err := ValidateCandle(previous); err != nil {
			continue
		}

		// Check for bullish engulfing
		if e.isBullishEngulfing(current, previous) {
			strength := e.calculateBullishEngulfingStrength(current, previous)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Bullish Engulfing",
				Description: fmt.Sprintf("Bullish engulfing at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBullish),
				Timestamp:   current.Timestamp,
			})
		}

		// Check for bearish engulfing
		if e.isBearishEngulfing(current, previous) {
			strength := e.calculateBearishEngulfingStrength(current, previous)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Bearish Engulfing",
				Description: fmt.Sprintf("Bearish engulfing at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBearish),
				Timestamp:   current.Timestamp,
			})
		}
	}

	return matches, nil
}

// isBullishEngulfing checks if the current candle forms a bullish engulfing pattern
func (e *EngulfingPattern) isBullishEngulfing(current, previous Candle) bool {
	// Current candle must be bullish
	if !current.IsBullish() {
		return false
	}

	// Previous candle must be bearish
	if !previous.IsBearish() {
		return false
	}

	// Current candle's body must completely engulf previous candle's body
	currentBodyHigh := max(current.Open, current.Close)
	currentBodyLow := min(current.Open, current.Close)
	previousBodyHigh := max(previous.Open, previous.Close)
	previousBodyLow := min(previous.Open, previous.Close)

	return currentBodyHigh >= previousBodyHigh && currentBodyLow <= previousBodyLow
}

// isBearishEngulfing checks if the current candle forms a bearish engulfing pattern
func (e *EngulfingPattern) isBearishEngulfing(current, previous Candle) bool {
	// Current candle must be bearish
	if !current.IsBearish() {
		return false
	}

	// Previous candle must be bullish
	if !previous.IsBullish() {
		return false
	}

	// Current candle's body must completely engulf previous candle's body
	currentBodyHigh := max(current.Open, current.Close)
	currentBodyLow := min(current.Open, current.Close)
	previousBodyHigh := max(previous.Open, previous.Close)
	previousBodyLow := min(previous.Open, previous.Close)

	return currentBodyHigh >= previousBodyHigh && currentBodyLow <= previousBodyLow
}

// calculateBullishEngulfingStrength calculates the strength of a bullish engulfing pattern
func (e *EngulfingPattern) calculateBullishEngulfingStrength(current, previous Candle) float64 {
	currentBodySize := current.GetBodySize()
	previousBodySize := previous.GetBodySize()

	if previousBodySize == 0 {
		return float64(StrengthWeak)
	}

	// Calculate engulfing ratio
	engulfingRatio := currentBodySize / previousBodySize

	// Calculate strength based on engulfing ratio and volume
	strength := min(engulfingRatio/2.0, 1.0) // Cap at 1.0

	// Boost strength if current candle has high volume
	if current.Volume > previous.Volume*1.5 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if the engulfing is very strong
	if engulfingRatio > 3.0 {
		strength = min(strength*1.3, 1.0)
	}

	return max(strength, float64(StrengthWeak))
}

// calculateBearishEngulfingStrength calculates the strength of a bearish engulfing pattern
func (e *EngulfingPattern) calculateBearishEngulfingStrength(current, previous Candle) float64 {
	currentBodySize := current.GetBodySize()
	previousBodySize := previous.GetBodySize()

	if previousBodySize == 0 {
		return float64(StrengthWeak)
	}

	// Calculate engulfing ratio
	engulfingRatio := currentBodySize / previousBodySize

	// Calculate strength based on engulfing ratio and volume
	strength := min(engulfingRatio/2.0, 1.0) // Cap at 1.0

	// Boost strength if current candle has high volume
	if current.Volume > previous.Volume*1.5 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if the engulfing is very strong
	if engulfingRatio > 3.0 {
		strength = min(strength*1.3, 1.0)
	}

	return max(strength, float64(StrengthWeak))
}
