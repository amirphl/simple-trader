package pattern

import (
	"fmt"
)

// HammerPattern detects hammer and hanging man patterns
type HammerPattern struct {
	name        string
	description string
}

// NewHammerPattern creates a new hammer pattern detector
func NewHammerPattern() *HammerPattern {
	return &HammerPattern{
		name:        "Hammer",
		description: "Detects hammer (bullish) and hanging man (bearish) patterns with long lower shadows and small bodies",
	}
}

// Name returns the pattern name
func (h *HammerPattern) Name() string {
	return h.name
}

// Description returns the pattern description
func (h *HammerPattern) Description() string {
	return h.description
}

// Detect finds hammer patterns in the given candles
func (h *HammerPattern) Detect(candles []Candle) ([]PatternMatch, error) {
	if len(candles) < 1 {
		return nil, fmt.Errorf("need at least 1 candle to detect hammer patterns")
	}

	var matches []PatternMatch

	for i := range len(candles) {
		current := candles[i]

		// Validate candle
		if err := ValidateCandle(current); err != nil {
			continue
		}

		// Check for hammer (bullish)
		if h.isHammer(current) {
			strength := h.calculateHammerStrength(current)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Hammer",
				Description: fmt.Sprintf("Bullish hammer at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBullish),
				Timestamp:   current.Timestamp,
			})
		}

		// Check for hanging man (bearish) - only if not already detected as hammer
		if !h.isHammer(current) && h.isHangingMan(current) {
			strength := h.calculateHangingManStrength(current)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Hanging Man",
				Description: fmt.Sprintf("Bearish hanging man at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBearish),
				Timestamp:   current.Timestamp,
			})
		}
	}

	return matches, nil
}

// isHammer checks if the candle forms a hammer pattern (bullish)
func (h *HammerPattern) isHammer(c Candle) bool {
	// Hammer must have a small body
	if c.GetBodyRatio() > 0.3 {
		return false
	}

	// Hammer must have a long lower shadow (at least 2x the body size)
	lowerShadow := c.GetLowerShadow()
	bodySize := c.GetBodySize()
	if bodySize == 0 {
		return false
	}
	if lowerShadow/bodySize < 2.0 {
		return false
	}

	// Hammer must have a very small upper shadow (less than 10% of total range)
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio > 0.1 {
		return false
	}

	// For hammer, the close should be near the high (bullish)
	closeToHighRatio := (c.High - c.Close) / c.GetTotalRange()
	return closeToHighRatio < 0.1
}

// isHangingMan checks if the candle forms a hanging man pattern (bearish)
func (h *HammerPattern) isHangingMan(c Candle) bool {
	// Hanging man must have a small body
	if c.GetBodyRatio() > 0.3 {
		return false
	}

	// Hanging man must have a long lower shadow (at least 2x the body size)
	lowerShadow := c.GetLowerShadow()
	bodySize := c.GetBodySize()
	if bodySize == 0 {
		return false
	}
	if lowerShadow/bodySize < 2.0 {
		return false
	}

	// Hanging man must have a very small upper shadow (less than 10% of total range)
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio > 0.1 {
		return false
	}

	// For hanging man, the close should be near the low (bearish)
	closeToLowRatio := (c.Close - c.Low) / c.GetTotalRange()
	return closeToLowRatio < 0.1
}

// calculateHammerStrength calculates the strength of a hammer pattern
func (h *HammerPattern) calculateHammerStrength(c Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength based on lower shadow length
	lowerShadowRatio := c.GetLowerShadowRatio()
	if lowerShadowRatio > 0.6 {
		strength = float64(StrengthStrong)
	} else if lowerShadowRatio > 0.4 {
		strength = float64(StrengthMedium)
	}

	// Boost strength if body is very small
	bodyRatio := c.GetBodyRatio()
	if bodyRatio < 0.1 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if upper shadow is very small
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio < 0.05 {
		strength = min(strength*1.1, 1.0)
	}

	// Boost strength if close is very close to high
	closeToHighRatio := (c.High - c.Close) / c.GetTotalRange()
	if closeToHighRatio < 0.05 {
		strength = min(strength*1.1, 1.0)
	}

	return strength
}

// calculateHangingManStrength calculates the strength of a hanging man pattern
func (h *HammerPattern) calculateHangingManStrength(c Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength based on lower shadow length
	lowerShadowRatio := c.GetLowerShadowRatio()
	if lowerShadowRatio > 0.6 {
		strength = float64(StrengthStrong)
	} else if lowerShadowRatio > 0.4 {
		strength = float64(StrengthMedium)
	}

	// Boost strength if body is very small
	bodyRatio := c.GetBodyRatio()
	if bodyRatio < 0.1 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if upper shadow is very small
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio < 0.05 {
		strength = min(strength*1.1, 1.0)
	}

	// Boost strength if close is very close to low
	closeToLowRatio := (c.Close - c.Low) / c.GetTotalRange()
	if closeToLowRatio < 0.05 {
		strength = min(strength*1.1, 1.0)
	}

	return strength
}
