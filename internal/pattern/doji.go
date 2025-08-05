package pattern

import (
	"fmt"
)

// DojiPattern detects various types of doji patterns
type DojiPattern struct {
	name        string
	description string
}

// NewDojiPattern creates a new doji pattern detector
func NewDojiPattern() *DojiPattern {
	return &DojiPattern{
		name:        "Doji",
		description: "Detects various doji patterns including standard doji, long-legged doji, dragonfly doji, and gravestone doji",
	}
}

// Name returns the pattern name
func (d *DojiPattern) Name() string {
	return d.name
}

// Description returns the pattern description
func (d *DojiPattern) Description() string {
	return d.description
}

// Detect finds doji patterns in the given candles
func (d *DojiPattern) Detect(candles []Candle) ([]PatternMatch, error) {
	if len(candles) < 1 {
		return nil, fmt.Errorf("need at least 1 candle to detect doji patterns")
	}

	var matches []PatternMatch

	for i := range len(candles) {
		current := candles[i]

		// Validate candle
		if err := ValidateCandle(current); err != nil {
			continue
		}

		// Check for dragonfly doji first (most specific)
		if d.isDragonflyDoji(current) {
			strength := d.calculateDragonflyDojiStrength(current)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Dragonfly Doji",
				Description: fmt.Sprintf("Dragonfly doji at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBullish),
				Timestamp:   current.Timestamp,
			})
		} else if d.isGravestoneDoji(current) {
			// Check for gravestone doji
			strength := d.calculateGravestoneDojiStrength(current)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Gravestone Doji",
				Description: fmt.Sprintf("Gravestone doji at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBearish),
				Timestamp:   current.Timestamp,
			})
		} else if d.isLongLeggedDoji(current) {
			// Check for long-legged doji
			strength := d.calculateLongLeggedDojiStrength(current)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Long-Legged Doji",
				Description: fmt.Sprintf("Long-legged doji at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeNeutral),
				Timestamp:   current.Timestamp,
			})
		} else if d.isStandardDoji(current) {
			// Check for standard doji last (least specific)
			strength := d.calculateStandardDojiStrength(current)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Standard Doji",
				Description: fmt.Sprintf("Standard doji at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeNeutral),
				Timestamp:   current.Timestamp,
			})
		}
	}

	return matches, nil
}

// isStandardDoji checks if the candle forms a standard doji pattern
func (d *DojiPattern) isStandardDoji(c Candle) bool {
	// Standard doji has a very small body
	if !c.IsDoji() {
		return false
	}

	// Standard doji should have moderate shadows
	upperShadowRatio := c.GetUpperShadowRatio()
	lowerShadowRatio := c.GetLowerShadowRatio()

	// Both shadows should be present but not too long
	// Standard doji should not be classified as other specific types
	return upperShadowRatio > 0.1 && upperShadowRatio < 0.4 &&
		lowerShadowRatio > 0.1 && lowerShadowRatio < 0.4 &&
		upperShadowRatio > 0.05 && // Not dragonfly
		lowerShadowRatio > 0.05 // Not gravestone
}

// isLongLeggedDoji checks if the candle forms a long-legged doji pattern
func (d *DojiPattern) isLongLeggedDoji(c Candle) bool {
	// Long-legged doji has a very small body
	if !c.IsDoji() {
		return false
	}

	// Long-legged doji has very long shadows
	upperShadowRatio := c.GetUpperShadowRatio()
	lowerShadowRatio := c.GetLowerShadowRatio()

	// Both shadows should be long (more than 40% of total range)
	return upperShadowRatio > 0.4 && lowerShadowRatio > 0.4
}

// isDragonflyDoji checks if the candle forms a dragonfly doji pattern
func (d *DojiPattern) isDragonflyDoji(c Candle) bool {
	// Dragonfly doji has a very small body
	if !c.IsDoji() {
		return false
	}

	// Dragonfly doji has no upper shadow (or very small)
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio > 0.05 {
		return false
	}

	// Dragonfly doji has a long lower shadow
	lowerShadowRatio := c.GetLowerShadowRatio()
	return lowerShadowRatio > 0.3
}

// isGravestoneDoji checks if the candle forms a gravestone doji pattern
func (d *DojiPattern) isGravestoneDoji(c Candle) bool {
	// Gravestone doji has a very small body
	if !c.IsDoji() {
		return false
	}

	// Gravestone doji has a long upper shadow
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio < 0.3 {
		return false
	}

	// Gravestone doji has no lower shadow (or very small)
	lowerShadowRatio := c.GetLowerShadowRatio()
	return lowerShadowRatio < 0.05
}

// calculateStandardDojiStrength calculates the strength of a standard doji pattern
func (d *DojiPattern) calculateStandardDojiStrength(c Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength if body is very small
	bodyRatio := c.GetBodyRatio()
	if bodyRatio < 0.05 {
		strength = float64(StrengthStrong)
	} else if bodyRatio < 0.1 {
		strength = float64(StrengthMedium)
	}

	// Boost strength if shadows are well-balanced
	upperShadowRatio := c.GetUpperShadowRatio()
	lowerShadowRatio := c.GetLowerShadowRatio()
	shadowBalance := 1.0 - abs(upperShadowRatio-lowerShadowRatio)
	if shadowBalance > 0.8 {
		strength = min(strength*1.2, 1.0)
	}

	return strength
}

// calculateLongLeggedDojiStrength calculates the strength of a long-legged doji pattern
func (d *DojiPattern) calculateLongLeggedDojiStrength(c Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength if body is very small
	bodyRatio := c.GetBodyRatio()
	if bodyRatio < 0.05 {
		strength = float64(StrengthStrong)
	} else if bodyRatio < 0.1 {
		strength = float64(StrengthMedium)
	}

	// Boost strength if shadows are very long
	upperShadowRatio := c.GetUpperShadowRatio()
	lowerShadowRatio := c.GetLowerShadowRatio()
	totalShadowRatio := upperShadowRatio + lowerShadowRatio

	if totalShadowRatio > 0.9 {
		strength = min(strength*1.3, 1.0)
	} else if totalShadowRatio > 0.8 {
		strength = min(strength*1.2, 1.0)
	}

	return strength
}

// calculateDragonflyDojiStrength calculates the strength of a dragonfly doji pattern
func (d *DojiPattern) calculateDragonflyDojiStrength(c Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength if body is very small
	bodyRatio := c.GetBodyRatio()
	if bodyRatio < 0.05 {
		strength = float64(StrengthStrong)
	} else if bodyRatio < 0.1 {
		strength = float64(StrengthMedium)
	}

	// Boost strength if upper shadow is very small
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio < 0.02 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if lower shadow is very long
	lowerShadowRatio := c.GetLowerShadowRatio()
	if lowerShadowRatio > 0.6 {
		strength = min(strength*1.3, 1.0)
	} else if lowerShadowRatio > 0.4 {
		strength = min(strength*1.1, 1.0)
	}

	return strength
}

// calculateGravestoneDojiStrength calculates the strength of a gravestone doji pattern
func (d *DojiPattern) calculateGravestoneDojiStrength(c Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength if body is very small
	bodyRatio := c.GetBodyRatio()
	if bodyRatio < 0.05 {
		strength = float64(StrengthStrong)
	} else if bodyRatio < 0.1 {
		strength = float64(StrengthMedium)
	}

	// Boost strength if lower shadow is very small
	lowerShadowRatio := c.GetLowerShadowRatio()
	if lowerShadowRatio < 0.02 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if upper shadow is very long
	upperShadowRatio := c.GetUpperShadowRatio()
	if upperShadowRatio > 0.6 {
		strength = min(strength*1.3, 1.0)
	} else if upperShadowRatio > 0.4 {
		strength = min(strength*1.1, 1.0)
	}

	return strength
}
