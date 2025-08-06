package pattern

import (
	"fmt"
)

// MorningEveningStarPattern detects morning star and evening star patterns
type MorningEveningStarPattern struct {
	name        string
	description string
}

// NewMorningEveningStarPattern creates a new morning/evening star pattern detector
func NewMorningEveningStarPattern() *MorningEveningStarPattern {
	return &MorningEveningStarPattern{
		name:        "Morning/Evening Star",
		description: "Detects morning star (bullish reversal) and evening star (bearish reversal) patterns",
	}
}

// Name returns the pattern name
func (m *MorningEveningStarPattern) Name() string {
	return m.name
}

// Description returns the pattern description
func (m *MorningEveningStarPattern) Description() string {
	return m.description
}

// Detect finds morning star and evening star patterns in the given candles
func (m *MorningEveningStarPattern) Detect(candles []Candle) ([]PatternMatch, error) {
	if len(candles) < 3 {
		return nil, fmt.Errorf("need at least 3 candles to detect morning/evening star patterns")
	}

	var matches []PatternMatch

	for i := 2; i < len(candles); i++ {
		first := candles[i-2]
		second := candles[i-1]
		third := candles[i]

		// Validate candles
		if err := ValidateCandle(first); err != nil {
			continue
		}
		if err := ValidateCandle(second); err != nil {
			continue
		}
		if err := ValidateCandle(third); err != nil {
			continue
		}

		// Check for morning star (bullish reversal)
		if m.isMorningStar(first, second, third) {
			strength := m.calculateMorningStarStrength(first, second, third)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Morning Star",
				Description: fmt.Sprintf("Morning star at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBullish),
				Timestamp:   third.Timestamp,
			})
		}

		// Check for evening star (bearish reversal)
		if m.isEveningStar(first, second, third) {
			strength := m.calculateEveningStarStrength(first, second, third)
			matches = append(matches, PatternMatch{
				Index:       i,
				Pattern:     "Evening Star",
				Description: fmt.Sprintf("Evening star at index %d", i),
				Strength:    strength,
				Direction:   string(PatternTypeBearish),
				Timestamp:   third.Timestamp,
			})
		}
	}

	return matches, nil
}

// isMorningStar checks if the three candles form a morning star pattern
func (m *MorningEveningStarPattern) isMorningStar(first, second, third Candle) bool {
	// First candle should be bearish (downtrend)
	if !first.IsBearish() {
		return false
	}

	// Second candle should be a small body (doji-like) with a gap down
	if second.GetBodyRatio() > 0.3 {
		return false
	}

	// Check for gap down between first and second candle
	firstLow := first.Low
	secondHigh := second.High
	if secondHigh >= firstLow {
		return false
	}

	// Third candle should be bullish
	if !third.IsBullish() {
		return false
	}

	// Third candle should close above the midpoint of the first candle's body
	firstMidpoint := (first.Open + first.Close) / 2
	return third.Close > firstMidpoint
}

// isEveningStar checks if the three candles form an evening star pattern
func (m *MorningEveningStarPattern) isEveningStar(first, second, third Candle) bool {
	// First candle should be bullish (uptrend)
	if !first.IsBullish() {
		return false
	}

	// Second candle should be a small body (doji-like) with a gap up
	if second.GetBodyRatio() > 0.3 {
		return false
	}

	// Check for gap up between first and second candle
	firstHigh := first.High
	secondLow := second.Low
	if secondLow <= firstHigh {
		return false
	}

	// Third candle should be bearish
	if !third.IsBearish() {
		return false
	}

	// Third candle should close below the midpoint of the first candle's body
	firstMidpoint := (first.Open + first.Close) / 2
	return third.Close < firstMidpoint
}

// calculateMorningStarStrength calculates the strength of a morning star pattern
func (m *MorningEveningStarPattern) calculateMorningStarStrength(first, second, third Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength if first candle is strongly bearish
	firstBodyRatio := first.GetBodyRatio()
	if firstBodyRatio > 0.7 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if second candle is a perfect doji
	if second.IsDoji() {
		strength = min(strength*1.3, 1.0)
	}

	// Boost strength if gap is significant
	firstLow := first.Low
	secondHigh := second.High
	gapSize := firstLow - secondHigh
	avgPrice := (first.High + first.Low) / 2
	gapRatio := gapSize / avgPrice
	if gapRatio > 0.02 { // 2% gap
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if third candle is strongly bullish
	thirdBodyRatio := third.GetBodyRatio()
	if thirdBodyRatio > 0.7 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if third candle closes well above first candle's midpoint
	firstMidpoint := (first.Open + first.Close) / 2
	closeAboveMidpoint := third.Close - firstMidpoint
	firstRange := first.High - first.Low
	if firstRange > 0 {
		closeRatio := closeAboveMidpoint / firstRange
		if closeRatio > 0.5 {
			strength = min(strength*1.3, 1.0)
		}
	}

	// Boost strength if volume increases on third candle
	if third.Volume > second.Volume*1.5 {
		strength = min(strength*1.1, 1.0)
	}

	return strength
}

// calculateEveningStarStrength calculates the strength of an evening star pattern
func (m *MorningEveningStarPattern) calculateEveningStarStrength(first, second, third Candle) float64 {
	strength := float64(StrengthWeak)

	// Boost strength if first candle is strongly bullish
	firstBodyRatio := first.GetBodyRatio()
	if firstBodyRatio > 0.7 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if second candle is a perfect doji
	if second.IsDoji() {
		strength = min(strength*1.3, 1.0)
	}

	// Boost strength if gap is significant
	firstHigh := first.High
	secondLow := second.Low
	gapSize := secondLow - firstHigh
	avgPrice := (first.High + first.Low) / 2
	gapRatio := gapSize / avgPrice
	if gapRatio > 0.02 { // 2% gap
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if third candle is strongly bearish
	thirdBodyRatio := third.GetBodyRatio()
	if thirdBodyRatio > 0.7 {
		strength = min(strength*1.2, 1.0)
	}

	// Boost strength if third candle closes well below first candle's midpoint
	firstMidpoint := (first.Open + first.Close) / 2
	closeBelowMidpoint := firstMidpoint - third.Close
	firstRange := first.High - first.Low
	if firstRange > 0 {
		closeRatio := closeBelowMidpoint / firstRange
		if closeRatio > 0.5 {
			strength = min(strength*1.3, 1.0)
		}
	}

	// Boost strength if volume increases on third candle
	if third.Volume > second.Volume*1.5 {
		strength = min(strength*1.1, 1.0)
	}

	return strength
}
