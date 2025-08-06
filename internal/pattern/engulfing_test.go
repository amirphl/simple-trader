package pattern

import (
	"testing"
	"time"
)

func TestEngulfingPattern(t *testing.T) {
	pattern := NewEngulfingPattern()

	t.Run("Name and Description", func(t *testing.T) {
		if pattern.Name() != "Engulfing" {
			t.Errorf("Expected name 'Engulfing', got '%s'", pattern.Name())
		}
		if pattern.Description() == "" {
			t.Error("Expected non-empty description")
		}
	})

	t.Run("Detect with insufficient candles", func(t *testing.T) {
		candles := []Candle{
			{Timestamp: time.Now(), Open: 100, High: 110, Low: 90, Close: 105, Volume: 1000},
		}
		_, err := pattern.Detect(candles)
		if err == nil {
			t.Error("Expected error for insufficient candles")
		}
	})

	t.Run("Bullish Engulfing", func(t *testing.T) {
		// Previous bearish candle
		previous := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}

		// Current bullish candle that engulfs the previous
		current := Candle{
			Timestamp: time.Now(),
			Open:      101,
			High:      108,
			Low:       99,
			Close:     106,
			Volume:    1500,
		}

		candles := []Candle{previous, current}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Bullish Engulfing" {
			t.Errorf("Expected 'Bullish Engulfing', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeBullish) {
			t.Errorf("Expected bullish direction, got '%s'", match.Direction)
		}
		if match.Index != 1 {
			t.Errorf("Expected index 1, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("Bearish Engulfing", func(t *testing.T) {
		// Previous bullish candle
		previous := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      100,
			High:      107,
			Low:       99,
			Close:     105,
			Volume:    1000,
		}

		// Current bearish candle that engulfs the previous
		current := Candle{
			Timestamp: time.Now(),
			Open:      107,
			High:      108,
			Low:       98,
			Close:     99,
			Volume:    1500,
		}

		candles := []Candle{previous, current}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Bearish Engulfing" {
			t.Errorf("Expected 'Bearish Engulfing', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeBearish) {
			t.Errorf("Expected bearish direction, got '%s'", match.Direction)
		}
		if match.Index != 1 {
			t.Errorf("Expected index 1, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("No Engulfing Pattern", func(t *testing.T) {
		// Two bullish candles (no reversal)
		previous := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      100,
			High:      105,
			Low:       99,
			Close:     104,
			Volume:    1000,
		}

		current := Candle{
			Timestamp: time.Now(),
			Open:      104,
			High:      108,
			Low:       103,
			Close:     107,
			Volume:    1000,
		}

		candles := []Candle{previous, current}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches, got %d", len(matches))
		}
	})

	t.Run("Partial Engulfing (Not Valid)", func(t *testing.T) {
		// Previous bearish candle
		previous := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}

		// Current bullish candle that doesn't fully engulf
		current := Candle{
			Timestamp: time.Now(),
			Open:      101,
			High:      106, // Doesn't reach previous high
			Low:       99,
			Close:     103,
			Volume:    1000,
		}

		candles := []Candle{previous, current}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches for partial engulfing, got %d", len(matches))
		}
	})

	t.Run("Multiple Patterns in Sequence", func(t *testing.T) {
		// Bearish candle
		candle1 := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}

		// Bullish engulfing
		candle2 := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      101,
			High:      108,
			Low:       99,
			Close:     106,
			Volume:    1500,
		}

		// Bearish engulfing
		candle3 := Candle{
			Timestamp: time.Now(),
			Open:      107,
			High:      109,
			Low:       98,
			Close:     100,
			Volume:    2000,
		}

		candles := []Candle{candle1, candle2, candle3}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches, got %d", len(matches))
		}

		// Check first match (bullish engulfing)
		if matches[0].Pattern != "Bullish Engulfing" {
			t.Errorf("Expected first match to be 'Bullish Engulfing', got '%s'", matches[0].Pattern)
		}
		if matches[0].Index != 1 {
			t.Errorf("Expected first match at index 1, got %d", matches[0].Index)
		}

		// Check second match (bearish engulfing)
		if matches[1].Pattern != "Bearish Engulfing" {
			t.Errorf("Expected second match to be 'Bearish Engulfing', got '%s'", matches[1].Pattern)
		}
		if matches[1].Index != 2 {
			t.Errorf("Expected second match at index 2, got %d", matches[1].Index)
		}
	})

	t.Run("Strength Calculation", func(t *testing.T) {
		// Previous small bearish candle
		previous := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      105,
			High:      106,
			Low:       104,
			Close:     104.5,
			Volume:    1000,
		}

		// Current large bullish candle that strongly engulfs
		current := Candle{
			Timestamp: time.Now(),
			Open:      104,
			High:      110,
			Low:       103,
			Close:     109,
			Volume:    2000, // Higher volume
		}

		candles := []Candle{previous, current}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		// Strong engulfing with high volume should have high strength
		if matches[0].Strength < 0.5 {
			t.Errorf("Expected high strength for strong engulfing, got %f", matches[0].Strength)
		}
	})
}

func TestEngulfingPatternInternalMethods(t *testing.T) {
	pattern := NewEngulfingPattern()

	t.Run("isBullishEngulfing", func(t *testing.T) {
		// Valid bullish engulfing
		previous := Candle{Open: 105, High: 107, Low: 100, Close: 102}
		current := Candle{Open: 101, High: 108, Low: 99, Close: 106}

		if !pattern.isBullishEngulfing(current, previous) {
			t.Error("Expected valid bullish engulfing")
		}

		// Invalid: current not bullish
		current2 := Candle{Open: 101, High: 108, Low: 99, Close: 100}
		if pattern.isBullishEngulfing(current2, previous) {
			t.Error("Expected invalid bullish engulfing (current not bullish)")
		}

		// Invalid: previous not bearish
		previous2 := Candle{Open: 100, High: 107, Low: 99, Close: 105}
		if pattern.isBullishEngulfing(current, previous2) {
			t.Error("Expected invalid bullish engulfing (previous not bearish)")
		}
	})

	t.Run("isBearishEngulfing", func(t *testing.T) {
		// Valid bearish engulfing
		previous := Candle{Open: 100, High: 107, Low: 99, Close: 105}
		current := Candle{Open: 106, High: 108, Low: 98, Close: 101}

		if !pattern.isBearishEngulfing(current, previous) {
			t.Error("Expected valid bearish engulfing")
		}

		// Invalid: current not bearish
		current2 := Candle{Open: 106, High: 108, Low: 98, Close: 107}
		if pattern.isBearishEngulfing(current2, previous) {
			t.Error("Expected invalid bearish engulfing (current not bearish)")
		}

		// Invalid: previous not bullish
		previous2 := Candle{Open: 105, High: 107, Low: 100, Close: 102}
		if pattern.isBearishEngulfing(current, previous2) {
			t.Error("Expected invalid bearish engulfing (previous not bullish)")
		}
	})
}
