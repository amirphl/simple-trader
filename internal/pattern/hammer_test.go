package pattern

import (
	"testing"
	"time"
)

func TestHammerPattern(t *testing.T) {
	pattern := NewHammerPattern()

	t.Run("Name and Description", func(t *testing.T) {
		if pattern.Name() != "Hammer" {
			t.Errorf("Expected name 'Hammer', got '%s'", pattern.Name())
		}
		if pattern.Description() == "" {
			t.Error("Expected non-empty description")
		}
	})

	t.Run("Detect with insufficient candles", func(t *testing.T) {
		candles := []Candle{}
		_, err := pattern.Detect(candles)
		if err == nil {
			t.Error("Expected error for insufficient candles")
		}
	})

	t.Run("Hammer Pattern (Bullish)", func(t *testing.T) {
		// Valid hammer: small body, long lower shadow, small upper shadow, close near high
		hammer := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      105,
			Low:       90,  // Long lower shadow
			Close:     104, // Close near high
			Volume:    1000,
		}

		candles := []Candle{hammer}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Hammer" {
			t.Errorf("Expected 'Hammer', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeBullish) {
			t.Errorf("Expected bullish direction, got '%s'", match.Direction)
		}
		if match.Index != 0 {
			t.Errorf("Expected index 0, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("Hanging Man Pattern (Bearish)", func(t *testing.T) {
		// Valid hanging man: small body, long lower shadow, small upper shadow, close near low
		hangingMan := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      105,
			Low:       90,   // Long lower shadow
			Close:     90.5, // Close very near low
			Volume:    1000,
		}

		candles := []Candle{hangingMan}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Hanging Man" {
			t.Errorf("Expected 'Hanging Man', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeBearish) {
			t.Errorf("Expected bearish direction, got '%s'", match.Direction)
		}
		if match.Index != 0 {
			t.Errorf("Expected index 0, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("Not a Hammer - Large Body", func(t *testing.T) {
		// Large body, not a hammer
		candle := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      110,
			Low:       90,
			Close:     108, // Large body
			Volume:    1000,
		}

		candles := []Candle{candle}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches for large body candle, got %d", len(matches))
		}
	})

	t.Run("Not a Hammer - Short Lower Shadow", func(t *testing.T) {
		// Small body but short lower shadow
		candle := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      105,
			Low:       98,  // Short lower shadow
			Close:     101, // Small body
			Volume:    1000,
		}

		candles := []Candle{candle}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches for short lower shadow, got %d", len(matches))
		}
	})

	t.Run("Not a Hammer - Large Upper Shadow", func(t *testing.T) {
		// Small body, long lower shadow, but large upper shadow
		candle := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      115, // Large upper shadow
			Low:       90,  // Long lower shadow
			Close:     101, // Small body
			Volume:    1000,
		}

		candles := []Candle{candle}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches for large upper shadow, got %d", len(matches))
		}
	})

	t.Run("Multiple Patterns in Sequence", func(t *testing.T) {
		// Hammer
		hammer := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      100,
			High:      105,
			Low:       90,
			Close:     104,
			Volume:    1000,
		}

		// Regular candle
		regular := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      104,
			High:      108,
			Low:       103,
			Close:     107,
			Volume:    1000,
		}

		// Hanging man
		hangingMan := Candle{
			Timestamp: time.Now(),
			Open:      107,
			High:      112,
			Low:       92,
			Close:     92.5, // Close near low
			Volume:    1000,
		}

		candles := []Candle{hammer, regular, hangingMan}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches, got %d", len(matches))
		}

		// Check first match (hammer)
		if matches[0].Pattern != "Hammer" {
			t.Errorf("Expected first match to be 'Hammer', got '%s'", matches[0].Pattern)
		}
		if matches[0].Index != 0 {
			t.Errorf("Expected first match at index 0, got %d", matches[0].Index)
		}

		// Check second match (hanging man)
		if matches[1].Pattern != "Hanging Man" {
			t.Errorf("Expected second match to be 'Hanging Man', got '%s'", matches[1].Pattern)
		}
		if matches[1].Index != 2 {
			t.Errorf("Expected second match at index 2, got %d", matches[1].Index)
		}
	})

	t.Run("Perfect Hammer - Maximum Strength", func(t *testing.T) {
		// Perfect hammer: very small body, very long lower shadow, no upper shadow
		perfectHammer := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      100.1,  // Almost no upper shadow
			Low:       80,     // Very long lower shadow
			Close:     100.05, // Very small body, close to high
			Volume:    1000,
		}

		candles := []Candle{perfectHammer}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		// Perfect hammer should have high strength
		if matches[0].Strength < 0.7 {
			t.Errorf("Expected high strength for perfect hammer, got %f", matches[0].Strength)
		}
	})

	t.Run("Perfect Hanging Man - Maximum Strength", func(t *testing.T) {
		// Perfect hanging man: very small body, very long lower shadow, no upper shadow
		perfectHangingMan := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      100.1, // Almost no upper shadow
			Low:       80,    // Very long lower shadow
			Close:     80.1,  // Very small body, close to low
			Volume:    1000,
		}

		candles := []Candle{perfectHangingMan}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		// Perfect hanging man should have high strength
		if matches[0].Strength < 0.7 {
			t.Errorf("Expected high strength for perfect hanging man, got %f", matches[0].Strength)
		}
	})
}

func TestHammerPatternInternalMethods(t *testing.T) {
	pattern := NewHammerPattern()

	t.Run("isHammer", func(t *testing.T) {
		// Valid hammer
		hammer := Candle{Open: 100, High: 105, Low: 90, Close: 104}
		if !pattern.isHammer(hammer) {
			t.Error("Expected valid hammer")
		}

		// Invalid: large body
		largeBody := Candle{Open: 100, High: 110, Low: 90, Close: 108}
		if pattern.isHammer(largeBody) {
			t.Error("Expected invalid hammer (large body)")
		}

		// Invalid: short lower shadow
		shortShadow := Candle{Open: 100, High: 105, Low: 98, Close: 101}
		if pattern.isHammer(shortShadow) {
			t.Error("Expected invalid hammer (short lower shadow)")
		}

		// Invalid: large upper shadow
		largeUpperShadow := Candle{Open: 100, High: 115, Low: 90, Close: 101}
		if pattern.isHammer(largeUpperShadow) {
			t.Error("Expected invalid hammer (large upper shadow)")
		}

		// Invalid: close not near high
		closeNotNearHigh := Candle{Open: 100, High: 105, Low: 90, Close: 95}
		if pattern.isHammer(closeNotNearHigh) {
			t.Error("Expected invalid hammer (close not near high)")
		}
	})

	t.Run("isHangingMan", func(t *testing.T) {
		// Valid hanging man
		hangingMan := Candle{Open: 100, High: 105, Low: 90, Close: 91}
		if !pattern.isHangingMan(hangingMan) {
			t.Error("Expected valid hanging man")
		}

		// Invalid: large body
		largeBody := Candle{Open: 100, High: 110, Low: 90, Close: 92}
		if pattern.isHangingMan(largeBody) {
			t.Error("Expected invalid hanging man (large body)")
		}

		// Invalid: short lower shadow
		shortShadow := Candle{Open: 100, High: 105, Low: 98, Close: 99}
		if pattern.isHangingMan(shortShadow) {
			t.Error("Expected invalid hanging man (short lower shadow)")
		}

		// Invalid: large upper shadow
		largeUpperShadow := Candle{Open: 100, High: 115, Low: 90, Close: 99}
		if pattern.isHangingMan(largeUpperShadow) {
			t.Error("Expected invalid hanging man (large upper shadow)")
		}

		// Invalid: close not near low
		closeNotNearLow := Candle{Open: 100, High: 105, Low: 90, Close: 95}
		if pattern.isHangingMan(closeNotNearLow) {
			t.Error("Expected invalid hanging man (close not near low)")
		}
	})
}
