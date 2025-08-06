package pattern

import (
	"testing"
	"time"
)

func TestMorningEveningStarPattern(t *testing.T) {
	pattern := NewMorningEveningStarPattern()

	t.Run("Name and Description", func(t *testing.T) {
		if pattern.Name() != "Morning/Evening Star" {
			t.Errorf("Expected name 'Morning/Evening Star', got '%s'", pattern.Name())
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

	t.Run("Morning Star Pattern (Bullish)", func(t *testing.T) {
		// First candle: bearish (downtrend)
		first := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}

		// Second candle: small body with gap down
		second := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      99,
			High:      99.5,
			Low:       98.5,
			Close:     98.8, // Small body, gap down from first candle
			Volume:    500,
		}

		// Third candle: bullish
		third := Candle{
			Timestamp: time.Now(),
			Open:      98.8,
			High:      108,
			Low:       98.5,
			Close:     106, // Closes above first candle's midpoint
			Volume:    1500,
		}

		candles := []Candle{first, second, third}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Morning Star" {
			t.Errorf("Expected 'Morning Star', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeBullish) {
			t.Errorf("Expected bullish direction, got '%s'", match.Direction)
		}
		if match.Index != 2 {
			t.Errorf("Expected index 2, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("Evening Star Pattern (Bearish)", func(t *testing.T) {
		// First candle: bullish (uptrend)
		first := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      100,
			High:      107,
			Low:       99,
			Close:     105,
			Volume:    1000,
		}

		// Second candle: small body with gap up
		second := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      108,
			High:      108.5,
			Low:       107.5,
			Close:     107.8, // Small body, gap up from first candle
			Volume:    500,
		}

		// Third candle: bearish
		third := Candle{
			Timestamp: time.Now(),
			Open:      107.8,
			High:      108,
			Low:       98,
			Close:     100, // Closes below first candle's midpoint
			Volume:    1500,
		}

		candles := []Candle{first, second, third}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Evening Star" {
			t.Errorf("Expected 'Evening Star', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeBearish) {
			t.Errorf("Expected bearish direction, got '%s'", match.Direction)
		}
		if match.Index != 2 {
			t.Errorf("Expected index 2, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("No Pattern - No Gap", func(t *testing.T) {
		// First candle: bearish
		first := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}

		// Second candle: no gap
		second := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      102,
			High:      103,
			Low:       101,
			Close:     102.5,
			Volume:    500,
		}

		// Third candle: bullish
		third := Candle{
			Timestamp: time.Now(),
			Open:      102.5,
			High:      108,
			Low:       102,
			Close:     106,
			Volume:    1500,
		}

		candles := []Candle{first, second, third}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches for no gap, got %d", len(matches))
		}
	})

	t.Run("No Pattern - Large Second Candle", func(t *testing.T) {
		// First candle: bearish
		first := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}

		// Second candle: large body (not doji-like)
		second := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      101,
			High:      104,
			Low:       100,
			Close:     103, // Large body
			Volume:    500,
		}

		// Third candle: bullish
		third := Candle{
			Timestamp: time.Now(),
			Open:      103,
			High:      108,
			Low:       102,
			Close:     106,
			Volume:    1500,
		}

		candles := []Candle{first, second, third}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches for large second candle, got %d", len(matches))
		}
	})

	t.Run("No Pattern - Third Candle Not Reversing", func(t *testing.T) {
		// First candle: bearish
		first := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}

		// Second candle: small body with gap down
		second := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      101,
			High:      101.5,
			Low:       100.5,
			Close:     100.8,
			Volume:    500,
		}

		// Third candle: bearish (not reversing)
		third := Candle{
			Timestamp: time.Now(),
			Open:      100.8,
			High:      102,
			Low:       99,
			Close:     101, // Doesn't close above first candle's midpoint
			Volume:    1500,
		}

		candles := []Candle{first, second, third}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 0 {
			t.Fatalf("Expected 0 matches for non-reversing third candle, got %d", len(matches))
		}
	})

	t.Run("Multiple Patterns in Sequence", func(t *testing.T) {
		// Morning star pattern
		first1 := Candle{
			Timestamp: time.Now().Add(-6 * time.Minute),
			Open:      105,
			High:      107,
			Low:       100,
			Close:     102,
			Volume:    1000,
		}
		second1 := Candle{
			Timestamp: time.Now().Add(-5 * time.Minute),
			Open:      99,
			High:      99.5,
			Low:       98.5,
			Close:     98.8, // Gap down from first candle
			Volume:    500,
		}
		third1 := Candle{
			Timestamp: time.Now().Add(-4 * time.Minute),
			Open:      98.8,
			High:      108,
			Low:       98.5,
			Close:     106,
			Volume:    1500,
		}

		// Evening star pattern
		first2 := Candle{
			Timestamp: time.Now().Add(-3 * time.Minute),
			Open:      106,
			High:      112,
			Low:       105,
			Close:     110,
			Volume:    1000,
		}
		second2 := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      113,
			High:      113.5,
			Low:       112.5,
			Close:     112.8, // Gap up from first candle
			Volume:    500,
		}
		third2 := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      112.8,
			High:      113,
			Low:       103,
			Close:     105,
			Volume:    1500,
		}

		// Regular candle
		regular := Candle{
			Timestamp: time.Now(),
			Open:      105,
			High:      107,
			Low:       104,
			Close:     106,
			Volume:    1000,
		}

		candles := []Candle{first1, second1, third1, first2, second2, third2, regular}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 2 {
			t.Fatalf("Expected 2 matches, got %d", len(matches))
		}

		// Check first match (morning star)
		if matches[0].Pattern != "Morning Star" {
			t.Errorf("Expected first match to be 'Morning Star', got '%s'", matches[0].Pattern)
		}
		if matches[0].Index != 2 {
			t.Errorf("Expected first match at index 2, got %d", matches[0].Index)
		}

		// Check second match (evening star)
		if matches[1].Pattern != "Evening Star" {
			t.Errorf("Expected second match to be 'Evening Star', got '%s'", matches[1].Pattern)
		}
		if matches[1].Index != 5 {
			t.Errorf("Expected second match at index 5, got %d", matches[1].Index)
		}
	})

	t.Run("Perfect Morning Star - Maximum Strength", func(t *testing.T) {
		// Perfect morning star: strong bearish first, perfect doji second, strong bullish third
		first := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      105,
			High:      106,
			Low:       100,
			Close:     101, // Strong bearish
			Volume:    1000,
		}

		second := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      98,
			High:      98.1,
			Low:       97.9,
			Close:     98.001, // Perfect doji, gap down
			Volume:    500,
		}

		third := Candle{
			Timestamp: time.Now(),
			Open:      100.001,
			High:      110,
			Low:       100,
			Close:     109,  // Strong bullish, closes well above midpoint
			Volume:    2000, // High volume
		}

		candles := []Candle{first, second, third}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		// Perfect morning star should have high strength
		if matches[0].Strength < 0.7 {
			t.Errorf("Expected high strength for perfect morning star, got %f", matches[0].Strength)
		}
	})

	t.Run("Perfect Evening Star - Maximum Strength", func(t *testing.T) {
		// Perfect evening star: strong bullish first, perfect doji second, strong bearish third
		first := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      100,
			High:      106,
			Low:       99,
			Close:     105, // Strong bullish
			Volume:    1000,
		}

		second := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      106,
			High:      106.1,
			Low:       105.9,
			Close:     106.001, // Perfect doji
			Volume:    500,
		}

		third := Candle{
			Timestamp: time.Now(),
			Open:      106.001,
			High:      106,
			Low:       96,
			Close:     97,   // Strong bearish, closes well below midpoint
			Volume:    2000, // High volume
		}

		candles := []Candle{first, second, third}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		// Perfect evening star should have high strength
		if matches[0].Strength < 0.7 {
			t.Errorf("Expected high strength for perfect evening star, got %f", matches[0].Strength)
		}
	})
}

func TestMorningEveningStarPatternInternalMethods(t *testing.T) {
	pattern := NewMorningEveningStarPattern()

	t.Run("isMorningStar", func(t *testing.T) {
		// Valid morning star
		first := Candle{Open: 105, High: 107, Low: 100, Close: 102}
		second := Candle{Open: 101, High: 101.5, Low: 100.5, Close: 100.8}
		third := Candle{Open: 100.8, High: 108, Low: 100.5, Close: 106}

		if !pattern.isMorningStar(first, second, third) {
			t.Error("Expected valid morning star")
		}

		// Invalid: first not bearish
		first2 := Candle{Open: 100, High: 107, Low: 99, Close: 105}
		if pattern.isMorningStar(first2, second, third) {
			t.Error("Expected invalid morning star (first not bearish)")
		}

		// Invalid: second large body
		second2 := Candle{Open: 101, High: 104, Low: 100, Close: 103}
		if pattern.isMorningStar(first, second2, third) {
			t.Error("Expected invalid morning star (second large body)")
		}

		// Invalid: no gap
		second3 := Candle{Open: 102, High: 103, Low: 101, Close: 102.5}
		if pattern.isMorningStar(first, second3, third) {
			t.Error("Expected invalid morning star (no gap)")
		}

		// Invalid: third not bullish
		third2 := Candle{Open: 100.8, High: 102, Low: 99, Close: 101}
		if pattern.isMorningStar(first, second, third2) {
			t.Error("Expected invalid morning star (third not bullish)")
		}

		// Invalid: third doesn't close above midpoint
		third3 := Candle{Open: 100.8, High: 103, Low: 100.5, Close: 101.5}
		if pattern.isMorningStar(first, second, third3) {
			t.Error("Expected invalid morning star (third doesn't close above midpoint)")
		}
	})

	t.Run("isEveningStar", func(t *testing.T) {
		// Valid evening star
		first := Candle{Open: 100, High: 107, Low: 99, Close: 105}
		second := Candle{Open: 106, High: 106.5, Low: 105.5, Close: 105.8}
		third := Candle{Open: 105.8, High: 106, Low: 98, Close: 100}

		if !pattern.isEveningStar(first, second, third) {
			t.Error("Expected valid evening star")
		}

		// Invalid: first not bullish
		first2 := Candle{Open: 105, High: 107, Low: 100, Close: 102}
		if pattern.isEveningStar(first2, second, third) {
			t.Error("Expected invalid evening star (first not bullish)")
		}

		// Invalid: second large body
		second2 := Candle{Open: 106, High: 109, Low: 105, Close: 108}
		if pattern.isEveningStar(first, second2, third) {
			t.Error("Expected invalid evening star (second large body)")
		}

		// Invalid: no gap
		second3 := Candle{Open: 105, High: 106, Low: 104, Close: 105.5}
		if pattern.isEveningStar(first, second3, third) {
			t.Error("Expected invalid evening star (no gap)")
		}

		// Invalid: third not bearish
		third2 := Candle{Open: 105.8, High: 108, Low: 105.5, Close: 107}
		if pattern.isEveningStar(first, second, third2) {
			t.Error("Expected invalid evening star (third not bearish)")
		}

		// Invalid: third doesn't close below midpoint
		third3 := Candle{Open: 105.8, High: 106, Low: 103, Close: 104.5}
		if pattern.isEveningStar(first, second, third3) {
			t.Error("Expected invalid evening star (third doesn't close below midpoint)")
		}
	})
}
