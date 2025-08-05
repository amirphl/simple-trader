package pattern

import (
	"testing"
	"time"
)

func TestDojiPattern(t *testing.T) {
	pattern := NewDojiPattern()

	t.Run("Name and Description", func(t *testing.T) {
		if pattern.Name() != "Doji" {
			t.Errorf("Expected name 'Doji', got '%s'", pattern.Name())
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

	t.Run("Standard Doji", func(t *testing.T) {
		// Valid standard doji: small body, moderate shadows
		standardDoji := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      102,
			Low:       98,
			Close:     100.1, // Very small body
			Volume:    1000,
		}

		candles := []Candle{standardDoji}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Standard Doji" {
			t.Errorf("Expected 'Standard Doji', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeNeutral) {
			t.Errorf("Expected neutral direction, got '%s'", match.Direction)
		}
		if match.Index != 0 {
			t.Errorf("Expected index 0, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("Long-Legged Doji", func(t *testing.T) {
		// Valid long-legged doji: small body, very long shadows
		longLeggedDoji := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      120,   // Very long upper shadow
			Low:       80,    // Very long lower shadow
			Close:     100.1, // Very small body
			Volume:    1000,
		}

		candles := []Candle{longLeggedDoji}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Long-Legged Doji" {
			t.Errorf("Expected 'Long-Legged Doji', got '%s'", match.Pattern)
		}
		if match.Direction != string(PatternTypeNeutral) {
			t.Errorf("Expected neutral direction, got '%s'", match.Direction)
		}
		if match.Index != 0 {
			t.Errorf("Expected index 0, got %d", match.Index)
		}
		if match.Strength <= 0 {
			t.Error("Expected positive strength")
		}
	})

	t.Run("Dragonfly Doji", func(t *testing.T) {
		// Valid dragonfly doji: small body, no upper shadow, long lower shadow
		dragonflyDoji := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      100.1,  // Almost no upper shadow
			Low:       80,     // Long lower shadow
			Close:     100.05, // Very small body
			Volume:    1000,
		}

		candles := []Candle{dragonflyDoji}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Dragonfly Doji" {
			t.Errorf("Expected 'Dragonfly Doji', got '%s'", match.Pattern)
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

	t.Run("Gravestone Doji", func(t *testing.T) {
		// Valid gravestone doji: small body, long upper shadow, no lower shadow
		gravestoneDoji := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      120,    // Long upper shadow
			Low:       99.9,   // Almost no lower shadow
			Close:     100.05, // Very small body
			Volume:    1000,
		}

		candles := []Candle{gravestoneDoji}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		match := matches[0]
		if match.Pattern != "Gravestone Doji" {
			t.Errorf("Expected 'Gravestone Doji', got '%s'", match.Pattern)
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

	t.Run("Not a Doji - Large Body", func(t *testing.T) {
		// Large body, not a doji
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

	t.Run("Multiple Doji Types in Sequence", func(t *testing.T) {
		// Standard doji
		standardDoji := Candle{
			Timestamp: time.Now().Add(-3 * time.Minute),
			Open:      100,
			High:      102,
			Low:       98,
			Close:     100.1,
			Volume:    1000,
		}

		// Regular candle
		regular := Candle{
			Timestamp: time.Now().Add(-2 * time.Minute),
			Open:      100.1,
			High:      108,
			Low:       99,
			Close:     107,
			Volume:    1000,
		}

		// Dragonfly doji
		dragonflyDoji := Candle{
			Timestamp: time.Now().Add(-time.Minute),
			Open:      107,
			High:      107.1,
			Low:       87,
			Close:     107.05,
			Volume:    1000,
		}

		// Gravestone doji
		gravestoneDoji := Candle{
			Timestamp: time.Now(),
			Open:      107.05,
			High:      127.05,
			Low:       107,
			Close:     107.1,
			Volume:    1000,
		}

		candles := []Candle{standardDoji, regular, dragonflyDoji, gravestoneDoji}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 3 {
			t.Fatalf("Expected 3 matches, got %d", len(matches))
		}

		// Check first match (standard doji)
		if matches[0].Pattern != "Standard Doji" {
			t.Errorf("Expected first match to be 'Standard Doji', got '%s'", matches[0].Pattern)
		}
		if matches[0].Index != 0 {
			t.Errorf("Expected first match at index 0, got %d", matches[0].Index)
		}

		// Check second match (dragonfly doji)
		if matches[1].Pattern != "Dragonfly Doji" {
			t.Errorf("Expected second match to be 'Dragonfly Doji', got '%s'", matches[1].Pattern)
		}
		if matches[1].Index != 2 {
			t.Errorf("Expected second match at index 2, got %d", matches[1].Index)
		}

		// Check third match (gravestone doji)
		if matches[2].Pattern != "Gravestone Doji" {
			t.Errorf("Expected third match to be 'Gravestone Doji', got '%s'", matches[2].Pattern)
		}
		if matches[2].Index != 3 {
			t.Errorf("Expected third match at index 3, got %d", matches[2].Index)
		}
	})

	t.Run("Perfect Doji - Maximum Strength", func(t *testing.T) {
		// Perfect doji: almost no body, balanced shadows
		perfectDoji := Candle{
			Timestamp: time.Now(),
			Open:      100,
			High:      105,
			Low:       95,
			Close:     100.001, // Almost no body
			Volume:    1000,
		}

		candles := []Candle{perfectDoji}
		matches, err := pattern.Detect(candles)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(matches) != 1 {
			t.Fatalf("Expected 1 match, got %d", len(matches))
		}

		// Perfect doji should have high strength
		if matches[0].Strength < 0.7 {
			t.Errorf("Expected high strength for perfect doji, got %f", matches[0].Strength)
		}
	})
}

func TestDojiPatternInternalMethods(t *testing.T) {
	pattern := NewDojiPattern()

	t.Run("isStandardDoji", func(t *testing.T) {
		// Valid standard doji
		standardDoji := Candle{Open: 100, High: 105, Low: 95, Close: 100.1}
		if !pattern.isStandardDoji(standardDoji) {
			t.Error("Expected valid standard doji")
		}

		// Invalid: large body
		largeBody := Candle{Open: 100, High: 110, Low: 90, Close: 108}
		if pattern.isStandardDoji(largeBody) {
			t.Error("Expected invalid standard doji (large body)")
		}

		// Invalid: too short shadows
		shortShadows := Candle{Open: 100, High: 101, Low: 99, Close: 100.1}
		if pattern.isStandardDoji(shortShadows) {
			t.Error("Expected invalid standard doji (short shadows)")
		}

		// Invalid: too long shadows
		longShadows := Candle{Open: 100, High: 120, Low: 80, Close: 100.1}
		if pattern.isStandardDoji(longShadows) {
			t.Error("Expected invalid standard doji (long shadows)")
		}
	})

	t.Run("isLongLeggedDoji", func(t *testing.T) {
		// Valid long-legged doji
		longLeggedDoji := Candle{Open: 100, High: 120, Low: 80, Close: 100.1}
		if !pattern.isLongLeggedDoji(longLeggedDoji) {
			t.Error("Expected valid long-legged doji")
		}

		// Invalid: large body
		largeBody := Candle{Open: 100, High: 110, Low: 90, Close: 108}
		if pattern.isLongLeggedDoji(largeBody) {
			t.Error("Expected invalid long-legged doji (large body)")
		}

		// Invalid: short shadows
		shortShadows := Candle{Open: 100, High: 105, Low: 95, Close: 100.1}
		if pattern.isLongLeggedDoji(shortShadows) {
			t.Error("Expected invalid long-legged doji (short shadows)")
		}
	})

	t.Run("isDragonflyDoji", func(t *testing.T) {
		// Valid dragonfly doji
		dragonflyDoji := Candle{Open: 100, High: 100.1, Low: 80, Close: 100.05}
		if !pattern.isDragonflyDoji(dragonflyDoji) {
			t.Error("Expected valid dragonfly doji")
		}

		// Invalid: large body
		largeBody := Candle{Open: 100, High: 110, Low: 90, Close: 108}
		if pattern.isDragonflyDoji(largeBody) {
			t.Error("Expected invalid dragonfly doji (large body)")
		}

		// Invalid: large upper shadow
		largeUpperShadow := Candle{Open: 100, High: 110, Low: 80, Close: 100.05}
		if pattern.isDragonflyDoji(largeUpperShadow) {
			t.Error("Expected invalid dragonfly doji (large upper shadow)")
		}

		// Invalid: short lower shadow
		shortLowerShadow := Candle{Open: 100, High: 100.1, Low: 95, Close: 100.05}
		if pattern.isDragonflyDoji(shortLowerShadow) {
			t.Error("Expected invalid dragonfly doji (short lower shadow)")
		}
	})

	t.Run("isGravestoneDoji", func(t *testing.T) {
		// Valid gravestone doji
		gravestoneDoji := Candle{Open: 100, High: 120, Low: 99.9, Close: 100.05}
		if !pattern.isGravestoneDoji(gravestoneDoji) {
			t.Error("Expected valid gravestone doji")
		}

		// Invalid: large body
		largeBody := Candle{Open: 100, High: 110, Low: 90, Close: 108}
		if pattern.isGravestoneDoji(largeBody) {
			t.Error("Expected invalid gravestone doji (large body)")
		}

		// Invalid: short upper shadow
		shortUpperShadow := Candle{Open: 100, High: 105, Low: 99.9, Close: 100.05}
		if pattern.isGravestoneDoji(shortUpperShadow) {
			t.Error("Expected invalid gravestone doji (short upper shadow)")
		}

		// Invalid: large lower shadow
		largeLowerShadow := Candle{Open: 100, High: 120, Low: 80, Close: 100.05}
		if pattern.isGravestoneDoji(largeLowerShadow) {
			t.Error("Expected invalid gravestone doji (large lower shadow)")
		}
	})
}
