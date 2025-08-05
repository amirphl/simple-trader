package pattern

import (
	"testing"
	"time"
)

func TestValidateCandle(t *testing.T) {
	tests := []struct {
		name    string
		candle  Candle
		wantErr bool
	}{
		{
			name: "valid candle",
			candle: Candle{
				Timestamp: time.Now(),
				Open:      100,
				High:      110,
				Low:       90,
				Close:     105,
				Volume:    1000,
			},
			wantErr: false,
		},
		{
			name: "high less than low",
			candle: Candle{
				Timestamp: time.Now(),
				Open:      100,
				High:      90,
				Low:       110,
				Close:     105,
				Volume:    1000,
			},
			wantErr: true,
		},
		{
			name: "open above high",
			candle: Candle{
				Timestamp: time.Now(),
				Open:      120,
				High:      110,
				Low:       90,
				Close:     105,
				Volume:    1000,
			},
			wantErr: true,
		},
		{
			name: "open below low",
			candle: Candle{
				Timestamp: time.Now(),
				Open:      80,
				High:      110,
				Low:       90,
				Close:     105,
				Volume:    1000,
			},
			wantErr: true,
		},
		{
			name: "close above high",
			candle: Candle{
				Timestamp: time.Now(),
				Open:      100,
				High:      110,
				Low:       90,
				Close:     120,
				Volume:    1000,
			},
			wantErr: true,
		},
		{
			name: "close below low",
			candle: Candle{
				Timestamp: time.Now(),
				Open:      100,
				High:      110,
				Low:       90,
				Close:     80,
				Volume:    1000,
			},
			wantErr: true,
		},
		{
			name: "negative volume",
			candle: Candle{
				Timestamp: time.Now(),
				Open:      100,
				High:      110,
				Low:       90,
				Close:     105,
				Volume:    -1000,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCandle(tt.candle)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateCandle() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCandleMethods(t *testing.T) {
	// Bullish candle
	bullish := Candle{
		Timestamp: time.Now(),
		Open:      100,
		High:      110,
		Low:       90,
		Close:     105,
		Volume:    1000,
	}

	// Bearish candle
	bearish := Candle{
		Timestamp: time.Now(),
		Open:      105,
		High:      110,
		Low:       90,
		Close:     100,
		Volume:    1000,
	}

	// Doji candle
	doji := Candle{
		Timestamp: time.Now(),
		Open:      100,
		High:      105,
		Low:       95,
		Close:     100.1,
		Volume:    1000,
	}

	t.Run("GetBodySize", func(t *testing.T) {
		if bullish.GetBodySize() != 5 {
			t.Errorf("Expected body size 5, got %f", bullish.GetBodySize())
		}
		if bearish.GetBodySize() != 5 {
			t.Errorf("Expected body size 5, got %f", bearish.GetBodySize())
		}
	})

	t.Run("GetUpperShadow", func(t *testing.T) {
		if bullish.GetUpperShadow() != 5 {
			t.Errorf("Expected upper shadow 5, got %f", bullish.GetUpperShadow())
		}
		if bearish.GetUpperShadow() != 5 {
			t.Errorf("Expected upper shadow 5, got %f", bearish.GetUpperShadow())
		}
	})

	t.Run("GetLowerShadow", func(t *testing.T) {
		if bullish.GetLowerShadow() != 10 {
			t.Errorf("Expected lower shadow 10, got %f", bullish.GetLowerShadow())
		}
		if bearish.GetLowerShadow() != 10 {
			t.Errorf("Expected lower shadow 10, got %f", bearish.GetLowerShadow())
		}
	})

	t.Run("GetTotalRange", func(t *testing.T) {
		if bullish.GetTotalRange() != 20 {
			t.Errorf("Expected total range 20, got %f", bullish.GetTotalRange())
		}
	})

	t.Run("IsBullish", func(t *testing.T) {
		if !bullish.IsBullish() {
			t.Error("Expected bullish candle to be bullish")
		}
		if bearish.IsBullish() {
			t.Error("Expected bearish candle to not be bullish")
		}
	})

	t.Run("IsBearish", func(t *testing.T) {
		if !bearish.IsBearish() {
			t.Error("Expected bearish candle to be bearish")
		}
		if bullish.IsBearish() {
			t.Error("Expected bullish candle to not be bearish")
		}
	})

	t.Run("IsDoji", func(t *testing.T) {
		if !doji.IsDoji() {
			t.Error("Expected doji candle to be doji")
		}
		if bullish.IsDoji() {
			t.Error("Expected bullish candle to not be doji")
		}
	})

	t.Run("GetBodyRatio", func(t *testing.T) {
		expected := 5.0 / 20.0 // body size / total range
		if bullish.GetBodyRatio() != expected {
			t.Errorf("Expected body ratio %f, got %f", expected, bullish.GetBodyRatio())
		}
	})

	t.Run("GetUpperShadowRatio", func(t *testing.T) {
		expected := 5.0 / 20.0 // upper shadow / total range
		if bullish.GetUpperShadowRatio() != expected {
			t.Errorf("Expected upper shadow ratio %f, got %f", expected, bullish.GetUpperShadowRatio())
		}
	})

	t.Run("GetLowerShadowRatio", func(t *testing.T) {
		expected := 10.0 / 20.0 // lower shadow / total range
		if bullish.GetLowerShadowRatio() != expected {
			t.Errorf("Expected lower shadow ratio %f, got %f", expected, bullish.GetLowerShadowRatio())
		}
	})
}

func TestUtilityFunctions(t *testing.T) {
	t.Run("abs", func(t *testing.T) {
		if abs(-5) != 5 {
			t.Errorf("Expected abs(-5) = 5, got %f", abs(-5))
		}
		if abs(5) != 5 {
			t.Errorf("Expected abs(5) = 5, got %f", abs(5))
		}
		if abs(0) != 0 {
			t.Errorf("Expected abs(0) = 0, got %f", abs(0))
		}
	})

	t.Run("max", func(t *testing.T) {
		if max(5, 3) != 5 {
			t.Errorf("Expected max(5, 3) = 5, got %f", max(5, 3))
		}
		if max(3, 5) != 5 {
			t.Errorf("Expected max(3, 5) = 5, got %f", max(3, 5))
		}
		if max(5, 5) != 5 {
			t.Errorf("Expected max(5, 5) = 5, got %f", max(5, 5))
		}
	})

	t.Run("min", func(t *testing.T) {
		if min(5, 3) != 3 {
			t.Errorf("Expected min(5, 3) = 3, got %f", min(5, 3))
		}
		if min(3, 5) != 3 {
			t.Errorf("Expected min(3, 5) = 3, got %f", min(3, 5))
		}
		if min(5, 5) != 5 {
			t.Errorf("Expected min(5, 5) = 5, got %f", min(5, 5))
		}
	})
}
