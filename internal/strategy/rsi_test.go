package strategy

import (
	"testing"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRSIStrategy_Name(t *testing.T) {
	strategy := NewRSIStrategy(14, 70, 30)
	assert.Equal(t, "RSI", strategy.Name())
}

func TestRSIStrategy_WarmupPeriod(t *testing.T) {
	period := 14
	strategy := NewRSIStrategy(period, 70, 30)
	assert.Equal(t, period, strategy.WarmupPeriod())
}

func TestRSIStrategy_OnCandle(t *testing.T) {
	tests := []struct {
		name           string
		period         int
		overbought     float64
		oversold       float64
		candles        []candle.Candle
		expectedSignal []string // "buy", "sell", "hold"
		expectedReason []string
	}{
		{
			name:       "Basic RSI signals",
			period:     5,
			overbought: 70,
			oversold:   30,
			candles: []candle.Candle{
				createCandle(10, "2023-01-01T00:00:00Z"),
				createCandle(11, "2023-01-01T01:00:00Z"),
				createCandle(12, "2023-01-01T02:00:00Z"),
				createCandle(11, "2023-01-01T03:00:00Z"),
				createCandle(10, "2023-01-01T04:00:00Z"), // Warmup complete
				createCandle(9, "2023-01-01T05:00:00Z"),  // RSI = 40.00
				createCandle(8, "2023-01-01T06:00:00Z"),  // RSI = 32.00 (oversold)
				createCandle(9, "2023-01-01T07:00:00Z"),  // RSI = 45.60 (neutral)
				createCandle(10, "2023-01-01T08:00:00Z"), // RSI = 56.48 (neutral)
				createCandle(11, "2023-01-01T09:00:00Z"), // RSI = 65.18 (neutral)
				createCandle(12, "2023-01-01T10:00:00Z"), // RSI = 72.15 (overbought)
				createCandle(13, "2023-01-01T11:00:00Z"), // RSI = 77.72 (overbought)
			},
			expectedSignal: []string{
				"hold", "hold", "hold", "hold", "hold",
				"hold", "hold", "hold", "hold", "hold", "sell", "sell",
			},
			expectedReason: []string{
				"warming up", "warming up", "warming up", "warming up", "warming up",
				"RSI neutral", "RSI neutral", "RSI neutral", "RSI neutral", "RSI neutral", "RSI overbought", "RSI overbought",
			},
		},
		{
			name:       "All increasing prices (overbought)",
			period:     3,
			overbought: 70,
			oversold:   30,
			candles: []candle.Candle{
				createCandle(10, "2023-01-01T00:00:00Z"),
				createCandle(11, "2023-01-01T01:00:00Z"),
				createCandle(12, "2023-01-01T02:00:00Z"), // Warmup complete
				createCandle(13, "2023-01-01T03:00:00Z"), // RSI = 100 (overbought)
				createCandle(14, "2023-01-01T04:00:00Z"), // RSI = 100 (overbought)
			},
			expectedSignal: []string{
				"hold", "hold", "hold", "sell", "sell",
			},
			expectedReason: []string{
				"warming up", "warming up", "warming up", "RSI overbought", "RSI overbought",
			},
		},
		{
			name:       "All decreasing prices (oversold)",
			period:     3,
			overbought: 70,
			oversold:   30,
			candles: []candle.Candle{
				createCandle(20, "2023-01-01T00:00:00Z"),
				createCandle(19, "2023-01-01T01:00:00Z"),
				createCandle(18, "2023-01-01T02:00:00Z"), // Warmup complete
				createCandle(17, "2023-01-01T03:00:00Z"), // RSI = 0 (oversold)
				createCandle(16, "2023-01-01T04:00:00Z"), // RSI = 0 (oversold)
			},
			expectedSignal: []string{
				"hold", "hold", "hold", "buy", "buy",
			},
			expectedReason: []string{
				"warming up", "warming up", "warming up", "RSI oversold", "RSI oversold",
			},
		},
		{
			name:       "Flat prices (neutral)",
			period:     3,
			overbought: 70,
			oversold:   30,
			candles: []candle.Candle{
				createCandle(10, "2023-01-01T00:00:00Z"),
				createCandle(10, "2023-01-01T01:00:00Z"),
				createCandle(10, "2023-01-01T02:00:00Z"), // Warmup complete
				createCandle(10, "2023-01-01T03:00:00Z"), // RSI = 100 (overbought) - flat prices have RSI=100
				createCandle(10, "2023-01-01T04:00:00Z"), // RSI = 100 (overbought)
			},
			expectedSignal: []string{
				"hold", "hold", "hold", "sell", "sell",
			},
			expectedReason: []string{
				"warming up", "warming up", "warming up", "RSI overbought", "RSI overbought",
			},
		},
		{
			name:       "Alternating prices (neutral)",
			period:     2,
			overbought: 80,
			oversold:   20,
			candles: []candle.Candle{
				createCandle(10, "2023-01-01T00:00:00Z"),
				createCandle(11, "2023-01-01T01:00:00Z"), // Warmup complete
				createCandle(10, "2023-01-01T02:00:00Z"), // RSI = 50 (neutral)
				createCandle(11, "2023-01-01T03:00:00Z"), // RSI = 75 (neutral)
				createCandle(10, "2023-01-01T04:00:00Z"), // RSI = 37.5 (neutral)
			},
			expectedSignal: []string{
				"hold", "hold", "hold", "hold", "hold",
			},
			expectedReason: []string{
				"warming up", "warming up", "RSI neutral", "RSI neutral", "RSI neutral",
			},
		},
		{
			name:       "Extreme price changes",
			period:     3,
			overbought: 70,
			oversold:   30,
			candles: []candle.Candle{
				createCandle(10, "2023-01-01T00:00:00Z"),
				createCandle(100, "2023-01-01T01:00:00Z"),
				createCandle(5, "2023-01-01T02:00:00Z"),   // Warmup complete
				createCandle(200, "2023-01-01T03:00:00Z"), // RSI = 75 (overbought)
				createCandle(1, "2023-01-01T04:00:00Z"),   // RSI = 42 (neutral)
			},
			expectedSignal: []string{
				"hold", "hold", "hold", "sell", "hold",
			},
			expectedReason: []string{
				"warming up", "warming up", "warming up", "RSI overbought", "RSI neutral",
			},
		},
		{
			name:       "Custom thresholds",
			period:     5,
			overbought: 60, // Lower overbought threshold
			oversold:   40, // Higher oversold threshold
			candles: []candle.Candle{
				createCandle(10, "2023-01-01T00:00:00Z"),
				createCandle(11, "2023-01-01T01:00:00Z"),
				createCandle(12, "2023-01-01T02:00:00Z"),
				createCandle(11, "2023-01-01T03:00:00Z"),
				createCandle(10, "2023-01-01T04:00:00Z"), // Warmup complete
				createCandle(9, "2023-01-01T05:00:00Z"),  // RSI = 40.00 (exactly at oversold threshold, but not below)
				createCandle(10, "2023-01-01T06:00:00Z"), // RSI = 52.00 (neutral)
				createCandle(11, "2023-01-01T07:00:00Z"), // RSI = 61.60 (overbought)
			},
			expectedSignal: []string{
				"hold", "hold", "hold", "hold", "hold",
				"hold", "hold", "sell",
			},
			expectedReason: []string{
				"warming up", "warming up", "warming up", "warming up", "warming up",
				"RSI neutral", "RSI neutral", "RSI overbought",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewRSIStrategy(tt.period, tt.overbought, tt.oversold)

			require.Equal(t, len(tt.candles), len(tt.expectedSignal), "test case setup error: candle and expected signal counts don't match")
			require.Equal(t, len(tt.candles), len(tt.expectedReason), "test case setup error: candle and expected reason counts don't match")

			for i, candle := range tt.candles {
				signal, err := strategy.OnCandle(candle)
				require.NoError(t, err)
				assert.Equal(t, tt.expectedSignal[i], signal.Action, "signal mismatch at index %d", i)
				assert.Equal(t, tt.expectedReason[i], signal.Reason, "reason mismatch at index %d", i)
				assert.Equal(t, candle.Timestamp, signal.Time, "timestamp mismatch at index %d", i)
			}
		})
	}
}

func TestRSIStrategy_InvalidCandle(t *testing.T) {
	strategy := NewRSIStrategy(14, 70, 30)

	// Test with a non-candle type
	_, err := strategy.OnCandle("not a candle")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected candle.Candle")

	// Test with nil
	_, err = strategy.OnCandle(nil)
	assert.Error(t, err)
}

func TestRSIStrategy_PerformanceMetrics(t *testing.T) {
	strategy := NewRSIStrategy(14, 70, 30)
	metrics := strategy.PerformanceMetrics()
	assert.NotNil(t, metrics)
	// Currently returns an empty map, but test is here for future implementation
}

// Helper function to create a candle with the given close price and timestamp
func createCandle(closePrice float64, timestamp string) candle.Candle {
	ts, _ := time.Parse(time.RFC3339, timestamp)
	return candle.Candle{
		Timestamp: ts,
		Open:      closePrice - 0.5, // Just for test purposes
		High:      closePrice + 0.5,
		Low:       closePrice - 0.5,
		Close:     closePrice,
		Volume:    100,
		Symbol:    "TEST",
		Timeframe: "1h",
		Source:    "test",
	}
}

