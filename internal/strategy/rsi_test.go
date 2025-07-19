// Package strategy
package strategy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Mock storage for testing
type mockStorage struct {
	mock.Mock
}

func (m *mockStorage) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

// ErrorStorage always returns an error
type errorStorage struct{}

func (_ errorStorage) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]candle.Candle, error) {
	return nil, errors.New("storage error")
}

// EmptyStorage always returns empty candles
type emptyStorage struct{}

func (_ emptyStorage) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]candle.Candle, error) {
	return []candle.Candle{}, nil
}

// NoInitStorage is a storage implementation that doesn't require initialization
// This avoids the need to mock the GetCandles call in regular tests
type noInitStorage struct{}

func (_ noInitStorage) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]candle.Candle, error) {
	return []candle.Candle{}, nil
}

func TestRSIStrategy_Name(t *testing.T) {
	storage := &mockStorage{}
	strategy := NewRSIStrategy("TEST", 14, 70, 30, storage)
	assert.Equal(t, "RSI", strategy.Name())
}

func TestRSIStrategy_Symbol(t *testing.T) {
	storage := &mockStorage{}
	strategy := NewRSIStrategy("TEST", 14, 70, 30, storage)
	assert.Equal(t, "TEST", strategy.Symbol())
}

func TestRSIStrategy_WarmupPeriod(t *testing.T) {
	storage := &mockStorage{}
	period := 14
	strategy := NewRSIStrategy("TEST", period, 70, 30, storage)
	assert.Equal(t, period, strategy.WarmupPeriod())
}

func TestRSIStrategy_OnCandles(t *testing.T) {
	symbol := "TEST"

	tests := []struct {
		name           string
		period         int
		overbought     float64
		oversold       float64
		candles        []candle.Candle
		expectedSignal []Position // "long", "short", "hold"
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
			expectedSignal: []Position{
				Hold, Hold, Hold, Hold, Hold,
				Hold, Hold, Hold, Hold, Hold, ShortBearish, ShortBearish,
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
			expectedSignal: []Position{
				Hold, Hold, Hold, ShortBearish, ShortBearish,
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
			expectedSignal: []Position{
				Hold, Hold, Hold, LongBullish, LongBullish,
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
			expectedSignal: []Position{
				Hold, Hold, Hold, ShortBearish, ShortBearish,
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
			expectedSignal: []Position{
				Hold, Hold, Hold, Hold, Hold,
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
			expectedSignal: []Position{
				Hold, Hold, Hold, ShortBearish, Hold,
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
			expectedSignal: []Position{
				Hold, Hold, Hold, Hold, Hold,
				Hold, Hold, ShortBearish,
			},
			expectedReason: []string{
				"warming up", "warming up", "warming up", "warming up", "warming up",
				"RSI neutral", "RSI neutral", "RSI overbought",
			},
		},
		{
			name:       "No candles",
			period:     5,
			overbought: 70,
			oversold:   30,
			candles:    []candle.Candle{},
			expectedSignal: []Position{
				Hold,
			},
			expectedReason: []string{
				"no candles",
			},
		},
		{
			name:       "No matching candles",
			period:     5,
			overbought: 70,
			oversold:   30,
			candles: []candle.Candle{
				{Symbol: "OTHER", Close: 10},
			},
			expectedSignal: []Position{
				Hold,
			},
			expectedReason: []string{
				"no matching candles",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use noInitStorage to avoid the need for mocking GetCandles
			storage := noInitStorage{}
			strategy := NewRSIStrategy(symbol, tt.period, tt.overbought, tt.oversold, storage)

			// Force initialized to true to skip historical data fetching
			strategy.initialized = true

			if tt.name != "No candles" {
				require.Equal(t, len(tt.candles), len(tt.expectedSignal), "test case setup error: candle and expected signal counts don't match")
				require.Equal(t, len(tt.candles), len(tt.expectedReason), "test case setup error: candle and expected reason counts don't match")
			}

			for i, c := range tt.candles {
				// Set the correct symbol for the test
				if c.Symbol == "" {
					c.Symbol = symbol
				}

				signal, err := strategy.OnCandles(context.Background(), []candle.Candle{c})
				require.NoError(t, err)
				assert.Equal(t, tt.expectedSignal[i], signal.Position, "signal mismatch at index %d", i)
				assert.Equal(t, tt.expectedReason[i], signal.Reason, "reason mismatch at index %d", i)
				if c.Symbol == symbol {
					assert.Equal(t, c.Timestamp, signal.Time, "timestamp mismatch at index %d", i)
				}
			}
		})
	}
}

func TestRSIStrategy_HistoricalDataHandling(t *testing.T) {
	symbol := "TEST"
	period := 5

	// Create historical candles
	historicalCandles := []candle.Candle{
		createCandle(10, "2022-01-01T00:00:00Z"),
		createCandle(11, "2022-01-01T01:00:00Z"),
		createCandle(12, "2022-01-01T02:00:00Z"),
		createCandle(13, "2022-01-01T03:00:00Z"),
		createCandle(14, "2022-01-01T04:00:00Z"),
		createCandle(15, "2022-01-01T05:00:00Z"),
	}

	// Create mock storage that returns historical candles
	storage := &mockStorage{}
	storage.On("GetCandles", mock.Anything, symbol, "1m", mock.Anything, mock.Anything).Return(historicalCandles, nil)

	// Create strategy
	strategy := NewRSIStrategy(symbol, period, 70, 30, storage)

	// First candle should trigger historical data fetch
	firstCandle := createCandle(16, "2023-01-01T00:00:00Z")
	firstCandle.Symbol = symbol

	signal, err := strategy.OnCandles(context.Background(), []candle.Candle{firstCandle})
	require.NoError(t, err)

	// Should immediately have a valid signal because of historical data
	assert.Equal(t, ShortBearish, signal.Position)
	assert.Equal(t, "RSI overbought", signal.Reason)

	// Verify the mock was called
	storage.AssertExpectations(t)
}

// func TestRSIStrategy_ErrorHandlingInHistoricalData(t *testing.T) {
// 	symbol := "TEST"
// 	period := 5

// 	// Create strategy with error storage
// 	strategy := NewRSIStrategy(symbol, period, 70, 30, errorStorage{})

// 	// First candle should trigger historical data fetch (which will fail)
// 	firstCandle := createCandle(16, "2023-01-01T00:00:00Z")
// 	firstCandle.Symbol = symbol

// 	// Should still work, just with no historical data
// 	signal, err := strategy.OnCandles([]candle.Candle{firstCandle})
// 	require.NoError(t, err)

// 	// Should be warming up since we only have one candle
// 	assert.Equal(t, "hold", signal.Action)
// 	assert.Equal(t, "warming up", signal.Reason)

// 	// Add enough candles to complete warmup
// 	var candles []candle.Candle
// 	for i := 0; i < period; i++ {
// 		c := createCandle(float64(10+i), time.Now().Add(time.Duration(i)*time.Hour).Format(time.RFC3339))
// 		c.Symbol = symbol
// 		candles = append(candles, c)
// 	}

// 	// Process all candles
// 	for _, c := range candles {
// 		strategy.OnCandles([]candle.Candle{c})
// 	}

// 	// Now we should have a valid signal
// 	lastCandle := createCandle(20, time.Now().Add(time.Duration(period+1)*time.Hour).Format(time.RFC3339))
// 	lastCandle.Symbol = symbol

// 	signal, err = strategy.OnCandles([]candle.Candle{lastCandle})
// 	require.NoError(t, err)
// 	assert.Equal(t, "sell", signal.Action)
// 	assert.Equal(t, "RSI overbought", signal.Reason)
// }

func TestRSIStrategy_MemoryManagement(t *testing.T) {
	symbol := "TEST"
	period := 5

	// Create strategy with a small maxHistorySize for testing
	strategy := NewRSIStrategy(symbol, period, 70, 30, emptyStorage{})
	strategy.maxHistorySize = 10 // Small size for testing

	// Add more candles than maxHistorySize
	for i := 0; i < 20; i++ {
		c := createCandle(float64(10+i), time.Now().Add(time.Duration(i)*time.Hour).Format(time.RFC3339))
		c.Symbol = symbol
		strategy.OnCandles(context.Background(), []candle.Candle{c})
	}

	// Verify that prices array is trimmed
	assert.LessOrEqual(t, len(strategy.prices), strategy.maxHistorySize)

	// Verify that the most recent prices are kept
	assert.Equal(t, 29.0, strategy.prices[len(strategy.prices)-1]) // 10 + 19 = 29
}

func TestRSIStrategy_SignalFlipping(t *testing.T) {
	symbol := "TEST"
	period := 3

	// Create strategy
	strategy := NewRSIStrategy(symbol, period, 70, 30, emptyStorage{})

	// Add initial candles to complete warmup
	for i := 0; i < period; i++ {
		c := createCandle(float64(10+i), time.Now().Add(time.Duration(i)*time.Hour).Format(time.RFC3339))
		c.Symbol = symbol
		strategy.OnCandles(context.Background(), []candle.Candle{c})
	}

	// Create a sequence that will generate buy and sell signals
	testCases := []struct {
		price          float64
		expectedSignal Position
	}{
		{5.0, LongBullish},   // Oversold
		{6.0, Hold},          // Neutral
		{10.0, Hold},         // Neutral
		{15.0, ShortBearish}, // overbought
		{20.0, ShortBearish}, // Overbought
		{25.0, ShortBearish}, // Still overbought
		{15.0, Hold},         // Back to neutral
		{5.0, LongBullish},   // Oversold again
	}

	for i, tc := range testCases {
		c := createCandle(tc.price, time.Now().Add(time.Duration(period+i)*time.Hour).Format(time.RFC3339))
		c.Symbol = symbol

		signal, err := strategy.OnCandles(context.Background(), []candle.Candle{c})
		require.NoError(t, err)
		assert.Equal(t, tc.expectedSignal, signal.Position, "signal mismatch at index %d", i)
	}
}

func TestRSIStrategy_PerformanceMetrics(t *testing.T) {
	symbol := "TEST"
	period := 14
	overbought := 70.0
	oversold := 30.0

	// Create strategy
	strategy := NewRSIStrategy(symbol, period, overbought, oversold, emptyStorage{})

	// Get initial metrics
	metrics := strategy.PerformanceMetrics()

	// Verify metrics
	assert.Equal(t, 0.0, metrics["lastRSI"])
	assert.Equal(t, 0.0, metrics["historySize"])
	assert.Equal(t, float64(strategy.maxHistorySize), metrics["maxHistorySize"])
	assert.Equal(t, float64(period), metrics["period"])
	assert.Equal(t, overbought, metrics["overbought"])
	assert.Equal(t, oversold, metrics["oversold"])

	// Add some candles
	for i := 0; i < period+5; i++ {
		c := createCandle(float64(10+i), time.Now().Add(time.Duration(i)*time.Hour).Format(time.RFC3339))
		c.Symbol = symbol
		strategy.OnCandles(context.Background(), []candle.Candle{c})
	}

	// Get updated metrics
	metrics = strategy.PerformanceMetrics()

	// Verify metrics are updated
	assert.NotEqual(t, 0.0, metrics["lastRSI"])
	assert.Equal(t, float64(period+5), metrics["historySize"])
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
