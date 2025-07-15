// Package strategy
package strategy

// import (
// 	"testing"
// 	"time"

// 	"github.com/amirphl/simple-trader/internal/candle"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// 	"github.com/stretchr/testify/require"
// )

// // We can reuse the mock storage implementations from the RSI tests

// func TestSMAStrategy_Name(t *testing.T) {
// 	storage := &mockStorage{}
// 	strategy := NewSMAStrategy("TEST", 5, 10, storage)
// 	assert.Equal(t, "SMA Crossover", strategy.Name())
// }

// func TestSMAStrategy_Symbol(t *testing.T) {
// 	storage := &mockStorage{}
// 	strategy := NewSMAStrategy("TEST", 5, 10, storage)
// 	assert.Equal(t, "TEST", strategy.Symbol())
// }

// func TestSMAStrategy_WarmupPeriod(t *testing.T) {
// 	storage := &mockStorage{}
// 	fastPeriod := 5
// 	slowPeriod := 10
// 	strategy := NewSMAStrategy("TEST", fastPeriod, slowPeriod, storage)
// 	assert.Equal(t, slowPeriod, strategy.WarmupPeriod())
// }

// func TestSMAStrategy_OnCandles(t *testing.T) {
// 	symbol := "TEST"

// 	tests := []struct {
// 		name           string
// 		fastPeriod     int
// 		slowPeriod     int
// 		candles        []candle.Candle
// 		expectedSignal []string // "buy", "sell", "hold"
// 		expectedReason []string
// 	}{
// 		{
// 			name:       "Basic SMA crossover signals",
// 			fastPeriod: 3,
// 			slowPeriod: 5,
// 			candles: []candle.Candle{
// 				createCandle(10, "2023-01-01T00:00:00Z"),
// 				createCandle(10, "2023-01-01T01:00:00Z"),
// 				createCandle(10, "2023-01-01T02:00:00Z"),
// 				createCandle(10, "2023-01-01T03:00:00Z"),
// 				createCandle(10, "2023-01-01T04:00:00Z"), // Warmup complete
// 				createCandle(11, "2023-01-01T05:00:00Z"), // Fast SMA starts rising
// 				createCandle(12, "2023-01-01T06:00:00Z"), // Fast SMA crosses above Slow SMA
// 				createCandle(13, "2023-01-01T07:00:00Z"), // Fast SMA still above Slow SMA
// 				createCandle(12, "2023-01-01T08:00:00Z"), // Fast SMA starts falling
// 				createCandle(11, "2023-01-01T09:00:00Z"), // Fast SMA crosses below Slow SMA
// 				createCandle(10, "2023-01-01T10:00:00Z"), // Fast SMA still below Slow SMA
// 			},
// 			expectedSignal: []string{
// 				"hold", "hold", "hold", "hold", "hold",
// 				"hold", "buy", "hold", "hold", "sell", "hold",
// 			},
// 			expectedReason: []string{
// 				"warming up", "warming up", "warming up", "warming up", "warming up",
// 				"insufficient data for crossover detection", "SMA bullish crossover", "no SMA crossover", "no SMA crossover", "SMA bearish crossover", "no SMA crossover",
// 			},
// 		},
// 		{
// 			name:       "Uptrend with no crossovers",
// 			fastPeriod: 3,
// 			slowPeriod: 5,
// 			candles: []candle.Candle{
// 				createCandle(10, "2023-01-01T00:00:00Z"),
// 				createCandle(11, "2023-01-01T01:00:00Z"),
// 				createCandle(12, "2023-01-01T02:00:00Z"),
// 				createCandle(13, "2023-01-01T03:00:00Z"),
// 				createCandle(14, "2023-01-01T04:00:00Z"), // Warmup complete
// 				createCandle(15, "2023-01-01T05:00:00Z"), // Fast SMA already above Slow SMA
// 				createCandle(16, "2023-01-01T06:00:00Z"), // No crossover, still in uptrend
// 			},
// 			expectedSignal: []string{
// 				"hold", "hold", "hold", "hold", "hold",
// 				"hold", "hold",
// 			},
// 			expectedReason: []string{
// 				"warming up", "warming up", "warming up", "warming up", "warming up",
// 				"insufficient data for crossover detection", "no SMA crossover",
// 			},
// 		},
// 		{
// 			name:       "Downtrend with no crossovers",
// 			fastPeriod: 3,
// 			slowPeriod: 5,
// 			candles: []candle.Candle{
// 				createCandle(20, "2023-01-01T00:00:00Z"),
// 				createCandle(19, "2023-01-01T01:00:00Z"),
// 				createCandle(18, "2023-01-01T02:00:00Z"),
// 				createCandle(17, "2023-01-01T03:00:00Z"),
// 				createCandle(16, "2023-01-01T04:00:00Z"), // Warmup complete
// 				createCandle(15, "2023-01-01T05:00:00Z"), // Fast SMA already below Slow SMA
// 				createCandle(14, "2023-01-01T06:00:00Z"), // No crossover, still in downtrend
// 			},
// 			expectedSignal: []string{
// 				"hold", "hold", "hold", "hold", "hold",
// 				"hold", "hold",
// 			},
// 			expectedReason: []string{
// 				"warming up", "warming up", "warming up", "warming up", "warming up",
// 				"insufficient data for crossover detection", "no SMA crossover",
// 			},
// 		},
// 		{
// 			name:       "Multiple crossovers",
// 			fastPeriod: 2,
// 			slowPeriod: 4,
// 			candles: []candle.Candle{
// 				createCandle(10, "2023-01-01T00:00:00Z"),
// 				createCandle(11, "2023-01-01T01:00:00Z"),
// 				createCandle(12, "2023-01-01T02:00:00Z"),
// 				createCandle(13, "2023-01-01T03:00:00Z"), // Warmup complete
// 				createCandle(14, "2023-01-01T04:00:00Z"), // Fast SMA rising faster than Slow SMA
// 				createCandle(15, "2023-01-01T05:00:00Z"), // Fast SMA crosses above Slow SMA
// 				createCandle(13, "2023-01-01T06:00:00Z"), // Fast SMA starts falling
// 				createCandle(11, "2023-01-01T07:00:00Z"), // Fast SMA crosses below Slow SMA
// 				createCandle(10, "2023-01-01T08:00:00Z"), // Fast SMA still below Slow SMA
// 				createCandle(12, "2023-01-01T09:00:00Z"), // Fast SMA starts rising again
// 				createCandle(14, "2023-01-01T10:00:00Z"), // Fast SMA crosses above Slow SMA again
// 			},
// 			expectedSignal: []string{
// 				"hold", "hold", "hold", "hold",
// 				"hold", "buy", "hold", "sell", "hold", "hold", "buy",
// 			},
// 			expectedReason: []string{
// 				"warming up", "warming up", "warming up", "warming up",
// 				"insufficient data for crossover detection", "SMA bullish crossover", "no SMA crossover", "SMA bearish crossover", "no SMA crossover", "no SMA crossover", "SMA bullish crossover",
// 			},
// 		},
// 		{
// 			name:       "No candles",
// 			fastPeriod: 3,
// 			slowPeriod: 5,
// 			candles:    []candle.Candle{},
// 			expectedSignal: []string{
// 				"hold",
// 			},
// 			expectedReason: []string{
// 				"no candles",
// 			},
// 		},
// 		{
// 			name:       "No matching candles",
// 			fastPeriod: 3,
// 			slowPeriod: 5,
// 			candles: []candle.Candle{
// 				{Symbol: "OTHER", Close: 10},
// 			},
// 			expectedSignal: []string{
// 				"hold",
// 			},
// 			expectedReason: []string{
// 				"no matching candles",
// 			},
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			// Use noInitStorage to avoid the need for mocking GetCandles
// 			storage := noInitStorage{}
// 			strategy := NewSMAStrategy(symbol, tt.fastPeriod, tt.slowPeriod, storage)

// 			// Force initialized to true to skip historical data fetching
// 			strategy.initialized = true

// 			if tt.name != "No candles" {
// 				require.Equal(t, len(tt.candles), len(tt.expectedSignal), "test case setup error: candle and expected signal counts don't match")
// 				require.Equal(t, len(tt.candles), len(tt.expectedReason), "test case setup error: candle and expected reason counts don't match")
// 			}

// 			for i, c := range tt.candles {
// 				// Set the correct symbol for the test
// 				if c.Symbol == "" {
// 					c.Symbol = symbol
// 				}

// 				signal, err := strategy.OnCandles([]candle.Candle{c})
// 				require.NoError(t, err)
// 				assert.Equal(t, tt.expectedSignal[i], signal.Action, "signal mismatch at index %d", i)
// 				assert.Equal(t, tt.expectedReason[i], signal.Reason, "reason mismatch at index %d", i)
// 				if c.Symbol == symbol {
// 					assert.Equal(t, c.Timestamp, signal.Time, "timestamp mismatch at index %d", i)
// 				}
// 			}
// 		})
// 	}
// }

// func TestSMAStrategy_HistoricalDataHandling(t *testing.T) {
// 	symbol := "TEST"
// 	fastPeriod := 3
// 	slowPeriod := 5

// 	// Create historical candles with an uptrend
// 	historicalCandles := []candle.Candle{
// 		createCandle(10, "2022-01-01T00:00:00Z"),
// 		createCandle(11, "2022-01-01T01:00:00Z"),
// 		createCandle(12, "2022-01-01T02:00:00Z"),
// 		createCandle(13, "2022-01-01T03:00:00Z"),
// 		createCandle(14, "2022-01-01T04:00:00Z"),
// 		createCandle(15, "2022-01-01T05:00:00Z"),
// 	}

// 	// Create mock storage that returns historical candles
// 	storage := &mockStorage{}
// 	storage.On("GetCandles", mock.Anything, symbol, "1m", mock.Anything, mock.Anything).Return(historicalCandles, nil)

// 	// Create strategy
// 	strategy := NewSMAStrategy(symbol, fastPeriod, slowPeriod, storage)

// 	// First candle should trigger historical data fetch
// 	firstCandle := createCandle(16, "2023-01-01T00:00:00Z")
// 	firstCandle.Symbol = symbol

// 	signal, err := strategy.OnCandles([]candle.Candle{firstCandle})
// 	require.NoError(t, err)

// 	// Should have a valid signal because of historical data
// 	assert.Equal(t, "hold", signal.Action)

// 	// Verify the mock was called
// 	storage.AssertExpectations(t)
// }

// func TestSMAStrategy_MemoryManagement(t *testing.T) {
// 	symbol := "TEST"
// 	fastPeriod := 3
// 	slowPeriod := 5

// 	// Create strategy with a small maxHistorySize for testing
// 	strategy := NewSMAStrategy(symbol, fastPeriod, slowPeriod, emptyStorage{})
// 	strategy.maxHistorySize = 10 // Small size for testing

// 	// Add more candles than maxHistorySize
// 	for i := 0; i < 20; i++ {
// 		c := createCandle(float64(10+i), time.Now().Add(time.Duration(i)*time.Hour).Format(time.RFC3339))
// 		c.Symbol = symbol
// 		strategy.OnCandles([]candle.Candle{c})
// 	}

// 	// Verify that prices array is trimmed
// 	assert.LessOrEqual(t, len(strategy.prices), strategy.maxHistorySize)

// 	// Verify that the most recent prices are kept
// 	assert.Equal(t, 29.0, strategy.prices[len(strategy.prices)-1]) // 10 + 19 = 29
// }

// func TestSMAStrategy_SignalFlipping(t *testing.T) {
// 	symbol := "TEST"
// 	fastPeriod := 2
// 	slowPeriod := 4

// 	// Create strategy
// 	strategy := NewSMAStrategy(symbol, fastPeriod, slowPeriod, emptyStorage{})
// 	strategy.initialized = true

// 	// Create a sequence of candles that will generate a bullish crossover
// 	// First add enough candles to complete warmup
// 	prices := []float64{10, 10, 10, 10} // All equal prices to start
// 	for _, price := range prices {
// 		c := createCandle(price, time.Now().Format(time.RFC3339))
// 		c.Symbol = symbol
// 		strategy.OnCandles([]candle.Candle{c})
// 	}

// 	// Add one more candle to have enough data for crossover detection
// 	c := createCandle(10, time.Now().Add(time.Hour).Format(time.RFC3339))
// 	c.Symbol = symbol
// 	strategy.OnCandles([]candle.Candle{c})

// 	// Now create a bullish crossover
// 	c = createCandle(15, time.Now().Add(2*time.Hour).Format(time.RFC3339))
// 	c.Symbol = symbol
// 	signal, err := strategy.OnCandles([]candle.Candle{c})
// 	require.NoError(t, err)
// 	assert.Equal(t, "buy", signal.Action)
// 	assert.Equal(t, "SMA bullish crossover", signal.Reason)

// 	// Now create a bearish crossover
// 	c = createCandle(5, time.Now().Add(3*time.Hour).Format(time.RFC3339))
// 	c.Symbol = symbol
// 	signal, err = strategy.OnCandles([]candle.Candle{c})
// 	require.NoError(t, err)
// 	assert.Equal(t, "sell", signal.Action)
// 	assert.Equal(t, "SMA bearish crossover", signal.Reason)
// }

// func TestSMAStrategy_PerformanceMetrics(t *testing.T) {
// 	symbol := "TEST"
// 	fastPeriod := 3
// 	slowPeriod := 5

// 	// Create strategy
// 	strategy := NewSMAStrategy(symbol, fastPeriod, slowPeriod, emptyStorage{})

// 	// Get initial metrics
// 	metrics := strategy.PerformanceMetrics()

// 	// Verify metrics
// 	assert.Equal(t, 0.0, metrics["lastFastSMA"])
// 	assert.Equal(t, 0.0, metrics["lastSlowSMA"])
// 	assert.Equal(t, 0.0, metrics["historySize"])
// 	assert.Equal(t, float64(strategy.maxHistorySize), metrics["maxHistorySize"])
// 	assert.Equal(t, float64(fastPeriod), metrics["fastPeriod"])
// 	assert.Equal(t, float64(slowPeriod), metrics["slowPeriod"])

// 	// Add some candles
// 	for i := 0; i < slowPeriod+5; i++ {
// 		c := createCandle(float64(10+i), time.Now().Add(time.Duration(i)*time.Hour).Format(time.RFC3339))
// 		c.Symbol = symbol
// 		strategy.OnCandles([]candle.Candle{c})
// 	}

// 	// Get updated metrics
// 	metrics = strategy.PerformanceMetrics()

// 	// Verify metrics are updated
// 	assert.NotEqual(t, 0.0, metrics["lastFastSMA"])
// 	assert.NotEqual(t, 0.0, metrics["lastSlowSMA"])
// 	assert.Equal(t, float64(slowPeriod+5), metrics["historySize"])
// }
