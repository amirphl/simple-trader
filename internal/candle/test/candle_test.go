package candle

import (
	"context"
	"testing"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/db"

	dbconf "github.com/amirphl/simple-trader/internal/db/conf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Helper function to create test candles
func createTestCandles(symbol string, timeframe string, timestamps []time.Time, opens, highs, lows, closes, volumes []float64) []candle.Candle {
	candles := make([]candle.Candle, len(timestamps))
	for i := range timestamps {
		candles[i] = candle.Candle{
			Timestamp: timestamps[i],
			Open:      opens[i],
			High:      highs[i],
			Low:       lows[i],
			Close:     closes[i],
			Volume:    volumes[i],
			Symbol:    symbol,
			Timeframe: timeframe,
			Source:    "test",
		}
	}
	return candles
}

func TestDefaultAggregator_Aggregate(t *testing.T) {
	// Setup
	aggregator := &candle.DefaultAggregator{}
	now := time.Now().Truncate(time.Minute)

	t.Run("Empty candles", func(t *testing.T) {
		result, err := aggregator.Aggregate([]candle.Candle{}, "5m")
		assert.Nil(t, err)
		assert.Nil(t, result)
	})

	t.Run("Invalid timeframe", func(t *testing.T) {
		candles := createTestCandles("BTC/USD", "1m",
			[]time.Time{now},
			[]float64{10000},
			[]float64{10100},
			[]float64{9900},
			[]float64{10050},
			[]float64{1.5},
		)

		result, err := aggregator.Aggregate(candles, "invalid")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("Invalid candle", func(t *testing.T) {
		invalidCandle := candle.Candle{
			Timestamp: now,
			Open:      10000,
			High:      9900, // High < Low (invalid)
			Low:       10100,
			Close:     10050,
			Volume:    1.5,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
		}

		result, err := aggregator.Aggregate([]candle.Candle{invalidCandle}, "5m")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("Single candle", func(t *testing.T) {
		candles := createTestCandles("BTC/USD", "1m",
			[]time.Time{now},
			[]float64{10000},
			[]float64{10100},
			[]float64{9900},
			[]float64{10050},
			[]float64{1.5},
		)

		result, err := aggregator.Aggregate(candles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 1)

		expected := candle.Candle{
			Timestamp: now.Truncate(5 * time.Minute),
			Open:      10000,
			High:      10100,
			Low:       9900,
			Close:     10050,
			Volume:    1.5,
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[0])
	})

	t.Run("Multiple candles same bucket", func(t *testing.T) {
		// Create 3 candles in the same 5m bucket
		baseTime := now.Truncate(5 * time.Minute)
		candles := createTestCandles("BTC/USD", "1m",
			[]time.Time{
				baseTime,
				baseTime.Add(1 * time.Minute),
				baseTime.Add(2 * time.Minute),
			},
			[]float64{10000, 10050, 10070},
			[]float64{10100, 10150, 10120},
			[]float64{9900, 10000, 10050},
			[]float64{10050, 10070, 10080},
			[]float64{1.5, 2.0, 1.8},
		)

		result, err := aggregator.Aggregate(candles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 1)

		expected := candle.Candle{
			Timestamp: baseTime,
			Open:      10000,
			High:      10150, // Max of all highs
			Low:       9900,  // Min of all lows
			Close:     10080, // Close of last candle
			Volume:    5.3,   // Sum of all volumes
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[0])
	})

	t.Run("Multiple candles different buckets", func(t *testing.T) {
		// Create candles in two different 5m buckets
		baseTime := now.Truncate(5 * time.Minute)
		candles := createTestCandles("BTC/USD", "1m",
			[]time.Time{
				baseTime,
				baseTime.Add(1 * time.Minute),
				baseTime.Add(5 * time.Minute), // New bucket
				baseTime.Add(6 * time.Minute),
			},
			[]float64{10000, 10050, 10100, 10120},
			[]float64{10100, 10150, 10200, 10250},
			[]float64{9900, 10000, 10050, 10100},
			[]float64{10050, 10070, 10150, 10200},
			[]float64{1.5, 2.0, 1.8, 2.5},
		)

		result, err := aggregator.Aggregate(candles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 2)

		// First bucket
		expected1 := candle.Candle{
			Timestamp: baseTime,
			Open:      10000,
			High:      10150,
			Low:       9900,
			Close:     10070,
			Volume:    3.5,
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		// Second bucket
		expected2 := candle.Candle{
			Timestamp: baseTime.Add(5 * time.Minute),
			Open:      10100,
			High:      10250,
			Low:       10050,
			Close:     10200,
			Volume:    4.3,
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected1, result[0])
		assert.Equal(t, expected2, result[1])
	})

	t.Run("Unsorted input candles", func(t *testing.T) {
		// Create unsorted candles
		baseTime := now.Truncate(5 * time.Minute)
		candles := []candle.Candle{
			{
				Timestamp: baseTime.Add(2 * time.Minute),
				Open:      10070,
				High:      10120,
				Low:       10050,
				Close:     10080,
				Volume:    1.8,
				Symbol:    "BTC/USD",
				Timeframe: "1m",
				Source:    "test",
			},
			{
				Timestamp: baseTime,
				Open:      10000,
				High:      10100,
				Low:       9900,
				Close:     10050,
				Volume:    1.5,
				Symbol:    "BTC/USD",
				Timeframe: "1m",
				Source:    "test",
			},
			{
				Timestamp: baseTime.Add(1 * time.Minute),
				Open:      10050,
				High:      10150,
				Low:       10000.0,
				Close:     10070,
				Volume:    2.0,
				Symbol:    "BTC/USD",
				Timeframe: "1m",
				Source:    "test",
			},
		}

		result, err := aggregator.Aggregate(candles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 1)

		expected := candle.Candle{
			Timestamp: baseTime,
			Open:      10000, // Should be from the earliest candle
			High:      10150,
			Low:       9900,
			Close:     10080, // Should be from the latest candle
			Volume:    5.3,
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[0])
	})

	t.Run("Aggregating to larger timeframes", func(t *testing.T) {
		// Test aggregating from 1m to 1h
		baseTime := now.Truncate(time.Hour)

		// Create 60 candles (1 hour worth of 1m candles)
		timestamps := make([]time.Time, 60)
		opens := make([]float64, 60)
		highs := make([]float64, 60)
		lows := make([]float64, 60)
		closes := make([]float64, 60)
		volumes := make([]float64, 60)

		for i := range 60 {
			timestamps[i] = baseTime.Add(time.Duration(i) * time.Minute)
			opens[i] = 10000 + float64(i)
			highs[i] = 10100 + float64(i)
			lows[i] = 9900 + float64(i)
			closes[i] = 10050 + float64(i)
			volumes[i] = 1.0 + float64(i)/10
		}

		candles := createTestCandles("BTC/USD", "1m", timestamps, opens, highs, lows, closes, volumes)

		result, err := aggregator.Aggregate(candles, "1h")
		require.NoError(t, err)
		require.Len(t, result, 1)

		expected := candle.Candle{
			Timestamp: baseTime,
			Open:      10000,                       // First candle's open
			High:      10159,                       // Max high
			Low:       9900,                        // Min low
			Close:     10109,                       // Last candle's close
			Volume:    float64(60*10+59*60/2) / 10, // Sum of all volumes
			Symbol:    "BTC/USD",
			Timeframe: "1h",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[0])
	})
}

func TestDefaultAggregator_AggregateFrom1m(t *testing.T) {
	// Setup
	aggregator := &candle.DefaultAggregator{}
	now := time.Now().Truncate(time.Minute)

	t.Run("Empty candles", func(t *testing.T) {
		result, err := aggregator.AggregateFrom1m([]candle.Candle{}, "5m")
		assert.Nil(t, err)
		assert.Nil(t, result)
	})

	t.Run("Non-1m candles", func(t *testing.T) {
		candles := createTestCandles("BTC/USD", "5m", // Not 1m
			[]time.Time{now},
			[]float64{10000},
			[]float64{10100},
			[]float64{9900},
			[]float64{10050},
			[]float64{1.5},
		)

		result, err := aggregator.AggregateFrom1m(candles, "15m")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "is not 1m")
	})

	t.Run("Valid 1m candles", func(t *testing.T) {
		baseTime := now.Truncate(15 * time.Minute)
		candles := createTestCandles("BTC/USD", "1m",
			[]time.Time{
				baseTime,
				baseTime.Add(1 * time.Minute),
				baseTime.Add(2 * time.Minute),
			},
			[]float64{10000, 10050, 10070},
			[]float64{10100, 10150, 10120},
			[]float64{9900, 10000, 10050},
			[]float64{10050, 10070, 10080},
			[]float64{1.5, 2.0, 1.8},
		)

		result, err := aggregator.AggregateFrom1m(candles, "15m")
		require.NoError(t, err)
		require.Len(t, result, 1)

		expected := candle.Candle{
			Timestamp: baseTime,
			Open:      10000,
			High:      10150,
			Low:       9900,
			Close:     10080,
			Volume:    5.3,
			Symbol:    "BTC/USD",
			Timeframe: "15m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[0])
	})

	t.Run("Mixed timeframes", func(t *testing.T) {
		// Create a mix of 1m and other timeframes
		baseTime := now.Truncate(15 * time.Minute)

		validCandles := createTestCandles("BTC/USD", "1m",
			[]time.Time{baseTime, baseTime.Add(1 * time.Minute)},
			[]float64{10000, 10050},
			[]float64{10100, 10150},
			[]float64{9900, 10000},
			[]float64{10050, 10070},
			[]float64{1.5, 2.0},
		)

		invalidCandle := candle.Candle{
			Timestamp: baseTime.Add(2 * time.Minute),
			Open:      10070,
			High:      10120,
			Low:       10050,
			Close:     10080,
			Volume:    1.8,
			Symbol:    "BTC/USD",
			Timeframe: "5m", // Not 1m
			Source:    "test",
		}

		mixedCandles := append(validCandles, invalidCandle)

		result, err := aggregator.AggregateFrom1m(mixedCandles, "15m")
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "is not 1m")
	})
}

func TestDefaultAggregator_Aggregate1mTimeRange(t *testing.T) {
	// Set up test database
	cfg, cleanup := dbconf.NewTestConfig(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := db.New(*cfg)
	require.NoError(t, err)

	// Create aggregator with storage
	aggregator := candle.NewAggregator(db)

	// Define test time range
	now := time.Now().UTC().Truncate(time.Hour)
	start := now.Add(-60 * time.Minute)
	end := now

	// Test 1: Basic aggregation with valid data
	t.Run("Basic aggregation with valid data", func(t *testing.T) {
		// Insert 60 1-minute candles
		var oneMinCandles []candle.Candle
		for i := range 60 {
			c := candle.Candle{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: start.Add(time.Duration(i) * time.Minute),
				Open:      10000.0 + float64(i),
				High:      10100.0 + float64(i),
				Low:       9900.0 + float64(i),
				Close:     10050.0 + float64(i),
				Volume:    1.5 + float64(i)*0.1,
				Source:    "test",
			}
			oneMinCandles = append(oneMinCandles, c)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err = db.SaveCandles(ctx, oneMinCandles)
		require.NoError(t, err)

		// Aggregate to 15m
		aggregated, err := aggregator.Aggregate1mTimeRange(ctx, "BTC-USDT", start, end, "15m")
		require.NoError(t, err)

		// Should get 4 15-minute candles
		assert.Len(t, aggregated, 4)

		// Verify first candle
		assert.Equal(t, start, aggregated[0].Timestamp)
		assert.Equal(t, oneMinCandles[0].Open, aggregated[0].Open)
		assert.Equal(t, "constructed", aggregated[0].Source)
		assert.Equal(t, "15m", aggregated[0].Timeframe)

		// Verify high is the highest of the 15 candles
		highestHigh := oneMinCandles[0].High
		for i := range 15 {
			if oneMinCandles[i].High > highestHigh {
				highestHigh = oneMinCandles[i].High
			}
		}
		assert.Equal(t, highestHigh, aggregated[0].High)
	})

	// Test 2: Empty time range
	t.Run("Empty time range", func(t *testing.T) {
		emptyStart := now.Add(10 * time.Minute)
		emptyEnd := now.Add(5 * time.Minute) // End before start

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		aggregated, err := aggregator.Aggregate1mTimeRange(ctx, "BTC-USDT", emptyStart, emptyEnd, "5m")
		assert.NoError(t, err)
		assert.Empty(t, aggregated)
	})

	// Test 3: No data in time range
	t.Run("No data in time range", func(t *testing.T) {
		futureStart := now.Add(time.Hour)
		futureEnd := now.Add(2 * time.Hour)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		aggregated, err := aggregator.Aggregate1mTimeRange(ctx, "BTC-USDT", futureStart, futureEnd, "15m")
		assert.NoError(t, err)
		assert.Empty(t, aggregated)
	})

	// Test 4: Invalid timeframe
	t.Run("Invalid timeframe", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := aggregator.Aggregate1mTimeRange(ctx, "BTC-USDT", start, end, "2m")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid timeframe")
	})

	// Test 5: Non-existent symbol
	t.Run("Non-existent symbol", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		aggregated, err := aggregator.Aggregate1mTimeRange(ctx, "NON-EXISTENT", start, end, "15m")
		assert.NoError(t, err)
		assert.Empty(t, aggregated)
	})

	// Test 6: Partial data in time range
	t.Run("Partial data in time range", func(t *testing.T) {
		partialStart := now.Add(-30 * time.Minute)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		aggregated, err := aggregator.Aggregate1mTimeRange(ctx, "BTC-USDT", partialStart, end, "15m")
		require.NoError(t, err)

		// Should get 2 15-minute candles
		assert.Len(t, aggregated, 2)
	})

	// Test 7: Exact timeframe boundary
	t.Run("Exact timeframe boundary", func(t *testing.T) {
		// Create time range exactly matching a 15m boundary
		boundaryStart := now.Truncate(15 * time.Minute)
		boundaryEnd := boundaryStart.Add(15 * time.Minute)

		// Insert 15 1-minute candles exactly in this range
		var boundaryCandles []candle.Candle
		for i := range 15 {
			c := candle.Candle{
				Symbol:    "BOUNDARY-TEST",
				Timeframe: "1m",
				Timestamp: boundaryStart.Add(time.Duration(i) * time.Minute),
				Open:      1000.0 + float64(i),
				High:      1100.0 + float64(i),
				Low:       900.0 + float64(i),
				Close:     1050.0 + float64(i),
				Volume:    1.0,
				Source:    "test",
			}
			boundaryCandles = append(boundaryCandles, c)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err = db.SaveCandles(ctx, boundaryCandles)
		require.NoError(t, err)

		aggregated, err := aggregator.Aggregate1mTimeRange(ctx, "BOUNDARY-TEST", boundaryStart, boundaryEnd, "15m")
		require.NoError(t, err)

		// Should get exactly 1 candle
		assert.Len(t, aggregated, 1)
		assert.Equal(t, boundaryStart, aggregated[0].Timestamp)
		assert.Equal(t, boundaryCandles[0].Open, aggregated[0].Open)
		assert.Equal(t, boundaryCandles[14].Close, aggregated[0].Close)
	})

	// Test 8: Missing candles in the middle
	t.Run("Missing candles in the middle", func(t *testing.T) {
		// Create candles with a gap in the middle
		var gappedCandles []candle.Candle
		gapStart := now.Add(-45 * time.Minute).Truncate(time.Hour)

		// First 15 minutes of candles
		for i := range 15 {
			c := candle.Candle{
				Symbol:    "GAPPED-DATA",
				Timeframe: "1m",
				Timestamp: gapStart.Add(time.Duration(i) * time.Minute),
				Open:      2000.0,
				High:      2100.0,
				Low:       1900.0,
				Close:     2050.0,
				Volume:    1.0,
				Source:    "test",
			}
			gappedCandles = append(gappedCandles, c)
		}

		// Skip 15 minutes

		// Last 15 minutes of candles
		for i := 30; i < 45; i++ {
			c := candle.Candle{
				Symbol:    "GAPPED-DATA",
				Timeframe: "1m",
				Timestamp: gapStart.Add(time.Duration(i) * time.Minute),
				Open:      3000.0,
				High:      3100.0,
				Low:       2900.0,
				Close:     3050.0,
				Volume:    1.0,
				Source:    "test",
			}
			gappedCandles = append(gappedCandles, c)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		err = db.SaveCandles(ctx, gappedCandles)
		require.NoError(t, err)

		aggregated, err := aggregator.Aggregate1mTimeRange(ctx, "GAPPED-DATA", gapStart, gapStart.Add(45*time.Minute), "15m")
		require.NoError(t, err)

		// Should get 3 candles (one will be empty/missing)
		assert.Len(t, aggregated, 2)
	})
}

// Mock implementations for testing
type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) SaveCandle(ctx context.Context, c *candle.Candle) error {
	args := m.Called(ctx, c)
	return args.Error(0)
}

func (m *MockStorage) SaveCandles(ctx context.Context, candles []candle.Candle) error {
	args := m.Called(ctx, candles)
	return args.Error(0)
}

func (m *MockStorage) SaveConstructedCandles(ctx context.Context, candles []candle.Candle) error {
	args := m.Called(ctx, candles)
	return args.Error(0)
}

func (m *MockStorage) GetCandle(ctx context.Context, symbol, timeframe string, timestamp time.Time, source string) (*candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, timestamp, source)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetCandlesV2(ctx context.Context, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(ctx, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetCandlesInRange(ctx context.Context, symbol, timeframe string, start, end time.Time, source string) ([]candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, start, end, source)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetConstructedCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetRawCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatestCandle(ctx context.Context, symbol, timeframe string) (*candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatestCandleInRange(ctx context.Context, symbol, timeframe string, start, end time.Time) (*candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, start, end)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatestConstructedCandle(ctx context.Context, symbol, timeframe string) (*candle.Candle, error) {
	args := m.Called(ctx, symbol, timeframe)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatest1mCandle(ctx context.Context, symbol string) (*candle.Candle, error) {
	args := m.Called(ctx, symbol)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) DeleteCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	args := m.Called(ctx, symbol, timeframe, before)
	return args.Error(0)
}

func (m *MockStorage) DeleteCandlesInRange(ctx context.Context, symbol, timeframe string, start, end time.Time, source string) error {
	args := m.Called(ctx, symbol, timeframe, start, end, source)
	return args.Error(0)
}

func (m *MockStorage) DeleteConstructedCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	args := m.Called(ctx, symbol, timeframe, before)
	return args.Error(0)
}

func (m *MockStorage) GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	args := m.Called(ctx, symbol, timeframe, start, end)
	return args.Int(0), args.Error(1)
}

func (m *MockStorage) GetConstructedCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	args := m.Called(ctx, symbol, timeframe, start, end)
	return args.Int(0), args.Error(1)
}

func (m *MockStorage) UpdateCandle(ctx context.Context, c candle.Candle) error {
	args := m.Called(ctx, c)
	return args.Error(0)
}

func (m *MockStorage) UpdateCandles(ctx context.Context, candles []candle.Candle) error {
	args := m.Called(ctx, candles)
	return args.Error(0)
}

func (m *MockStorage) GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error) {
	args := m.Called(ctx, symbol)
	return args.Get(0).(map[string]any), args.Error(1)
}

func (m *MockStorage) GetMissingCandleRanges(ctx context.Context, symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error) {
	args := m.Called(ctx, symbol, start, end)
	return args.Get(0).([]struct{ Start, End time.Time }), args.Error(1)
}

func (m *MockStorage) GetCandleSourceStats(ctx context.Context, symbol string, start, end time.Time) (map[string]any, error) {
	args := m.Called(ctx, symbol, start, end)
	return args.Get(0).(map[string]any), args.Error(1)
}

type MockAggregator struct {
	mock.Mock
}

func (m *MockAggregator) Aggregate(candles []candle.Candle, timeframe string) ([]candle.Candle, error) {
	args := m.Called(candles, timeframe)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockAggregator) AggregateIncremental(newCandle candle.Candle, existingCandles []candle.Candle, timeframe string) ([]candle.Candle, error) {
	args := m.Called(newCandle, existingCandles, timeframe)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockAggregator) AggregateFrom1m(oneMCandles []candle.Candle, targetTimeframe string) ([]candle.Candle, error) {
	args := m.Called(oneMCandles, targetTimeframe)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockAggregator) Aggregate1mTimeRange(ctx context.Context, symbol string, start, end time.Time, targetTimeframe string) ([]candle.Candle, error) {
	args := m.Called(ctx, symbol, start, end, targetTimeframe)
	return args.Get(0).([]candle.Candle), args.Error(1)
}
