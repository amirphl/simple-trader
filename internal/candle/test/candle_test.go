package candle

import (
	"errors"
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
				Low:       10000,
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

func TestDefaultAggregator_AggregateIncremental(t *testing.T) {
	// Setup
	aggregator := &candle.DefaultAggregator{}
	now := time.Now().Truncate(time.Minute)

	t.Run("Invalid new candle", func(t *testing.T) {
		existingCandles := []candle.Candle{}
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

		result, err := aggregator.AggregateIncremental(invalidCandle, existingCandles, "5m")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("Invalid timeframe", func(t *testing.T) {
		existingCandles := []candle.Candle{}
		newCandle := candle.Candle{
			Timestamp: now,
			Open:      10000,
			High:      10100,
			Low:       9900,
			Close:     10050,
			Volume:    1.5,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
		}

		result, err := aggregator.AggregateIncremental(newCandle, existingCandles, "invalid")
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("Empty existing candles", func(t *testing.T) {
		existingCandles := []candle.Candle{}
		newCandle := candle.Candle{
			Timestamp: now,
			Open:      10000,
			High:      10100,
			Low:       9900,
			Close:     10050,
			Volume:    1.5,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
		}

		result, err := aggregator.AggregateIncremental(newCandle, existingCandles, "5m")
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

	t.Run("Update existing bucket", func(t *testing.T) {
		baseTime := now.Truncate(5 * time.Minute)
		existingCandles := []candle.Candle{
			{
				Timestamp: baseTime,
				Open:      10000,
				High:      10100,
				Low:       9900,
				Close:     10050,
				Volume:    1.5,
				Symbol:    "BTC/USD",
				Timeframe: "5m",
				Source:    "constructed",
			},
		}

		// New candle in the same bucket
		newCandle := candle.Candle{
			Timestamp: baseTime.Add(2 * time.Minute),
			Open:      10060,
			High:      10200, // Higher than existing
			Low:       9800,  // Lower than existing
			Close:     10100,
			Volume:    2.0,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
			Source:    "test",
		}

		result, err := aggregator.AggregateIncremental(newCandle, existingCandles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 1)

		expected := candle.Candle{
			Timestamp: baseTime,
			Open:      10000, // Original open
			High:      10200, // New high
			Low:       9800,  // New low
			Close:     10100, // New close
			Volume:    3.5,   // Sum of volumes
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[0])
	})

	t.Run("Add new bucket", func(t *testing.T) {
		baseTime := now.Truncate(5 * time.Minute)
		existingCandles := []candle.Candle{
			{
				Timestamp: baseTime,
				Open:      10000,
				High:      10100,
				Low:       9900,
				Close:     10050,
				Volume:    1.5,
				Symbol:    "BTC/USD",
				Timeframe: "5m",
				Source:    "constructed",
			},
		}

		// New candle in a different bucket
		newCandle := candle.Candle{
			Timestamp: baseTime.Add(5 * time.Minute), // Next 5m bucket
			Open:      10060,
			High:      10200,
			Low:       9800,
			Close:     10100,
			Volume:    2.0,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
			Source:    "test",
		}

		result, err := aggregator.AggregateIncremental(newCandle, existingCandles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 2)

		// First candle should be unchanged
		assert.Equal(t, existingCandles[0], result[0])

		// Second candle should be new
		expected := candle.Candle{
			Timestamp: baseTime.Add(5 * time.Minute),
			Open:      10060,
			High:      10200,
			Low:       9800,
			Close:     10100,
			Volume:    2.0,
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[1])
	})

	t.Run("Multiple existing buckets", func(t *testing.T) {
		baseTime := now.Truncate(5 * time.Minute)
		existingCandles := []candle.Candle{
			{
				Timestamp: baseTime,
				Open:      10000,
				High:      10100,
				Low:       9900,
				Close:     10050,
				Volume:    1.5,
				Symbol:    "BTC/USD",
				Timeframe: "5m",
				Source:    "constructed",
			},
			{
				Timestamp: baseTime.Add(5 * time.Minute),
				Open:      10060,
				High:      10200,
				Low:       9800,
				Close:     10100,
				Volume:    2.0,
				Symbol:    "BTC/USD",
				Timeframe: "5m",
				Source:    "constructed",
			},
		}

		// New candle updating the second bucket
		newCandle := candle.Candle{
			Timestamp: baseTime.Add(7 * time.Minute),
			Open:      10150,
			High:      10300,
			Low:       10000,
			Close:     10250,
			Volume:    2.5,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
			Source:    "test",
		}

		result, err := aggregator.AggregateIncremental(newCandle, existingCandles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 2)

		// First candle should be unchanged
		assert.Equal(t, existingCandles[0], result[0])

		// Second candle should be updated
		expected := candle.Candle{
			Timestamp: baseTime.Add(5 * time.Minute),
			Open:      10060,
			High:      10300, // Updated
			Low:       9800,
			Close:     10250, // Updated
			Volume:    4.5,   // Updated
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[1])
	})

	t.Run("Unsorted existing candles", func(t *testing.T) {
		baseTime := now.Truncate(5 * time.Minute)
		existingCandles := []candle.Candle{
			{
				Timestamp: baseTime.Add(5 * time.Minute),
				Open:      10060,
				High:      10200,
				Low:       9800,
				Close:     10100,
				Volume:    2.0,
				Symbol:    "BTC/USD",
				Timeframe: "5m",
				Source:    "constructed",
			},
			{
				Timestamp: baseTime,
				Open:      10000,
				High:      10100,
				Low:       9900,
				Close:     10050,
				Volume:    1.5,
				Symbol:    "BTC/USD",
				Timeframe: "5m",
				Source:    "constructed",
			},
		}

		// New candle for a new bucket
		newCandle := candle.Candle{
			Timestamp: baseTime.Add(10 * time.Minute),
			Open:      10150,
			High:      10300,
			Low:       10000,
			Close:     10250,
			Volume:    2.5,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
			Source:    "test",
		}

		result, err := aggregator.AggregateIncremental(newCandle, existingCandles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 3)

		// Result should be sorted
		assert.True(t, result[0].Timestamp.Before(result[1].Timestamp))
		assert.True(t, result[1].Timestamp.Before(result[2].Timestamp))

		// New candle should be the last one
		expected := candle.Candle{
			Timestamp: baseTime.Add(10 * time.Minute),
			Open:      10150,
			High:      10300,
			Low:       10000,
			Close:     10250,
			Volume:    2.5,
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[2])
	})

	t.Run("Zero volume candle", func(t *testing.T) {
		existingCandles := []candle.Candle{}
		newCandle := candle.Candle{
			Timestamp: now,
			Open:      10000,
			High:      10100,
			Low:       9900,
			Close:     10050,
			Volume:    0, // Zero volume
			Symbol:    "BTC/USD",
			Timeframe: "1m",
			Source:    "test",
		}

		result, err := aggregator.AggregateIncremental(newCandle, existingCandles, "5m")
		require.NoError(t, err)
		require.Len(t, result, 1)

		expected := candle.Candle{
			Timestamp: now.Truncate(5 * time.Minute),
			Open:      10000,
			High:      10100,
			Low:       9900,
			Close:     10050,
			Volume:    0, // Zero volume
			Symbol:    "BTC/USD",
			Timeframe: "5m",
			Source:    "constructed",
		}

		assert.Equal(t, expected, result[0])
	})
}

func TestDefaultAggregator_Aggregate1mTimeRange(t *testing.T) {
	// Set up test database
	cfg, cleanup := dbconf.NewTestConfig(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := db.New(*cfg)
	require.NoError(t, err)

	// Create aggregator
	aggregator := &candle.DefaultAggregator{}

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

		err = db.SaveCandles(oneMinCandles)
		require.NoError(t, err)

		// Aggregate to 15m
		aggregated, err := aggregator.Aggregate1mTimeRange("BTC-USDT", start, end, "15m", db)
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

		aggregated, err := aggregator.Aggregate1mTimeRange("BTC-USDT", emptyStart, emptyEnd, "5m", db)
		assert.NoError(t, err)
		assert.Empty(t, aggregated)
	})

	// Test 3: No data in time range
	t.Run("No data in time range", func(t *testing.T) {
		futureStart := now.Add(time.Hour)
		futureEnd := now.Add(2 * time.Hour)

		aggregated, err := aggregator.Aggregate1mTimeRange("BTC-USDT", futureStart, futureEnd, "15m", db)
		assert.NoError(t, err)
		assert.Empty(t, aggregated)
	})

	// Test 4: Invalid timeframe
	t.Run("Invalid timeframe", func(t *testing.T) {
		_, err := aggregator.Aggregate1mTimeRange("BTC-USDT", start, end, "2m", db)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid timeframe")
	})

	// Test 5: Non-existent symbol
	t.Run("Non-existent symbol", func(t *testing.T) {
		aggregated, err := aggregator.Aggregate1mTimeRange("NON-EXISTENT", start, end, "15m", db)
		assert.NoError(t, err)
		assert.Empty(t, aggregated)
	})

	// Test 6: Partial data in time range
	t.Run("Partial data in time range", func(t *testing.T) {
		partialStart := now.Add(-30 * time.Minute)

		aggregated, err := aggregator.Aggregate1mTimeRange("BTC-USDT", partialStart, end, "15m", db)
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

		err = db.SaveCandles(boundaryCandles)
		require.NoError(t, err)

		aggregated, err := aggregator.Aggregate1mTimeRange("BOUNDARY-TEST", boundaryStart, boundaryEnd, "15m", db)
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

		err = db.SaveCandles(gappedCandles)
		require.NoError(t, err)

		aggregated, err := aggregator.Aggregate1mTimeRange("GAPPED-DATA", gapStart, gapStart.Add(45*time.Minute), "15m", db)
		require.NoError(t, err)

		// Should get 3 candles (one will be empty/missing)
		assert.Len(t, aggregated, 2)
	})
}

// Mock implementations for testing
type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) SaveCandle(c *candle.Candle) error {
	args := m.Called(c)
	return args.Error(0)
}

func (m *MockStorage) SaveCandles(candles []candle.Candle) error {
	args := m.Called(candles)
	return args.Error(0)
}

func (m *MockStorage) SaveConstructedCandles(candles []candle.Candle) error {
	args := m.Called(candles)
	return args.Error(0)
}

func (m *MockStorage) GetCandle(symbol, timeframe string, timestamp time.Time, source string) (*candle.Candle, error) {
	args := m.Called(symbol, timeframe, timestamp, source)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetCandles(symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(symbol, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetCandlesV2(timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetCandlesInRange(symbol, timeframe string, start, end time.Time, source string) ([]candle.Candle, error) {
	args := m.Called(symbol, timeframe, start, end, source)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetConstructedCandles(symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(symbol, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetRawCandles(symbol, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	args := m.Called(symbol, timeframe, start, end)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatestCandle(symbol, timeframe string) (*candle.Candle, error) {
	args := m.Called(symbol, timeframe)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatestCandleInRange(symbol, timeframe string, start, end time.Time) (*candle.Candle, error) {
	args := m.Called(symbol, timeframe, start, end)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatestConstructedCandle(symbol, timeframe string) (*candle.Candle, error) {
	args := m.Called(symbol, timeframe)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) GetLatest1mCandle(symbol string) (*candle.Candle, error) {
	args := m.Called(symbol)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*candle.Candle), args.Error(1)
}

func (m *MockStorage) DeleteCandles(symbol, timeframe string, before time.Time) error {
	args := m.Called(symbol, timeframe, before)
	return args.Error(0)
}

func (m *MockStorage) DeleteCandlesInRange(symbol, timeframe string, start, end time.Time, source string) error {
	args := m.Called(symbol, timeframe, start, end, source)
	return args.Error(0)
}

func (m *MockStorage) DeleteConstructedCandles(symbol, timeframe string, before time.Time) error {
	args := m.Called(symbol, timeframe, before)
	return args.Error(0)
}

func (m *MockStorage) GetCandleCount(symbol, timeframe string, start, end time.Time) (int, error) {
	args := m.Called(symbol, timeframe, start, end)
	return args.Int(0), args.Error(1)
}

func (m *MockStorage) GetConstructedCandleCount(symbol, timeframe string, start, end time.Time) (int, error) {
	args := m.Called(symbol, timeframe, start, end)
	return args.Int(0), args.Error(1)
}

func (m *MockStorage) UpdateCandle(c candle.Candle) error {
	args := m.Called(c)
	return args.Error(0)
}

func (m *MockStorage) UpdateCandles(candles []candle.Candle) error {
	args := m.Called(candles)
	return args.Error(0)
}

func (m *MockStorage) GetAggregationStats(symbol string) (map[string]any, error) {
	args := m.Called(symbol)
	return args.Get(0).(map[string]any), args.Error(1)
}

func (m *MockStorage) GetMissingCandleRanges(symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error) {
	args := m.Called(symbol, start, end)
	return args.Get(0).([]struct{ Start, End time.Time }), args.Error(1)
}

func (m *MockStorage) GetCandleSourceStats(symbol string, start, end time.Time) (map[string]any, error) {
	args := m.Called(symbol, start, end)
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

func (m *MockAggregator) Aggregate1mTimeRange(symbol string, start, end time.Time, targetTimeframe string, storage candle.Storage) ([]candle.Candle, error) {
	args := m.Called(symbol, start, end, targetTimeframe, storage)
	return args.Get(0).([]candle.Candle), args.Error(1)
}

func TestDefaultIngester_IngestCandle(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Basic ingestion of non-1m candle
	t.Run("Basic ingestion of non-1m candle", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Set up expectations
		mockStorage.On("SaveCandles", []candle.Candle{testCandle}).Return(nil)

		// Execute
		err := ingester.IngestCandle(testCandle)

		// Verify
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t) // Should not call aggregator for non-1m candles
	})

	// Test 2: Ingestion of 1m candle with aggregation
	t.Run("Ingestion of 1m candle with aggregation", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for saving the original candle
		mockStorage.On("SaveCandles", []candle.Candle{testCandle}).Return(nil)

		// Set up expectations for aggregation
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{testCandle}, nil)

		// Mock getting latest candles for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return((*candle.Candle)(nil), nil)
		}

		// Mock aggregation results for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			aggregatedCandle := candle.Candle{
				Symbol:    "BTC-USDT",
				Timeframe: tf,
				Timestamp: now.Truncate(candle.GetTimeframeDuration(tf)),
				Open:      testCandle.Open,
				High:      testCandle.High,
				Low:       testCandle.Low,
				Close:     testCandle.Close,
				Volume:    testCandle.Volume,
				Source:    "constructed",
			}

			mockAggregator.On("AggregateFrom1m", mock.Anything, tf).Return([]candle.Candle{aggregatedCandle}, nil).Once()
		}

		// Mock saving constructed candles
		mockStorage.On("SaveConstructedCandles", mock.Anything).Return(nil)

		// Execute
		err := ingester.IngestCandle(testCandle)

		// Verify
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t)
	})

	// Test 3: Invalid candle
	t.Run("Invalid candle", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		// Invalid candle with negative price
		invalidCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      -10000.0, // Negative price is invalid
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Execute
		err := ingester.IngestCandle(invalidCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid candle")
		mockStorage.AssertNotCalled(t, "SaveCandles")
	})

	// Test 4: Storage error
	t.Run("Storage error", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Set up expectations for storage error
		mockStorage.On("SaveCandles", []candle.Candle{testCandle}).Return(errors.New("database error"))

		// Execute
		err := ingester.IngestCandle(testCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to save candle")
		mockStorage.AssertExpectations(t)
	})

	// Test 5: Aggregation error
	t.Run("Aggregation error", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Set up expectations
		mockStorage.On("SaveCandles", []candle.Candle{testCandle}).Return(nil)

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Mock error when getting candles for aggregation
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{}, errors.New("database error"))

		// Execute
		err := ingester.IngestCandle(testCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get 1m candles")
		mockStorage.AssertExpectations(t)
	})

	// Test 6: Empty symbol
	t.Run("Empty symbol", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		invalidCandle := candle.Candle{
			Symbol:    "", // Empty symbol
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Execute
		err := ingester.IngestCandle(invalidCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle symbol cannot be empty")
	})

	// Test 7: Zero timestamp
	t.Run("Zero timestamp", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		invalidCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: time.Time{}, // Zero timestamp
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Execute
		err := ingester.IngestCandle(invalidCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle timestamp is zero")
	})

	// Test 8: Update existing candle in higher timeframe
	t.Run("Update existing candle in higher timeframe", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for saving the original candle
		mockStorage.On("SaveCandles", []candle.Candle{testCandle}).Return(nil)
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{testCandle}, nil)

		// For 5m timeframe, simulate an existing candle that needs updating
		fiveMinStart := now.Truncate(5 * time.Minute)
		existingFiveMinCandle := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: fiveMinStart,
			Open:      9900.0,
			High:      10000.0,
			Low:       9800.0,
			Close:     9950.0,
			Volume:    1.0,
			Source:    "constructed",
		}

		// For other timeframes, return nil (no existing candle)
		for _, tf := range candle.GetAggregationTimeframes() {
			if tf == "5m" {
				mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return(existingFiveMinCandle, nil)
			} else {
				mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return((*candle.Candle)(nil), nil)
			}
		}

		// Mock aggregation results for 5m timeframe (updating existing)
		updatedFiveMinCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: fiveMinStart,
			Open:      9900.0,  // Unchanged
			High:      10100.0, // Updated
			Low:       9800.0,  // Unchanged
			Close:     10050.0, // Updated
			Volume:    2.5,     // Updated
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.Anything, "5m").Return([]candle.Candle{updatedFiveMinCandle}, nil)

		// Mock for other timeframes (new candles)
		for _, tf := range candle.GetAggregationTimeframes() {
			if tf != "5m" {
				aggregatedCandle := candle.Candle{
					Symbol:    "BTC-USDT",
					Timeframe: tf,
					Timestamp: now.Truncate(candle.GetTimeframeDuration(tf)),
					Open:      testCandle.Open,
					High:      testCandle.High,
					Low:       testCandle.Low,
					Close:     testCandle.Close,
					Volume:    testCandle.Volume,
					Source:    "constructed",
				}
				mockAggregator.On("AggregateFrom1m", mock.Anything, tf).Return([]candle.Candle{aggregatedCandle}, nil)
			}
		}

		// Mock updating and saving candles
		mockStorage.On("UpdateCandles", mock.Anything).Return(nil)
		mockStorage.On("SaveConstructedCandles", mock.Anything).Return(nil)

		// Execute
		err := ingester.IngestCandle(testCandle)

		// Verify
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t)
	})

	// Test 9: No 1m candles found for aggregation
	t.Run("No 1m candles found for aggregation", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for saving the original candle
		mockStorage.On("SaveCandles", []candle.Candle{testCandle}).Return(nil)

		// Return empty slice (no candles found)
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{}, nil)

		// Execute
		err := ingester.IngestCandle(testCandle)

		// Verify
		assert.NoError(t, err) // Should not error, just skip aggregation
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertNotCalled(t, "AggregateFrom1m")
	})
}

func TestDefaultIngester_AggregateToHigherTimeframes(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Basic aggregation with a single 1m candle
	t.Run("Basic aggregation with a single 1m candle", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for getting candles
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{testCandle}, nil)

		// Mock getting latest candles for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return((*candle.Candle)(nil), nil)
		}

		// Mock aggregation results for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			aggregatedCandle := candle.Candle{
				Symbol:    "BTC-USDT",
				Timeframe: tf,
				Timestamp: now.Truncate(candle.GetTimeframeDuration(tf)),
				Open:      testCandle.Open,
				High:      testCandle.High,
				Low:       testCandle.Low,
				Close:     testCandle.Close,
				Volume:    testCandle.Volume,
				Source:    "constructed",
			}

			mockAggregator.On("AggregateFrom1m", mock.Anything, tf).Return([]candle.Candle{aggregatedCandle}, nil).Once()
		}

		// Mock saving constructed candles
		mockStorage.On("SaveConstructedCandles", mock.Anything).Return(nil)

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t)
	})

	// Test 2: Error getting 1m candles
	t.Run("Error getting 1m candles", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Mock error when getting candles
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{}, errors.New("database error"))

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get 1m candles")
		mockStorage.AssertExpectations(t)
	})

	// Test 3: No 1m candles found
	t.Run("No 1m candles found", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Return empty slice (no candles found)
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{}, nil)

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.NoError(t, err) // Should not error, just return
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertNotCalled(t, "AggregateFrom1m")
	})

	// Test 4: Error getting latest candle
	t.Run("Error getting latest candle", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for getting candles
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{testCandle}, nil)

		// Mock error when getting latest candle for 5m timeframe
		mockStorage.On("GetLatestCandle", "BTC-USDT", "5m").Return((*candle.Candle)(nil), errors.New("database error"))

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get latest 5m candle")
		mockStorage.AssertExpectations(t)
	})

	// Test 5: Error in aggregation
	t.Run("Error in aggregation", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for getting candles
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{testCandle}, nil)

		// Mock getting latest candles for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return((*candle.Candle)(nil), nil)
		}

		// Mock aggregation error for 5m timeframe
		mockAggregator.On("AggregateFrom1m", mock.Anything, "5m").Return([]candle.Candle{}, errors.New("aggregation error")).Once()

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to aggregate to 5m")
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t)
	})

	// Test 6: Error saving constructed candles
	t.Run("Error saving constructed candles", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for getting candles
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{testCandle}, nil)

		// Mock getting latest candles for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return((*candle.Candle)(nil), nil)
		}

		// Mock aggregation results for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			aggregatedCandle := candle.Candle{
				Symbol:    "BTC-USDT",
				Timeframe: tf,
				Timestamp: now.Truncate(candle.GetTimeframeDuration(tf)),
				Open:      testCandle.Open,
				High:      testCandle.High,
				Low:       testCandle.Low,
				Close:     testCandle.Close,
				Volume:    testCandle.Volume,
				Source:    "constructed",
			}

			mockAggregator.On("AggregateFrom1m", mock.Anything, tf).Return([]candle.Candle{aggregatedCandle}, nil).Once()
		}

		// Mock error when saving constructed candles
		mockStorage.On("SaveConstructedCandles", mock.Anything).Return(errors.New("database error"))

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to save constructed candles")
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t)
	})

	// Test 7: Error updating candles
	t.Run("Error updating candles", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := now.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Set up expectations for getting candles
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return([]candle.Candle{testCandle}, nil)

		// For 5m timeframe, simulate an existing candle that needs updating
		fiveMinStart := now.Truncate(5 * time.Minute)
		existingFiveMinCandle := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: fiveMinStart,
			Open:      9900.0,
			High:      10000.0,
			Low:       9800.0,
			Close:     9950.0,
			Volume:    1.0,
			Source:    "constructed",
		}

		// Mock getting latest candles for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			if tf == "5m" {
				mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return(existingFiveMinCandle, nil)
			} else {
				mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return((*candle.Candle)(nil), nil)
			}
		}

		// Mock aggregation results for 5m timeframe (updating existing)
		updatedFiveMinCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: fiveMinStart,
			Open:      9900.0,
			High:      10100.0,
			Low:       9800.0,
			Close:     10050.0,
			Volume:    2.5,
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.Anything, "5m").Return([]candle.Candle{updatedFiveMinCandle}, nil)

		// Mock for other timeframes (new candles)
		for _, tf := range candle.GetAggregationTimeframes() {
			if tf != "5m" {
				aggregatedCandle := candle.Candle{
					Symbol:    "BTC-USDT",
					Timeframe: tf,
					Timestamp: now.Truncate(candle.GetTimeframeDuration(tf)),
					Open:      testCandle.Open,
					High:      testCandle.High,
					Low:       testCandle.Low,
					Close:     testCandle.Close,
					Volume:    testCandle.Volume,
					Source:    "constructed",
				}
				mockAggregator.On("AggregateFrom1m", mock.Anything, tf).Return([]candle.Candle{aggregatedCandle}, nil)
			}
		}

		// Mock error when updating candles
		mockStorage.On("UpdateCandles", mock.Anything).Return(errors.New("database error"))

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to update candles")
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t)
	})

	// Test 8: Complex scenario - mix of updates and new candles
	t.Run("Complex scenario - mix of updates and new candles", func(t *testing.T) {
		mockStorage := new(MockStorage)
		mockAggregator := new(MockAggregator)

		ingester := candle.NewCandleIngester(mockStorage, mockAggregator).(*candle.DefaultIngester)

		// Create a candle at a specific time that will trigger both updates and new candles
		// For example, a candle at 10:14:00 will:
		// - Update existing 5m candle (10:10-10:15)
		// - Update existing 15m candle (10:00-10:15)
		// - Create new 30m candle (10:00-10:30)
		// - Create new 1h candle (10:00-11:00)
		specificTime := time.Date(2023, 5, 15, 10, 14, 0, 0, time.UTC)

		testCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: specificTime,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		// Mock 1-day bucket for aggregation
		dayStart := specificTime.Truncate(24 * time.Hour)
		dayEnd := dayStart.Add(24 * time.Hour)

		// Create multiple 1m candles for the day
		oneMCandles := []candle.Candle{
			// Previous candles in the 5m bucket
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: specificTime.Add(-4 * time.Minute),
				Open:      9800.0,
				High:      9850.0,
				Low:       9750.0,
				Close:     9820.0,
				Volume:    1.2,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: specificTime.Add(-3 * time.Minute),
				Open:      9820.0,
				High:      9900.0,
				Low:       9800.0,
				Close:     9880.0,
				Volume:    1.3,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: specificTime.Add(-2 * time.Minute),
				Open:      9880.0,
				High:      9950.0,
				Low:       9870.0,
				Close:     9920.0,
				Volume:    1.4,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: specificTime.Add(-1 * time.Minute),
				Open:      9920.0,
				High:      10000.0,
				Low:       9900.0,
				Close:     9980.0,
				Volume:    1.5,
				Source:    "test",
			},
			// Current candle
			testCandle,
		}

		// Set up expectations for getting candles
		mockStorage.On("GetCandles", "BTC-USDT", "1m", dayStart, dayEnd).Return(oneMCandles, nil)

		// Set up existing candles for different timeframes
		fiveMinStart := specificTime.Truncate(5 * time.Minute)
		fifteenMinStart := specificTime.Truncate(15 * time.Minute)

		// 5m and 15m candles exist and will be updated
		existingFiveMinCandle := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: fiveMinStart,
			Open:      9800.0,
			High:      10000.0,
			Low:       9750.0,
			Close:     9980.0,
			Volume:    5.4, // Sum of previous 4 candles
			Source:    "constructed",
		}

		existingFifteenMinCandle := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "15m",
			Timestamp: fifteenMinStart,
			Open:      9500.0, // From earlier candles
			High:      10000.0,
			Low:       9400.0,
			Close:     9980.0,
			Volume:    15.0,
			Source:    "constructed",
		}

		// Mock getting latest candles for each timeframe
		for _, tf := range candle.GetAggregationTimeframes() {
			switch tf {
			case "5m":
				mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return(existingFiveMinCandle, nil)
			case "15m":
				mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return(existingFifteenMinCandle, nil)
			default:
				mockStorage.On("GetLatestCandle", "BTC-USDT", tf).Return((*candle.Candle)(nil), nil)
			}
		}

		// Mock aggregation results for each timeframe

		// 5m - update existing
		updatedFiveMinCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: fiveMinStart,
			Open:      9800.0,  // Unchanged
			High:      10100.0, // Updated from new candle
			Low:       9750.0,  // Unchanged
			Close:     10050.0, // Updated from new candle
			Volume:    6.9,     // Updated with new candle
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.MatchedBy(func(candles []candle.Candle) bool {
			return len(candles) > 0 && candles[0].Timestamp.Equal(specificTime.Add(-4*time.Minute))
		}), "5m").Return([]candle.Candle{updatedFiveMinCandle}, nil)

		// 15m - update existing
		updatedFifteenMinCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "15m",
			Timestamp: fifteenMinStart,
			Open:      9500.0,  // Unchanged
			High:      10100.0, // Updated from new candle
			Low:       9400.0,  // Unchanged
			Close:     10050.0, // Updated from new candle
			Volume:    16.5,    // Updated with new candle
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.MatchedBy(func(candles []candle.Candle) bool {
			return len(candles) > 0 && candles[0].Timestamp.After(fifteenMinStart.Add(-time.Minute))
		}), "15m").Return([]candle.Candle{updatedFifteenMinCandle}, nil)

		// 30m - new candle
		thirtyMinCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "30m",
			Timestamp: specificTime.Truncate(30 * time.Minute),
			Open:      9400.0,  // From earlier candles
			High:      10100.0, // Including new candle
			Low:       9300.0,  // From earlier candles
			Close:     10050.0, // From new candle
			Volume:    25.0,    // Including new candle
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.Anything, "30m").Return([]candle.Candle{thirtyMinCandle}, nil)

		// 1h - new candle
		oneHourCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1h",
			Timestamp: specificTime.Truncate(time.Hour),
			Open:      9200.0,  // From earlier candles
			High:      10100.0, // Including new candle
			Low:       9100.0,  // From earlier candles
			Close:     10050.0, // From new candle
			Volume:    45.0,    // Including new candle
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.Anything, "1h").Return([]candle.Candle{oneHourCandle}, nil)

		// 4h - new candle
		fourHourCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "4h",
			Timestamp: specificTime.Truncate(4 * time.Hour),
			Open:      8800.0,  // From earlier candles
			High:      10100.0, // Including new candle
			Low:       8700.0,  // From earlier candles
			Close:     10050.0, // From new candle
			Volume:    120.0,   // Including new candle
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.Anything, "4h").Return([]candle.Candle{fourHourCandle}, nil)

		// 1d - new candle
		oneDayCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1d",
			Timestamp: specificTime.Truncate(24 * time.Hour),
			Open:      8500.0,  // From earlier candles
			High:      10100.0, // Including new candle
			Low:       8400.0,  // From earlier candles
			Close:     10050.0, // From new candle
			Volume:    350.0,   // Including new candle
			Source:    "constructed",
		}
		mockAggregator.On("AggregateFrom1m", mock.Anything, "1d").Return([]candle.Candle{oneDayCandle}, nil)

		// Mock updating and saving candles
		mockStorage.On("UpdateCandles", mock.MatchedBy(func(candles []candle.Candle) bool {
			if len(candles) != 2 {
				return false
			}
			// Should contain 5m and 15m updates
			return (candles[0].Timeframe == "5m" || candles[1].Timeframe == "5m") &&
				(candles[0].Timeframe == "15m" || candles[1].Timeframe == "15m")
		})).Return(nil)

		mockStorage.On("SaveConstructedCandles", mock.MatchedBy(func(candles []candle.Candle) bool {
			if len(candles) != 4 {
				return false
			}
			// Should contain 30m, 1h, 4h, and 1d new candles
			hasTimeframes := make(map[string]bool)
			for _, c := range candles {
				hasTimeframes[c.Timeframe] = true
			}
			return hasTimeframes["30m"] && hasTimeframes["1h"] && hasTimeframes["4h"] && hasTimeframes["1d"]
		})).Return(nil)

		// Execute
		err := ingester.AggregateToHigherTimeframes(testCandle)

		// Verify
		assert.NoError(t, err)
		mockStorage.AssertExpectations(t)
		mockAggregator.AssertExpectations(t)
	})
}
