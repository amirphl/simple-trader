package candle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create test candles
func createTestCandles(symbol string, timeframe string, timestamps []time.Time, opens, highs, lows, closes, volumes []float64) []Candle {
	candles := make([]Candle, len(timestamps))
	for i := range timestamps {
		candles[i] = Candle{
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
	aggregator := &DefaultAggregator{}
	now := time.Now().Truncate(time.Minute)

	t.Run("Empty candles", func(t *testing.T) {
		result, err := aggregator.Aggregate([]Candle{}, "5m")
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
		invalidCandle := Candle{
			Timestamp: now,
			Open:      10000,
			High:      9900, // High < Low (invalid)
			Low:       10100,
			Close:     10050,
			Volume:    1.5,
			Symbol:    "BTC/USD",
			Timeframe: "1m",
		}

		result, err := aggregator.Aggregate([]Candle{invalidCandle}, "5m")
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

		expected := Candle{
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

		expected := Candle{
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
		expected1 := Candle{
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
		expected2 := Candle{
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
		candles := []Candle{
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

		expected := Candle{
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

		for i := 0; i < 60; i++ {
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

		expected := Candle{
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
	aggregator := &DefaultAggregator{}
	now := time.Now().Truncate(time.Minute)

	t.Run("Empty candles", func(t *testing.T) {
		result, err := aggregator.AggregateFrom1m([]Candle{}, "5m")
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

		expected := Candle{
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

		invalidCandle := Candle{
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
	aggregator := &DefaultAggregator{}
	now := time.Now().Truncate(time.Minute)

	t.Run("Invalid new candle", func(t *testing.T) {
		existingCandles := []Candle{}
		invalidCandle := Candle{
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
		existingCandles := []Candle{}
		newCandle := Candle{
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
		existingCandles := []Candle{}
		newCandle := Candle{
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

		expected := Candle{
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
		existingCandles := []Candle{
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
		newCandle := Candle{
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

		expected := Candle{
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
		existingCandles := []Candle{
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
		newCandle := Candle{
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
		expected := Candle{
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
		existingCandles := []Candle{
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
		newCandle := Candle{
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
		expected := Candle{
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
		existingCandles := []Candle{
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
		newCandle := Candle{
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
		expected := Candle{
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
		existingCandles := []Candle{}
		newCandle := Candle{
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

		expected := Candle{
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
