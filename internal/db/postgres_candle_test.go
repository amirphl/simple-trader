package db

import (
	"testing"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresDB_SaveCandle(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Test 1: Basic candle save
	t.Run("Basic candle save", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
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

		err := db.SaveCandle(c)
		assert.NoError(t, err)

		// Verify the candle was saved
		retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, c.Symbol, retrieved.Symbol)
		assert.Equal(t, c.Timeframe, retrieved.Timeframe)
		assert.Equal(t, c.Timestamp.Unix(), retrieved.Timestamp.Unix())
		assert.Equal(t, c.Open, retrieved.Open)
		assert.Equal(t, c.High, retrieved.High)
		assert.Equal(t, c.Low, retrieved.Low)
		assert.Equal(t, c.Close, retrieved.Close)
		assert.Equal(t, c.Volume, retrieved.Volume)
		assert.Equal(t, c.Source, retrieved.Source)
	})

	// Test 2: Save candle with zero timestamp
	t.Run("Zero timestamp", func(t *testing.T) {
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: time.Time{}, // Zero time
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle timestamp is zero")
	})

	// Test 3: Save candle with invalid prices
	t.Run("Invalid prices", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      0.0, // Invalid: zero price
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle prices must be positive")
	})

	// Test 4: Save candle with high < low
	t.Run("High less than low", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      9900.0, // High < Low
			Low:       10100.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle high cannot be less than low")
	})

	// Test 5: Save candle with open outside high-low range
	t.Run("Open outside high-low range", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10200.0, // Open > High
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle open price must be between high and low")
	})

	// Test 6: Save candle with close outside high-low range
	t.Run("Close outside high-low range", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     9800.0, // Close < Low
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle close price must be between high and low")
	})

	// Test 7: Save candle with negative volume
	t.Run("Negative volume", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    -1.5, // Negative volume
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle volume cannot be negative")
	})

	// Test 8: Save candle with empty symbol
	t.Run("Empty symbol", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
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

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle symbol cannot be empty")
	})

	// Test 9: Save candle with empty timeframe
	t.Run("Empty timeframe", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "", // Empty timeframe
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle timeframe cannot be empty")
	})

	// Test 10: Save candle with extreme values
	t.Run("Extreme values", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      1e-10, // Very small value
			High:      1e10,  // Very large value
			Low:       1e-10,
			Close:     1e5,
			Volume:    1e-10,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.NoError(t, err) // Should accept extreme values as long as they're valid

		// Verify the candle was saved
		retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, c.Open, retrieved.Open)
		assert.Equal(t, c.High, retrieved.High)
		assert.Equal(t, c.Low, retrieved.Low)
		assert.Equal(t, c.Close, retrieved.Close)
		assert.Equal(t, c.Volume, retrieved.Volume)
	})

	// Test 11: Save candle with special characters in symbol
	t.Run("Special characters in symbol", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		c := &candle.Candle{
			Symbol:    "BTC/USDT-2024", // Special characters
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.NoError(t, err)

		// Verify the candle was saved
		retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, c.Symbol, retrieved.Symbol)
	})

	// Test 12: Save candle with very long symbol name
	t.Run("Very long symbol name", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		longSymbol := "VERY-LONG-SYMBOL-NAME-THAT-EXCEEDS-NORMAL-LENGTH"
		c := &candle.Candle{
			Symbol:    longSymbol,
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		}

		err := db.SaveCandle(c)
		assert.Error(t, err) // Should fail due to symbol length constraint
		assert.Contains(t, err.Error(), "value too long for type character varying")
	})
}

func TestPostgresDB_SaveCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Test 1: Save multiple valid candles
	t.Run("Multiple valid candles", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-time.Minute),
				Open:      9900.0,
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "test",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    10.0,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		assert.NoError(t, err)

		// Verify all candles were saved
		for _, c := range candles {
			retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
			assert.NoError(t, err)
			assert.NotNil(t, retrieved)
			assert.Equal(t, c.Symbol, retrieved.Symbol)
			assert.Equal(t, c.Timeframe, retrieved.Timeframe)
			assert.Equal(t, c.Timestamp.Unix(), retrieved.Timestamp.Unix())
			assert.Equal(t, c.Open, retrieved.Open)
			assert.Equal(t, c.High, retrieved.High)
			assert.Equal(t, c.Low, retrieved.Low)
			assert.Equal(t, c.Close, retrieved.Close)
			assert.Equal(t, c.Volume, retrieved.Volume)
			assert.Equal(t, c.Source, retrieved.Source)
		}
	})

	// Test 2: Save empty slice
	t.Run("Empty slice", func(t *testing.T) {
		candles := []candle.Candle{}
		err := db.SaveCandles(candles)
		assert.NoError(t, err) // Should not error with empty slice
	})

	// Test 3: Save candles with one invalid candle
	t.Run("One invalid candle in batch", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-time.Minute),
				Open:      0.0, // Invalid: zero price
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle prices must be positive")
	})

	// Test 4: Save large batch of candles
	t.Run("Large batch", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := make([]candle.Candle, 100)

		for i := range 100 {
			candles[i] = candle.Candle{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(time.Duration(-i) * time.Minute),
				Open:      10000.0 + float64(i),
				High:      10100.0 + float64(i),
				Low:       9900.0 + float64(i),
				Close:     10050.0 + float64(i),
				Volume:    1.5 + float64(i),
				Source:    "test",
			}
		}

		err := db.SaveCandles(candles)
		assert.NoError(t, err)

		// Verify a sample of candles were saved
		for i := 0; i < 10; i += 10 {
			c := candles[i]
			retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
			assert.NoError(t, err)
			assert.NotNil(t, retrieved)
			assert.Equal(t, c.Open, retrieved.Open)
			assert.Equal(t, c.High, retrieved.High)
			assert.Equal(t, c.Low, retrieved.Low)
			assert.Equal(t, c.Close, retrieved.Close)
			assert.Equal(t, c.Volume, retrieved.Volume)
		}
	})

	// Test 5: Save candles with duplicate timestamps (should update existing)
	t.Run("Duplicate timestamps", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)

		// First candle
		c1 := candle.Candle{
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

		// Second candle with same timestamp but different values
		c2 := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10200.0, // Different high
			Low:       9800.0,  // Different low
			Close:     10150.0, // Different close
			Volume:    2.5,     // Different volume
			Source:    "test",
		}

		// Save first candle
		err := db.SaveCandles([]candle.Candle{c1})
		assert.NoError(t, err)

		// Save second candle (should update the first)
		err = db.SaveCandles([]candle.Candle{c2})
		assert.NoError(t, err)

		// Verify the candle was updated
		retrieved, err := db.GetCandle(c1.Symbol, c1.Timeframe, c1.Timestamp, c1.Source)
		assert.NoError(t, err)
		assert.NotNil(t, retrieved)
		assert.Equal(t, c2.High, retrieved.High) // Should have updated values
		assert.Equal(t, c2.Low, retrieved.Low)
		assert.Equal(t, c2.Close, retrieved.Close)
		assert.Equal(t, c2.Volume, retrieved.Volume)
	})

	// Test 6: Save candles with different timeframes
	t.Run("Different timeframes", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Truncate(5 * time.Minute),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Truncate(time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		assert.NoError(t, err)

		// Verify all candles were saved
		for _, c := range candles {
			retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
			assert.NoError(t, err)
			assert.NotNil(t, retrieved)
			assert.Equal(t, c.Timeframe, retrieved.Timeframe)
		}
	})
}

func TestPostgresDB_SaveConstructedCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Test 1: Save constructed candles
	t.Run("Save constructed candles", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Truncate(5 * time.Minute),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original", // This should be changed to "constructed"
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "15m",
				Timestamp: now.Truncate(15 * time.Minute),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "another", // This should be changed to "constructed"
			},
		}

		err := db.SaveConstructedCandles(candles)
		assert.NoError(t, err)

		// Verify all candles were saved with "constructed" source
		for _, c := range candles {
			retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, "constructed")
			assert.NoError(t, err)
			assert.NotNil(t, retrieved)
			assert.Equal(t, "constructed", retrieved.Source)
			assert.Equal(t, c.Symbol, retrieved.Symbol)
			assert.Equal(t, c.Timeframe, retrieved.Timeframe)
			assert.Equal(t, c.Timestamp.Unix(), retrieved.Timestamp.Unix())
			assert.Equal(t, c.Open, retrieved.Open)
			assert.Equal(t, c.High, retrieved.High)
			assert.Equal(t, c.Low, retrieved.Low)
			assert.Equal(t, c.Close, retrieved.Close)
			assert.Equal(t, c.Volume, retrieved.Volume)
		}
	})

	// Test 2: Save empty constructed candles slice
	t.Run("Empty constructed candles slice", func(t *testing.T) {
		candles := []candle.Candle{}
		err := db.SaveConstructedCandles(candles)
		assert.NoError(t, err) // Should not error with empty slice
	})

	// Test 3: Save constructed candles with invalid data
	t.Run("Invalid constructed candles", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Truncate(5 * time.Minute),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Truncate(5 * time.Minute).Add(time.Minute),
				Open:      0.0, // Invalid: zero price
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original",
			},
		}

		err := db.SaveConstructedCandles(candles)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "candle prices must be positive")
	})

	// Test 4: Save constructed candles and verify they don't interfere with raw candles
	t.Run("Constructed vs raw candles separation", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)

		// Save a raw candle first
		rawCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: now.Truncate(5 * time.Minute),
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "raw",
		}

		err := db.SaveCandle(&rawCandle)
		assert.NoError(t, err)

		// Save a constructed candle with same symbol/timeframe/timestamp
		constructedCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "5m",
			Timestamp: now.Truncate(5 * time.Minute),
			Open:      10000.0,
			High:      10200.0, // Different values
			Low:       9800.0,
			Close:     10150.0,
			Volume:    2.5,
			Source:    "original", // Will be changed to "constructed"
		}

		err = db.SaveConstructedCandles([]candle.Candle{constructedCandle})
		assert.NoError(t, err)

		// Verify both candles exist with different sources
		rawRetrieved, err := db.GetCandle(rawCandle.Symbol, rawCandle.Timeframe, rawCandle.Timestamp, "raw")
		assert.NoError(t, err)
		assert.NotNil(t, rawRetrieved)
		assert.Equal(t, "raw", rawRetrieved.Source)
		assert.Equal(t, rawCandle.High, rawRetrieved.High)

		constructedRetrieved, err := db.GetCandle(constructedCandle.Symbol, constructedCandle.Timeframe, constructedCandle.Timestamp, "constructed")
		assert.NoError(t, err)
		assert.NotNil(t, constructedRetrieved)
		assert.Equal(t, "constructed", constructedRetrieved.Source)
		assert.Equal(t, constructedCandle.High, constructedRetrieved.High)
	})

	// Test 5: Save large batch of constructed candles
	t.Run("Large batch of constructed candles", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := make([]candle.Candle, 50)

		for i := 0; i < 50; i++ {
			candles[i] = candle.Candle{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Truncate(5 * time.Minute).Add(time.Duration(-i*5) * time.Minute),
				Open:      10000.0 + float64(i),
				High:      10100.0 + float64(i),
				Low:       9900.0 + float64(i),
				Close:     10050.0 + float64(i),
				Volume:    1.5 + float64(i),
				Source:    "original", // Will be changed to "constructed"
			}
		}

		err := db.SaveConstructedCandles(candles)
		assert.NoError(t, err)

		// Verify a sample of candles were saved with "constructed" source
		for i := 0; i < 10; i += 10 {
			c := candles[i]
			retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, "constructed")
			assert.NoError(t, err)
			assert.NotNil(t, retrieved)
			assert.Equal(t, "constructed", retrieved.Source)
			assert.Equal(t, c.Open, retrieved.Open)
			assert.Equal(t, c.High, retrieved.High)
			assert.Equal(t, c.Low, retrieved.Low)
			assert.Equal(t, c.Close, retrieved.Close)
			assert.Equal(t, c.Volume, retrieved.Volume)
		}
	})

	// Test 6: Save constructed candles with different timeframes
	t.Run("Constructed candles with different timeframes", func(t *testing.T) {
		now := time.Now().UTC().Truncate(time.Minute)
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Truncate(5 * time.Minute),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "15m",
				Timestamp: now.Truncate(15 * time.Minute),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Truncate(time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "4h",
				Timestamp: now.Truncate(4 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1d",
				Timestamp: now.Truncate(24 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "original",
			},
		}

		err := db.SaveConstructedCandles(candles)
		assert.NoError(t, err)

		// Verify all candles were saved with "constructed" source
		for _, c := range candles {
			retrieved, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, "constructed")
			assert.NoError(t, err)
			assert.NotNil(t, retrieved)
			assert.Equal(t, "constructed", retrieved.Source)
			assert.Equal(t, c.Timeframe, retrieved.Timeframe)
		}
	})
}

func TestPostgresDB_GetCandle(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Insert test candle
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

	err = db.SaveCandle(&testCandle)
	require.NoError(t, err)

	// Test 1: Get existing candle
	t.Run("Get existing candle", func(t *testing.T) {
		c, err := db.GetCandle("BTC-USDT", "1m", now, "test")
		assert.NoError(t, err)
		assert.NotNil(t, c)
		assert.Equal(t, "BTC-USDT", c.Symbol)
		assert.Equal(t, "1m", c.Timeframe)
		assert.Equal(t, now.Unix(), c.Timestamp.Unix())
		assert.Equal(t, 10000.0, c.Open)
		assert.Equal(t, 10100.0, c.High)
		assert.Equal(t, 9900.0, c.Low)
		assert.Equal(t, 10050.0, c.Close)
		assert.Equal(t, 1.5, c.Volume)
		assert.Equal(t, "test", c.Source)
	})

	// Test 2: Get non-existent candle
	t.Run("Get non-existent candle", func(t *testing.T) {
		c, err := db.GetCandle("BTC-USDT", "1m", now.Add(time.Hour), "test")
		assert.NoError(t, err)
		assert.Nil(t, c)
	})

	// Test 3: Get candle with different source
	t.Run("Get candle with wrong source", func(t *testing.T) {
		c, err := db.GetCandle("BTC-USDT", "1m", now, "wrong-source")
		assert.NoError(t, err)
		assert.Nil(t, c)
	})

	// Test 4: Get candle with invalid parameters
	t.Run("Get candle with empty symbol", func(t *testing.T) {
		c, err := db.GetCandle("", "1m", now, "test")
		assert.NoError(t, err)
		assert.Nil(t, c)
	})
}

func TestPostgresDB_GetCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Insert test candles
	candles := []candle.Candle{
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		},
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-time.Minute),
			Open:      9900.0,
			High:      10000.0,
			Low:       9800.0,
			Close:     10000.0,
			Volume:    2.0,
			Source:    "test",
		},
		{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      300.0,
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "test",
		},
	}

	err = db.SaveCandles(candles)
	require.NoError(t, err)

	// Test 1: Get multiple candles for one symbol
	t.Run("Get multiple candles for one symbol", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetCandles("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, result, 2)

		// Check candles are sorted by timestamp
		assert.True(t, result[0].Timestamp.Before(result[1].Timestamp) || result[0].Timestamp.Equal(result[1].Timestamp))
	})

	// Test 2: Get candles with no results
	t.Run("Get candles with no results", func(t *testing.T) {
		start := now.Add(-5 * time.Hour)
		end := now.Add(-1 * time.Hour)

		result, err := db.GetCandles("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	// Test 3: Get candles with invalid time range
	t.Run("Get candles with end before start", func(t *testing.T) {
		start := now.Add(5 * time.Minute)
		end := now.Add(-5 * time.Minute)

		result, err := db.GetCandles("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	// Test 4: Get candles for non-existent timeframe
	t.Run("Get candles for non-existent timeframe", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetCandles("BTC-USDT", "5m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestPostgresDB_GetCandlesV2(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Insert test candles with multiple symbols and same timeframe
	candles := []candle.Candle{
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test",
		},
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-time.Minute),
			Open:      9900.0,
			High:      10000.0,
			Low:       9800.0,
			Close:     10000.0,
			Volume:    2.0,
			Source:    "test",
		},
		{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      300.0,
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "test",
		},
		{
			Symbol:    "LTC-USDT",
			Timeframe: "5m",
			Timestamp: now,
			Open:      70.0,
			High:      72.0,
			Low:       68.0,
			Close:     71.0,
			Volume:    5.0,
			Source:    "test",
		},
	}

	err = db.SaveCandles(candles)
	require.NoError(t, err)

	// Test 1: Get all candles with specific timeframe
	t.Run("Get all 1m candles", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetCandlesV2("1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, result, 3)

		// Check if we got candles for both BTC and ETH
		symbolMap := make(map[string]bool)
		for _, c := range result {
			symbolMap[c.Symbol] = true
		}
		assert.Len(t, symbolMap, 2)
		assert.True(t, symbolMap["BTC-USDT"])
		assert.True(t, symbolMap["ETH-USDT"])
	})

	// Test 2: Get candles for timeframe with no results
	t.Run("Get candles for timeframe with no results", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetCandlesV2("15m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	// Test 3: Get candles with very small time range
	t.Run("Get candles with very small time range", func(t *testing.T) {
		start := now.Add(-1 * time.Second)
		end := now.Add(1 * time.Second)

		result, err := db.GetCandlesV2("1m", start, end)
		assert.NoError(t, err)
		// May return results if now is exactly on a minute boundary
		for _, c := range result {
			assert.Equal(t, "1m", c.Timeframe)
		}
	})

	// Test 4: Get candles with huge time range
	t.Run("Get candles with huge time range", func(t *testing.T) {
		start := now.Add(-365 * 24 * time.Hour) // 1 year ago
		end := now.Add(365 * 24 * time.Hour)    // 1 year in future

		result, err := db.GetCandlesV2("1m", start, end)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, len(result), 3) // At least our 3 candles should be there

		for _, c := range result {
			assert.Equal(t, "1m", c.Timeframe)
		}
	})
}

func TestPostgresDB_GetCandlesInRange(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Insert test candles
	candles := []candle.Candle{
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "test1",
		},
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-time.Minute),
			Open:      9900.0,
			High:      10000.0,
			Low:       9800.0,
			Close:     10000.0,
			Volume:    2.0,
			Source:    "test1",
		},
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-2 * time.Minute),
			Open:      9800.0,
			High:      9900.0,
			Low:       9700.0,
			Close:     9900.0,
			Volume:    1.8,
			Source:    "test2",
		},
	}

	err = db.SaveCandles(candles)
	require.NoError(t, err)

	// Test 1: Get candles with source filter
	t.Run("Get candles with source filter", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetCandlesInRange("BTC-USDT", "1m", start, end, "test1")
		assert.NoError(t, err)
		assert.Len(t, result, 2)

		for _, c := range result {
			assert.Equal(t, "test1", c.Source)
		}
	})

	// Test 2: Get candles without source filter
	t.Run("Get candles without source filter", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetCandlesInRange("BTC-USDT", "1m", start, end, "")
		assert.NoError(t, err)
		assert.Len(t, result, 3)
	})

	// Test 3: Get candles with non-existent source
	t.Run("Get candles with non-existent source", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetCandlesInRange("BTC-USDT", "1m", start, end, "non-existent")
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	// Test 4: Get candles with exact timestamp range
	t.Run("Get candles with exact timestamp range", func(t *testing.T) {
		start := now.Add(-time.Minute)
		end := now.Add(-time.Minute)

		result, err := db.GetCandlesInRange("BTC-USDT", "1m", start, end, "")
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, start.Unix(), result[0].Timestamp.Unix())
	})
}

func TestPostgresDB_GetConstructedCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Insert test candles
	candles := []candle.Candle{
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "constructed",
		},
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-time.Minute),
			Open:      9900.0,
			High:      10000.0,
			Low:       9800.0,
			Close:     10000.0,
			Volume:    2.0,
			Source:    "exchange1",
		},
		{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      300.0,
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "constructed",
		},
	}

	err = db.SaveCandles(candles)
	require.NoError(t, err)

	// Test 1: Get constructed candles for BTC
	t.Run("Get constructed candles for BTC", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetConstructedCandles("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "constructed", result[0].Source)
		assert.Equal(t, "BTC-USDT", result[0].Symbol)
	})

	// Test 2: Get constructed candles for ETH
	t.Run("Get constructed candles for ETH", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetConstructedCandles("ETH-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.Equal(t, "constructed", result[0].Source)
		assert.Equal(t, "ETH-USDT", result[0].Symbol)
	})

	// Test 3: Get constructed candles with no results
	t.Run("Get constructed candles with no results", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetConstructedCandles("LTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	// Test 4: Get constructed candles for wrong timeframe
	t.Run("Get constructed candles for wrong timeframe", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetConstructedCandles("BTC-USDT", "5m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})
}

func TestPostgresDB_GetRawCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Insert test candles
	candles := []candle.Candle{
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    "constructed",
		},
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-time.Minute),
			Open:      9900.0,
			High:      10000.0,
			Low:       9800.0,
			Close:     10000.0,
			Volume:    2.0,
			Source:    "exchange1",
		},
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-2 * time.Minute),
			Open:      9800.0,
			High:      9900.0,
			Low:       9700.0,
			Close:     9900.0,
			Volume:    1.8,
			Source:    "exchange2",
		},
		{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      300.0,
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "exchange1",
		},
	}

	err = db.SaveCandles(candles)
	require.NoError(t, err)

	// Test 1: Get raw candles for BTC
	t.Run("Get raw candles for BTC", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetRawCandles("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, result, 2)

		for _, c := range result {
			assert.NotEqual(t, "constructed", c.Source)
			assert.Equal(t, "BTC-USDT", c.Symbol)
		}
	})

	// Test 2: Get raw candles for ETH
	t.Run("Get raw candles for ETH", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetRawCandles("ETH-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, result, 1)
		assert.NotEqual(t, "constructed", result[0].Source)
		assert.Equal(t, "ETH-USDT", result[0].Symbol)
	})

	// Test 3: Get raw candles with no results
	t.Run("Get raw candles with no results", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetRawCandles("LTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	// Test 4: Get raw candles for wrong timeframe
	t.Run("Get raw candles for wrong timeframe", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		result, err := db.GetRawCandles("BTC-USDT", "5m", start, end)
		assert.NoError(t, err)
		assert.Empty(t, result)
	})

	// Test 5: Verify raw candles exclude constructed ones
	t.Run("Verify raw candles exclude constructed ones", func(t *testing.T) {
		start := now.Add(-5 * time.Minute)
		end := now.Add(5 * time.Minute)

		// Get all candles
		allCandles, err := db.GetCandles("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, allCandles, 3) // Should include constructed ones

		// Get raw candles
		rawCandles, err := db.GetRawCandles("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Len(t, rawCandles, 2) // Should exclude constructed ones

		// Verify constructed candles are excluded
		for _, c := range rawCandles {
			assert.NotEqual(t, "constructed", c.Source)
		}
	})
}

func TestPostgresDB_GetLatestCandle(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Create test candles with different timestamps
	now := time.Now().UTC().Truncate(time.Minute)
	symbol := "BTC-USDT"
	timeframe := "1m"
	source := "test"

	candles := []candle.Candle{
		{
			Symbol:    symbol,
			Timeframe: timeframe,
			Timestamp: now.Add(-2 * time.Minute),
			Open:      10000.0,
			High:      10100.0,
			Low:       9900.0,
			Close:     10050.0,
			Volume:    1.5,
			Source:    source,
		},
		{
			Symbol:    symbol,
			Timeframe: timeframe,
			Timestamp: now, // Latest
			Open:      10100.0,
			High:      10200.0,
			Low:       10000.0,
			Close:     10150.0,
			Volume:    2.0,
			Source:    source,
		},
		{
			Symbol:    symbol,
			Timeframe: timeframe,
			Timestamp: now.Add(-1 * time.Minute),
			Open:      10050.0,
			High:      10150.0,
			Low:       9950.0,
			Close:     10100.0,
			Volume:    1.8,
			Source:    source,
		},
	}

	// Save candles
	err = db.SaveCandles(candles)
	assert.NoError(t, err)

	// Test getting latest candle
	latest, err := db.GetLatestCandle(symbol, timeframe)
	assert.NoError(t, err)
	assert.NotNil(t, latest)
	assert.Equal(t, now.Unix(), latest.Timestamp.Unix())
	assert.Equal(t, 10100.0, latest.Open)
	assert.Equal(t, 10200.0, latest.High)
	assert.Equal(t, 10000.0, latest.Low)
	assert.Equal(t, 10150.0, latest.Close)
}

func TestPostgresDB_GetLatestCandleInRange(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Create test candles
	now := time.Now().UTC().Truncate(time.Minute)
	symbol := "BTC-USDT"
	timeframe := "1m"
	source := "test"

	candles := []candle.Candle{}
	for i := 0; i < 10; i++ {
		candles = append(candles, candle.Candle{
			Symbol:    symbol,
			Timeframe: timeframe,
			Timestamp: now.Add(time.Duration(-i) * time.Minute),
			Open:      10000.0 + float64(i),
			High:      10100.0 + float64(i),
			Low:       9900.0 + float64(i),
			Close:     10050.0 + float64(i),
			Volume:    1.5 + float64(i),
			Source:    source,
		})
	}

	// Save candles
	err = db.SaveCandles(candles)
	assert.NoError(t, err)

	// Test getting latest candle in range
	start := now.Add(-8 * time.Minute)
	end := now.Add(-3 * time.Minute)

	latest, err := db.GetLatestCandleInRange(symbol, timeframe, start, end)
	assert.NoError(t, err)
	assert.NotNil(t, latest)
	assert.Equal(t, now.Add(-3*time.Minute).Unix(), latest.Timestamp.Unix())
}

func TestPostgresDB_UpdateCandle(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Create and save a test candle
	now := time.Now().UTC().Truncate(time.Minute)
	c := candle.Candle{
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

	err = db.SaveCandle(&c)
	assert.NoError(t, err)

	// Update the candle
	c.High = 10200.0
	c.Low = 9800.0
	c.Close = 10150.0
	c.Volume = 2.5

	err = db.UpdateCandle(c)
	assert.NoError(t, err)

	// Retrieve and verify the updated candle
	updated, err := db.GetCandle(c.Symbol, c.Timeframe, c.Timestamp, c.Source)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
	assert.Equal(t, 10000.0, updated.Open)  // Open should remain the same
	assert.Equal(t, 10200.0, updated.High)  // Updated value
	assert.Equal(t, 9800.0, updated.Low)    // Updated value
	assert.Equal(t, 10150.0, updated.Close) // Updated value
	assert.Equal(t, 2.5, updated.Volume)    // Updated value
}

func TestPostgresDB_DeleteCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Create test candles
	now := time.Now().UTC().Truncate(time.Minute)
	symbol := "BTC-USDT"
	timeframe := "1m"
	source := "test"

	candles := []candle.Candle{}
	for i := 0; i < 10; i++ {
		candles = append(candles, candle.Candle{
			Symbol:    symbol,
			Timeframe: timeframe,
			Timestamp: now.Add(time.Duration(-i) * time.Minute),
			Open:      10000.0 + float64(i),
			High:      10100.0 + float64(i),
			Low:       9900.0 + float64(i),
			Close:     10050.0 + float64(i),
			Volume:    1.5 + float64(i),
			Source:    source,
		})
	}

	// Save candles
	err = db.SaveCandles(candles)
	assert.NoError(t, err)

	// Test deleting candles in range
	start := now.Add(-8 * time.Minute)
	end := now.Add(-3 * time.Minute)

	err = db.DeleteCandlesInRange(symbol, timeframe, start, end, source)
	assert.NoError(t, err)

	// Verify candles were deleted
	remainingCandles, err := db.GetCandlesInRange(symbol, timeframe, start, end, source)
	assert.NoError(t, err)
	assert.Len(t, remainingCandles, 0)

	// Verify candles outside range still exist
	beforeRange, err := db.GetCandlesInRange(symbol, timeframe, now.Add(-10*time.Minute), start.Add(-time.Second), source)
	assert.NoError(t, err)
	assert.Len(t, beforeRange, 1) // Should have candle at minute 9

	afterRange, err := db.GetCandlesInRange(symbol, timeframe, end.Add(time.Second), now, source)
	assert.NoError(t, err)
	assert.Len(t, afterRange, 3) // Should have candles at minutes 0, 1, 2
}
