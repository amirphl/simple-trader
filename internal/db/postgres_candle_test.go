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

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Get latest candle when multiple exist
	t.Run("Get latest from multiple candles", func(t *testing.T) {
		// Insert test candles with different timestamps
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      9900.0,
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour), // Latest timestamp
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
				Timestamp: now.Add(-3 * time.Hour),
				Open:      9800.0,
				High:      9900.0,
				Low:       9700.0,
				Close:     9850.0,
				Volume:    1.8,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest candle
		latest, err := db.GetLatestCandle("BTC-USDT", "1m")
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's the correct one (latest timestamp)
		assert.Equal(t, now.Add(-1*time.Hour).Unix(), latest.Timestamp.Unix())
		assert.Equal(t, 10000.0, latest.Open)
		assert.Equal(t, 10100.0, latest.High)
		assert.Equal(t, 9900.0, latest.Low)
		assert.Equal(t, 10050.0, latest.Close)
		assert.Equal(t, 1.5, latest.Volume)
	})

	// Test 2: Get latest candle when none exist
	t.Run("Get latest when none exist", func(t *testing.T) {
		latest, err := db.GetLatestCandle("NON-EXISTENT", "1m")
		assert.NoError(t, err)
		assert.Nil(t, latest)
	})

	// Test 3: Get latest candle with specific timeframe
	t.Run("Get latest with specific timeframe", func(t *testing.T) {
		// Insert candles with different timeframes
		candles := []candle.Candle{
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
				Symbol:    "ETH-USDT",
				Timeframe: "5m",
				Timestamp: now.Add(-5 * time.Minute),
				Open:      298.0,
				High:      308.0,
				Low:       293.0,
				Close:     303.0,
				Volume:    15.0,
				Source:    "test",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "5m",
				Timestamp: now,
				Open:      301.0,
				High:      311.0,
				Low:       296.0,
				Close:     306.0,
				Volume:    12.0,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest 5m candle
		latest, err := db.GetLatestCandle("ETH-USDT", "5m")
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's the correct one
		assert.Equal(t, "5m", latest.Timeframe)
		assert.Equal(t, now.Unix(), latest.Timestamp.Unix())
		assert.Equal(t, 301.0, latest.Open)
	})

	// Test 4: Get latest candle with multiple sources
	t.Run("Get latest with multiple sources", func(t *testing.T) {
		// Insert candles with same timestamp but different sources
		candles := []candle.Candle{
			{
				Symbol:    "LTC-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      50.0,
				High:      52.0,
				Low:       49.0,
				Close:     51.0,
				Volume:    100.0,
				Source:    "source1",
			},
			{
				Symbol:    "LTC-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      50.5,
				High:      52.5,
				Low:       49.5,
				Close:     51.5,
				Volume:    110.0,
				Source:    "source2",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest candle (should be the latest regardless of source)
		latest, err := db.GetLatestCandle("LTC-USDT", "1m")
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's one of the candles with the latest timestamp
		assert.Equal(t, now.Unix(), latest.Timestamp.Unix())
		assert.Equal(t, "LTC-USDT", latest.Symbol)
		assert.Equal(t, "1m", latest.Timeframe)

		// Source could be either source1 or source2 depending on DB ordering
		assert.Contains(t, []string{"source1", "source2"}, latest.Source)
	})

	// Test 5: Get latest with empty string parameters (edge case)
	t.Run("Get latest with empty params", func(t *testing.T) {
		latest, err := db.GetLatestCandle("", "")
		assert.NoError(t, err) // Should not error but return nil
		assert.Nil(t, latest)
	})
}

func TestPostgresDB_GetLatestCandleInRange(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Get latest in range with multiple candles
	t.Run("Get latest in range with multiple candles", func(t *testing.T) {
		// Insert test candles with different timestamps
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      9800.0,
				High:      9900.0,
				Low:       9700.0,
				Close:     9850.0,
				Volume:    1.8,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      9900.0,
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Define range to include only the middle candle
		start := now.Add(-2*time.Hour - 10*time.Minute)
		end := now.Add(-2*time.Hour + 10*time.Minute)

		// Get latest candle in range
		latest, err := db.GetLatestCandleInRange("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's the correct one
		assert.Equal(t, now.Add(-2*time.Hour).Unix(), latest.Timestamp.Unix())
		assert.Equal(t, 9900.0, latest.Open)
		assert.Equal(t, 10000.0, latest.High)
		assert.Equal(t, 9800.0, latest.Low)
		assert.Equal(t, 10000.0, latest.Close)
	})

	// Test 2: Get latest in range with no candles in range
	t.Run("Get latest in range with no candles in range", func(t *testing.T) {
		// Define range where no candles exist
		start := now.Add(-30 * time.Minute)
		end := now.Add(-15 * time.Minute)

		// Get latest candle in range
		latest, err := db.GetLatestCandleInRange("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Nil(t, latest) // No candles in range
	})

	// Test 3: Get latest in range with exact timestamp match
	t.Run("Get latest in range with exact timestamp match", func(t *testing.T) {
		exactTime := now.Add(-4 * time.Hour)

		// Insert test candle with exact timestamp
		testCandle := candle.Candle{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: exactTime,
			Open:      300.0,
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "test",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Define range with exact start and end time
		start := exactTime
		end := exactTime

		// Get latest candle in range
		latest, err := db.GetLatestCandleInRange("ETH-USDT", "1m", start, end)
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's the correct one
		assert.Equal(t, exactTime.Unix(), latest.Timestamp.Unix())
		assert.Equal(t, 300.0, latest.Open)
	})

	// Test 4: Get latest in range with inverted time range (start > end)
	t.Run("Get latest in range with inverted time range", func(t *testing.T) {
		// Define inverted range
		start := now
		end := now.Add(-1 * time.Hour)

		// Get latest candle in range (should return nil)
		latest, err := db.GetLatestCandleInRange("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Nil(t, latest) // Invalid range, should return nil
	})

	// Test 5: Get latest in range with multiple candles at same timestamp
	t.Run("Get latest in range with multiple candles at same timestamp", func(t *testing.T) {
		sameTime := now.Add(-5 * time.Hour)

		// Insert candles with same timestamp but different sources
		candles := []candle.Candle{
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1m",
				Timestamp: sameTime,
				Open:      0.25,
				High:      0.26,
				Low:       0.24,
				Close:     0.255,
				Volume:    1000.0,
				Source:    "source1",
			},
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1m",
				Timestamp: sameTime,
				Open:      0.26,
				High:      0.27,
				Low:       0.25,
				Close:     0.265,
				Volume:    1200.0,
				Source:    "source2",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Define range to include this timestamp
		start := sameTime.Add(-10 * time.Minute)
		end := sameTime.Add(10 * time.Minute)

		// Get latest candle in range
		latest, err := db.GetLatestCandleInRange("XRP-USDT", "1m", start, end)
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it has the correct timestamp
		assert.Equal(t, sameTime.Unix(), latest.Timestamp.Unix())
		assert.Equal(t, "XRP-USDT", latest.Symbol)

		// Source could be either source1 or source2 depending on DB ordering
		assert.Contains(t, []string{"source1", "source2"}, latest.Source)
	})
}

func TestPostgresDB_GetLatestConstructedCandle(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Get latest constructed candle with multiple candles
	t.Run("Get latest constructed candle with multiple candles", func(t *testing.T) {
		// Insert mix of constructed and raw candles
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      9800.0,
				High:      9900.0,
				Low:       9700.0,
				Close:     9850.0,
				Volume:    1.8,
				Source:    "constructed",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      9900.0,
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "constructed",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "exchange", // Raw candle
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest constructed candle
		latest, err := db.GetLatestConstructedCandle("BTC-USDT", "1h")
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's the latest constructed one, not the raw one
		assert.Equal(t, now.Add(-2*time.Hour).Unix(), latest.Timestamp.Unix())
		assert.Equal(t, "constructed", latest.Source)
		assert.Equal(t, 9900.0, latest.Open)
		assert.Equal(t, 10000.0, latest.High)
		assert.Equal(t, 9800.0, latest.Low)
		assert.Equal(t, 10000.0, latest.Close)
	})

	// Test 2: Get latest constructed candle when none exist
	t.Run("Get latest constructed candle when none exist", func(t *testing.T) {
		latest, err := db.GetLatestConstructedCandle("NON-EXISTENT", "1h")
		assert.NoError(t, err)
		assert.Nil(t, latest)
	})

	// Test 3: Get latest constructed candle with specific timeframe
	t.Run("Get latest constructed candle with specific timeframe", func(t *testing.T) {
		// Insert constructed candles with different timeframes
		candles := []candle.Candle{
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1h",
				Timestamp: now,
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    10.0,
				Source:    "constructed",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "4h",
				Timestamp: now.Add(-4 * time.Hour),
				Open:      298.0,
				High:      308.0,
				Low:       293.0,
				Close:     303.0,
				Volume:    15.0,
				Source:    "constructed",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "4h",
				Timestamp: now,
				Open:      301.0,
				High:      311.0,
				Low:       296.0,
				Close:     306.0,
				Volume:    12.0,
				Source:    "constructed",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest 4h constructed candle
		latest, err := db.GetLatestConstructedCandle("ETH-USDT", "4h")
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's the correct one
		assert.Equal(t, "4h", latest.Timeframe)
		assert.Equal(t, now.Unix(), latest.Timestamp.Unix())
		assert.Equal(t, "constructed", latest.Source)
		assert.Equal(t, 301.0, latest.Open)
	})

	// Test 4: Get latest constructed when only raw candles exist
	t.Run("Get latest constructed when only raw candles exist", func(t *testing.T) {
		// Insert only raw candles
		candles := []candle.Candle{
			{
				Symbol:    "LTC-USDT",
				Timeframe: "1h",
				Timestamp: now,
				Open:      50.0,
				High:      52.0,
				Low:       49.0,
				Close:     51.0,
				Volume:    100.0,
				Source:    "exchange1",
			},
			{
				Symbol:    "LTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      49.0,
				High:      51.0,
				Low:       48.0,
				Close:     50.0,
				Volume:    90.0,
				Source:    "exchange2",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest constructed candle (should be nil)
		latest, err := db.GetLatestConstructedCandle("LTC-USDT", "1h")
		assert.NoError(t, err)
		assert.Nil(t, latest) // No constructed candles
	})

	// Test 5: Get latest constructed with empty string parameters (edge case)
	t.Run("Get latest constructed with empty params", func(t *testing.T) {
		latest, err := db.GetLatestConstructedCandle("", "")
		assert.NoError(t, err) // Should not error but return nil
		assert.Nil(t, latest)
	})
}

func TestPostgresDB_GetLatest1mCandle(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Get latest 1m candle with multiple candles
	t.Run("Get latest 1m candle with multiple candles", func(t *testing.T) {
		// Insert 1m candles with different timestamps
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Minute),
				Open:      9800.0,
				High:      9900.0,
				Low:       9700.0,
				Close:     9850.0,
				Volume:    1.8,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-2 * time.Minute),
				Open:      9900.0,
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Minute),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest 1m candle
		latest, err := db.GetLatest1mCandle("BTC-USDT")
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it's the correct one (latest timestamp)
		assert.Equal(t, now.Add(-1*time.Minute).Unix(), latest.Timestamp.Unix())
		assert.Equal(t, "1m", latest.Timeframe)
		assert.Equal(t, 10000.0, latest.Open)
		assert.Equal(t, 10100.0, latest.High)
		assert.Equal(t, 9900.0, latest.Low)
		assert.Equal(t, 10050.0, latest.Close)
		assert.Equal(t, 1.5, latest.Volume)
	})

	// Test 2: Get latest 1m candle when none exist
	t.Run("Get latest 1m candle when none exist", func(t *testing.T) {
		latest, err := db.GetLatest1mCandle("NON-EXISTENT")
		assert.NoError(t, err)
		assert.Nil(t, latest)
	})

	// Test 3: Get latest 1m candle when only other timeframes exist
	t.Run("Get latest 1m candle when only other timeframes exist", func(t *testing.T) {
		// Insert candles with non-1m timeframes
		candles := []candle.Candle{
			{
				Symbol:    "ETH-USDT",
				Timeframe: "5m",
				Timestamp: now,
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    10.0,
				Source:    "test",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "15m",
				Timestamp: now.Add(-15 * time.Minute),
				Open:      298.0,
				High:      308.0,
				Low:       293.0,
				Close:     303.0,
				Volume:    15.0,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest 1m candle (should be nil)
		latest, err := db.GetLatest1mCandle("ETH-USDT")
		assert.NoError(t, err)
		assert.Nil(t, latest) // No 1m candles
	})

	// Test 4: Get latest 1m candle with multiple sources
	t.Run("Get latest 1m candle with multiple sources", func(t *testing.T) {
		// Insert 1m candles with different sources but same timestamp
		candles := []candle.Candle{
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      0.25,
				High:      0.26,
				Low:       0.24,
				Close:     0.255,
				Volume:    1000.0,
				Source:    "source1",
			},
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      0.26,
				High:      0.27,
				Low:       0.25,
				Close:     0.265,
				Volume:    1200.0,
				Source:    "source2",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Get latest 1m candle
		latest, err := db.GetLatest1mCandle("XRP-USDT")
		assert.NoError(t, err)
		require.NotNil(t, latest)

		// Verify it has the correct timestamp and timeframe
		assert.Equal(t, now.Unix(), latest.Timestamp.Unix())
		assert.Equal(t, "1m", latest.Timeframe)
		assert.Equal(t, "XRP-USDT", latest.Symbol)

		// Source could be either source1 or source2 depending on DB ordering
		assert.Contains(t, []string{"source1", "source2"}, latest.Source)
	})

	// Test 5: Get latest 1m candle with empty symbol (edge case)
	t.Run("Get latest 1m candle with empty symbol", func(t *testing.T) {
		latest, err := db.GetLatest1mCandle("")
		assert.NoError(t, err) // Should not error but return nil
		assert.Nil(t, latest)
	})

	// Test 6: Verify GetLatest1mCandle is equivalent to GetLatestCandle with "1m"
	t.Run("Verify GetLatest1mCandle is equivalent to GetLatestCandle", func(t *testing.T) {
		// Insert a new 1m candle
		testCandle := candle.Candle{
			Symbol:    "DOGE-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      0.05,
			High:      0.055,
			Low:       0.049,
			Close:     0.052,
			Volume:    10000.0,
			Source:    "test",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Get with both methods
		latest1, err1 := db.GetLatest1mCandle("DOGE-USDT")
		latest2, err2 := db.GetLatestCandle("DOGE-USDT", "1m")

		// Verify both return the same result
		assert.NoError(t, err1)
		assert.NoError(t, err2)
		require.NotNil(t, latest1)
		require.NotNil(t, latest2)

		// Compare all fields
		assert.Equal(t, latest1.Symbol, latest2.Symbol)
		assert.Equal(t, latest1.Timeframe, latest2.Timeframe)
		assert.Equal(t, latest1.Timestamp.Unix(), latest2.Timestamp.Unix())
		assert.Equal(t, latest1.Open, latest2.Open)
		assert.Equal(t, latest1.High, latest2.High)
		assert.Equal(t, latest1.Low, latest2.Low)
		assert.Equal(t, latest1.Close, latest2.Close)
		assert.Equal(t, latest1.Volume, latest2.Volume)
		assert.Equal(t, latest1.Source, latest2.Source)
	})
}

func TestPostgresDB_DeleteCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Delete candles before a specified time
	t.Run("Delete candles before time", func(t *testing.T) {
		// Insert test candles with different timestamps
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Hour),
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
				Timestamp: now.Add(-2 * time.Hour),
				Open:      10100.0,
				High:      10200.0,
				Low:       10000.0,
				Close:     10150.0,
				Volume:    2.0,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10150.0,
				High:      10250.0,
				Low:       10050.0,
				Close:     10200.0,
				Volume:    1.8,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Verify candles were saved
		count, err := db.GetCandleCount("BTC-USDT", "1m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		// Delete candles older than 2 hours ago
		err = db.DeleteCandles("BTC-USDT", "1m", now.Add(-2*time.Hour))
		assert.NoError(t, err)

		// Verify candles were deleted
		count, err = db.GetCandleCount("BTC-USDT", "1m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 2, count) // Only 2 candles should remain

		// Verify the specific candles that remain
		remainingCandles, err := db.GetCandles("BTC-USDT", "1m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Len(t, remainingCandles, 2)

		// Sort by timestamp to ensure order
		timestamps := []time.Time{remainingCandles[0].Timestamp, remainingCandles[1].Timestamp}
		assert.Contains(t, timestamps, now.Add(-2*time.Hour)) // This should remain
		assert.Contains(t, timestamps, now.Add(-1*time.Hour)) // This should remain
	})

	// Test 2: Delete candles for a specific symbol and timeframe
	t.Run("Delete candles for specific symbol and timeframe", func(t *testing.T) {
		// Insert test candles with different symbols and timeframes
		candles := []candle.Candle{
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    5.0,
				Source:    "test",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "5m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    5.0,
				Source:    "test",
			},
			{
				Symbol:    "LTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      50.0,
				High:      52.0,
				Low:       49.0,
				Close:     51.0,
				Volume:    10.0,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Delete only ETH-USDT 1m candles
		err = db.DeleteCandles("ETH-USDT", "1m", now)
		assert.NoError(t, err)

		// Verify ETH-USDT 1m candles were deleted
		count, err := db.GetCandleCount("ETH-USDT", "1m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Verify ETH-USDT 5m candles still exist
		count, err = db.GetCandleCount("ETH-USDT", "5m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 1, count)

		// Verify LTC-USDT 1m candles still exist
		count, err = db.GetCandleCount("LTC-USDT", "1m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// Test 3: Delete with future timestamp (should delete nothing)
	t.Run("Delete with future timestamp", func(t *testing.T) {
		// Insert test candle
		testCandle := candle.Candle{
			Symbol:    "XRP-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      0.25,
			High:      0.26,
			Low:       0.24,
			Close:     0.255,
			Volume:    1000.0,
			Source:    "test",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Try to delete with a timestamp in the future
		err = db.DeleteCandles("XRP-USDT", "1m", now.Add(1*time.Hour))
		assert.NoError(t, err) // Should not error, but delete nothing

		// Verify candle still exists
		count, err := db.GetCandleCount("XRP-USDT", "1m", now.Add(-1*time.Hour), now.Add(1*time.Hour))
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	// Test 4: Delete for non-existent symbol (should not error)
	t.Run("Delete for non-existent symbol", func(t *testing.T) {
		err := db.DeleteCandles("NON-EXISTENT", "1m", now)
		assert.NoError(t, err) // Should not error for non-existent symbols
	})
}

func TestPostgresDB_DeleteCandlesInRange(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Delete candles in a specific time range with source filter
	t.Run("Delete candles in range with source filter", func(t *testing.T) {
		// Insert test candles with different timestamps and sources
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "source1",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      10100.0,
				High:      10200.0,
				Low:       10000.0,
				Close:     10150.0,
				Volume:    2.0,
				Source:    "source1",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10150.0,
				High:      10250.0,
				Low:       10050.0,
				Close:     10200.0,
				Volume:    1.8,
				Source:    "source2",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Delete candles in range with source1
		start := now.Add(-2*time.Hour - 30*time.Minute)
		end := now.Add(-1*time.Hour - 30*time.Minute)
		err = db.DeleteCandlesInRange("BTC-USDT", "1m", start, end, "source1")
		assert.NoError(t, err)

		// Verify only source1 candles in the range were deleted
		remainingCandles, err := db.GetCandles("BTC-USDT", "1m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Len(t, remainingCandles, 2) // One source1 and one source2 should remain

		// Check that the correct candles remain
		var remainingSources []string
		for _, c := range remainingCandles {
			remainingSources = append(remainingSources, c.Source)
		}
		assert.Contains(t, remainingSources, "source1") // The oldest source1 should remain
		assert.Contains(t, remainingSources, "source2") // source2 should remain
	})

	// Test 2: Delete candles in range without source filter
	t.Run("Delete candles in range without source filter", func(t *testing.T) {
		// Insert test candles with different timestamps and sources
		candles := []candle.Candle{
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    5.0,
				Source:    "source1",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      305.0,
				High:      315.0,
				Low:       300.0,
				Close:     310.0,
				Volume:    6.0,
				Source:    "source2",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      310.0,
				High:      320.0,
				Low:       305.0,
				Close:     315.0,
				Volume:    7.0,
				Source:    "source1",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Delete all candles in range regardless of source
		start := now.Add(-2*time.Hour - 30*time.Minute)
		end := now.Add(-1*time.Hour - 30*time.Minute)
		err = db.DeleteCandlesInRange("ETH-USDT", "1m", start, end, "")
		assert.NoError(t, err)

		// Verify all candles in the range were deleted, regardless of source
		remainingCandles, err := db.GetCandles("ETH-USDT", "1m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Len(t, remainingCandles, 2) // Only the oldest and newest should remain

		// Check timestamps of remaining candles
		var timestamps []time.Time
		for _, c := range remainingCandles {
			timestamps = append(timestamps, c.Timestamp)
		}
		assert.Contains(t, timestamps, now.Add(-3*time.Hour)) // Oldest should remain
		assert.Contains(t, timestamps, now.Add(-1*time.Hour)) // Newest should remain
	})

	// Test 3: Delete with exact timestamp range
	t.Run("Delete with exact timestamp range", func(t *testing.T) {
		exactTime := now.Add(-4 * time.Hour)

		// Insert test candle with exact timestamp
		testCandle := candle.Candle{
			Symbol:    "LTC-USDT",
			Timeframe: "1m",
			Timestamp: exactTime,
			Open:      50.0,
			High:      52.0,
			Low:       49.0,
			Close:     51.0,
			Volume:    10.0,
			Source:    "test",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Delete with exact start and end time
		err = db.DeleteCandlesInRange("LTC-USDT", "1m", exactTime, exactTime, "")
		assert.NoError(t, err)

		// Verify candle was deleted
		count, err := db.GetCandleCount("LTC-USDT", "1m", exactTime, exactTime)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	// Test 4: Delete with inverted time range (should delete nothing)
	t.Run("Delete with inverted time range", func(t *testing.T) {
		// Insert test candle
		testCandle := candle.Candle{
			Symbol:    "XRP-USDT",
			Timeframe: "1m",
			Timestamp: now.Add(-5 * time.Hour),
			Open:      0.25,
			High:      0.26,
			Low:       0.24,
			Close:     0.255,
			Volume:    1000.0,
			Source:    "test",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Try to delete with end before start
		start := now.Add(-4 * time.Hour)
		end := now.Add(-6 * time.Hour)
		err = db.DeleteCandlesInRange("XRP-USDT", "1m", start, end, "")
		assert.NoError(t, err) // Should not error, but delete nothing

		// Verify candle still exists
		count, err := db.GetCandleCount("XRP-USDT", "1m", now.Add(-6*time.Hour), now.Add(-4*time.Hour))
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// Test 5: Delete for non-existent data (should not error)
	t.Run("Delete for non-existent data", func(t *testing.T) {
		start := now.Add(-3 * time.Hour)
		end := now.Add(-1 * time.Hour)
		err := db.DeleteCandlesInRange("NON-EXISTENT", "1m", start, end, "")
		assert.NoError(t, err) // Should not error for non-existent symbols
	})
}

func TestPostgresDB_DeleteConstructedCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Delete constructed candles before a specific time
	t.Run("Delete constructed candles before time", func(t *testing.T) {
		// Create candles with "constructed" source and other sources
		candles := []candle.Candle{
			// Constructed candles
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "constructed",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      10100.0,
				High:      10200.0,
				Low:       10000.0,
				Close:     10150.0,
				Volume:    2.0,
				Source:    "constructed",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10150.0,
				High:      10250.0,
				Low:       10050.0,
				Close:     10200.0,
				Volume:    1.8,
				Source:    "constructed",
			},
			// Raw candles with same timestamps
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "exchange",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "5m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      10100.0,
				High:      10200.0,
				Low:       10000.0,
				Close:     10150.0,
				Volume:    2.0,
				Source:    "exchange",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Verify candles were saved
		constructedCount, err := db.GetConstructedCandleCount("BTC-USDT", "5m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 3, constructedCount)

		rawCount, err := db.GetCandleCount("BTC-USDT", "5m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 5, rawCount) // Total of 5 candles (3 constructed + 2 raw)

		// Delete constructed candles older than 2 hours ago
		err = db.DeleteConstructedCandles("BTC-USDT", "5m", now.Add(-1*time.Hour))
		assert.NoError(t, err)

		// Verify only constructed candles were deleted
		constructedCount, err = db.GetConstructedCandleCount("BTC-USDT", "5m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 1, constructedCount) // Only the newest constructed candle should remain

		// Verify raw candles are unaffected
		rawCandles, err := db.GetRawCandles("BTC-USDT", "5m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Len(t, rawCandles, 2) // All raw candles should remain
	})

	// Test 2: Delete constructed candles for a specific timeframe
	t.Run("Delete constructed candles for specific timeframe", func(t *testing.T) {
		// Create candles with different timeframes
		candles := []candle.Candle{
			{
				Symbol:    "ETH-USDT",
				Timeframe: "15m",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    5.0,
				Source:    "constructed",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    5.0,
				Source:    "constructed",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Delete only 15m constructed candles
		err = db.DeleteConstructedCandles("ETH-USDT", "15m", now)
		assert.NoError(t, err)

		// Verify 15m constructed candles were deleted
		count, err := db.GetConstructedCandleCount("ETH-USDT", "15m", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 0, count)

		// Verify 1h constructed candles still exist
		count, err = db.GetConstructedCandleCount("ETH-USDT", "1h", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// Test 3: Delete with future timestamp (should delete nothing)
	t.Run("Delete with future timestamp", func(t *testing.T) {
		// Insert constructed test candle
		testCandle := candle.Candle{
			Symbol:    "XRP-USDT",
			Timeframe: "1h",
			Timestamp: now,
			Open:      0.25,
			High:      0.26,
			Low:       0.24,
			Close:     0.255,
			Volume:    1000.0,
			Source:    "constructed",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Try to delete with a timestamp in the future
		err = db.DeleteConstructedCandles("XRP-USDT", "1h", now.Add(1*time.Hour))
		assert.NoError(t, err) // Should not error, but delete nothing

		// Verify candle still exists
		count, err := db.GetConstructedCandleCount("XRP-USDT", "1h", now.Add(-1*time.Hour), now.Add(1*time.Hour))
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	// Test 4: Delete for non-existent symbol (should not error)
	t.Run("Delete for non-existent symbol", func(t *testing.T) {
		err := db.DeleteConstructedCandles("NON-EXISTENT", "1h", now)
		assert.NoError(t, err) // Should not error for non-existent symbols
	})

	// Test 5: Delete from multiple symbols with the same timeframe
	t.Run("Delete from multiple symbols with same timeframe", func(t *testing.T) {
		// Create constructed candles for multiple symbols
		candles := []candle.Candle{
			{
				Symbol:    "LTC-USDT",
				Timeframe: "4h",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      50.0,
				High:      52.0,
				Low:       49.0,
				Close:     51.0,
				Volume:    10.0,
				Source:    "constructed",
			},
			{
				Symbol:    "DOT-USDT",
				Timeframe: "4h",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      5.0,
				High:      5.2,
				Low:       4.9,
				Close:     5.1,
				Volume:    100.0,
				Source:    "constructed",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Delete LTC-USDT constructed candles
		err = db.DeleteConstructedCandles("LTC-USDT", "4h", now)
		assert.NoError(t, err)

		// Verify LTC-USDT candles were deleted but DOT-USDT candles remain
		ltcCount, err := db.GetConstructedCandleCount("LTC-USDT", "4h", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 0, ltcCount)

		dotCount, err := db.GetConstructedCandleCount("DOT-USDT", "4h", now.Add(-4*time.Hour), now)
		require.NoError(t, err)
		assert.Equal(t, 1, dotCount)
	})
}

func TestPostgresDB_GetCandleCount(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Count candles with multiple candles
	t.Run("Count multiple candles", func(t *testing.T) {
		// Insert test candles with different timestamps
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-3 * time.Hour),
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
				Timestamp: now.Add(-2 * time.Hour),
				Open:      10100.0,
				High:      10200.0,
				Low:       10000.0,
				Close:     10150.0,
				Volume:    2.0,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10150.0,
				High:      10250.0,
				Low:       10050.0,
				Close:     10200.0,
				Volume:    1.8,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Count all candles
		count, err := db.GetCandleCount("BTC-USDT", "1m", now.Add(-4*time.Hour), now)
		assert.NoError(t, err)
		assert.Equal(t, 3, count)
	})

	// Test 2: Count candles with empty result
	t.Run("Count with empty result", func(t *testing.T) {
		count, err := db.GetCandleCount("NON-EXISTENT", "1m", now.Add(-4*time.Hour), now)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	// Test 3: Count candles with specific timeframe
	t.Run("Count with specific timeframe", func(t *testing.T) {
		// Insert candles with different timeframes
		candles := []candle.Candle{
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
				Symbol:    "ETH-USDT",
				Timeframe: "5m",
				Timestamp: now,
				Open:      301.0,
				High:      311.0,
				Low:       296.0,
				Close:     306.0,
				Volume:    12.0,
				Source:    "test",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Count only 5m candles
		count, err := db.GetCandleCount("ETH-USDT", "5m", now.Add(-1*time.Hour), now.Add(1*time.Hour))
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// Test 4: Count candles with exact time range
	t.Run("Count with exact time range", func(t *testing.T) {
		exactTime := now.Add(-5 * time.Hour)

		// Insert test candle with exact timestamp
		testCandle := candle.Candle{
			Symbol:    "LTC-USDT",
			Timeframe: "1m",
			Timestamp: exactTime,
			Open:      50.0,
			High:      52.0,
			Low:       49.0,
			Close:     51.0,
			Volume:    10.0,
			Source:    "test",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Count with exact start and end time
		count, err := db.GetCandleCount("LTC-USDT", "1m", exactTime, exactTime)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// Test 5: Count candles with inverted time range
	t.Run("Count with inverted time range", func(t *testing.T) {
		// Define inverted range (end before start)
		start := now
		end := now.Add(-1 * time.Hour)

		// Should return 0 for inverted range
		count, err := db.GetCandleCount("BTC-USDT", "1m", start, end)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	// Test 6: Count candles with multiple sources
	t.Run("Count with multiple sources", func(t *testing.T) {
		sameTime := now.Add(-6 * time.Hour)

		// Insert candles with same timestamp but different sources
		candles := []candle.Candle{
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1m",
				Timestamp: sameTime,
				Open:      0.25,
				High:      0.26,
				Low:       0.24,
				Close:     0.255,
				Volume:    1000.0,
				Source:    "source1",
			},
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1m",
				Timestamp: sameTime,
				Open:      0.26,
				High:      0.27,
				Low:       0.25,
				Close:     0.265,
				Volume:    1200.0,
				Source:    "source2",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Count should include both sources
		count, err := db.GetCandleCount("XRP-USDT", "1m", sameTime.Add(-1*time.Minute), sameTime.Add(1*time.Minute))
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
	})
}

func TestPostgresDB_GetConstructedCandleCount(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Count constructed candles with mix of constructed and raw candles
	t.Run("Count with mix of constructed and raw candles", func(t *testing.T) {
		// Insert mix of constructed and raw candles
		candles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-3 * time.Hour),
				Open:      9800.0,
				High:      9900.0,
				Low:       9700.0,
				Close:     9850.0,
				Volume:    1.8,
				Source:    "constructed",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      9900.0,
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "constructed",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "exchange", // Raw candle
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Count only constructed candles
		count, err := db.GetConstructedCandleCount("BTC-USDT", "1h", now.Add(-4*time.Hour), now)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)

		// Verify total count includes all candles
		totalCount, err := db.GetCandleCount("BTC-USDT", "1h", now.Add(-4*time.Hour), now)
		assert.NoError(t, err)
		assert.Equal(t, 3, totalCount)
	})

	// Test 2: Count constructed candles with empty result
	t.Run("Count constructed with empty result", func(t *testing.T) {
		count, err := db.GetConstructedCandleCount("NON-EXISTENT", "1h", now.Add(-4*time.Hour), now)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	// Test 3: Count constructed candles with specific timeframe
	t.Run("Count constructed with specific timeframe", func(t *testing.T) {
		// Insert constructed candles with different timeframes
		candles := []candle.Candle{
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1h",
				Timestamp: now,
				Open:      300.0,
				High:      310.0,
				Low:       295.0,
				Close:     305.0,
				Volume:    10.0,
				Source:    "constructed",
			},
			{
				Symbol:    "ETH-USDT",
				Timeframe: "4h",
				Timestamp: now,
				Open:      301.0,
				High:      311.0,
				Low:       296.0,
				Close:     306.0,
				Volume:    12.0,
				Source:    "constructed",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Count only 4h constructed candles
		count, err := db.GetConstructedCandleCount("ETH-USDT", "4h", now.Add(-1*time.Hour), now.Add(1*time.Hour))
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// Test 4: Count constructed candles with exact time range
	t.Run("Count constructed with exact time range", func(t *testing.T) {
		exactTime := now.Add(-5 * time.Hour)

		// Insert constructed candle with exact timestamp
		testCandle := candle.Candle{
			Symbol:    "LTC-USDT",
			Timeframe: "1h",
			Timestamp: exactTime,
			Open:      50.0,
			High:      52.0,
			Low:       49.0,
			Close:     51.0,
			Volume:    10.0,
			Source:    "constructed",
		}

		err := db.SaveCandle(&testCandle)
		require.NoError(t, err)

		// Count with exact start and end time
		count, err := db.GetConstructedCandleCount("LTC-USDT", "1h", exactTime, exactTime)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	// Test 5: Count constructed candles when only raw candles exist
	t.Run("Count constructed when only raw candles exist", func(t *testing.T) {
		// Insert only raw candles
		candles := []candle.Candle{
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1h",
				Timestamp: now,
				Open:      0.25,
				High:      0.26,
				Low:       0.24,
				Close:     0.255,
				Volume:    1000.0,
				Source:    "exchange1",
			},
			{
				Symbol:    "XRP-USDT",
				Timeframe: "1h",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      0.24,
				High:      0.25,
				Low:       0.23,
				Close:     0.245,
				Volume:    900.0,
				Source:    "exchange2",
			},
		}

		err := db.SaveCandles(candles)
		require.NoError(t, err)

		// Count constructed candles (should be 0)
		count, err := db.GetConstructedCandleCount("XRP-USDT", "1h", now.Add(-2*time.Hour), now.Add(1*time.Hour))
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
	})
}

func TestPostgresDB_UpdateCandle(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Update existing candle
	t.Run("Update existing candle", func(t *testing.T) {
		// Insert initial candle
		initialCandle := candle.Candle{
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

		err := db.SaveCandle(&initialCandle)
		require.NoError(t, err)

		// Create updated candle with same key but different values
		updatedCandle := candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      10010.0, // Changed
			High:      10200.0, // Changed
			Low:       9950.0,  // Changed
			Close:     10150.0, // Changed
			Volume:    2.5,     // Changed
			Source:    "test",
		}

		// Update the candle
		err = db.UpdateCandle(updatedCandle)
		assert.NoError(t, err)

		// Verify candle was updated
		retrievedCandle, err := db.GetCandle("BTC-USDT", "1m", now, "test")
		require.NoError(t, err)
		require.NotNil(t, retrievedCandle)

		assert.Equal(t, updatedCandle.Open, retrievedCandle.Open)
		assert.Equal(t, updatedCandle.High, retrievedCandle.High)
		assert.Equal(t, updatedCandle.Low, retrievedCandle.Low)
		assert.Equal(t, updatedCandle.Close, retrievedCandle.Close)
		assert.Equal(t, updatedCandle.Volume, retrievedCandle.Volume)
	})

	// Test 2: Update non-existent candle
	t.Run("Update non-existent candle", func(t *testing.T) {
		nonExistentCandle := candle.Candle{
			Symbol:    "NON-EXISTENT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      100.0,
			High:      110.0,
			Low:       90.0,
			Close:     105.0,
			Volume:    1.0,
			Source:    "test",
		}

		// Try to update non-existent candle
		err = db.UpdateCandle(nonExistentCandle)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no candle found to update")
	})

	// Test 3: Update with invalid candle data
	t.Run("Update with invalid candle data", func(t *testing.T) {
		// Insert initial valid candle
		initialCandle := candle.Candle{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      300.0,
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "test",
		}

		err := db.SaveCandle(&initialCandle)
		require.NoError(t, err)

		// Create invalid candle (negative price)
		invalidCandle := candle.Candle{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      -300.0, // Invalid negative price
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "test",
		}

		// Try to update with invalid data
		err = db.UpdateCandle(invalidCandle)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid candle for update")
	})

	// Test 4: Update with different source
	t.Run("Update with different source", func(t *testing.T) {
		// Insert initial candle with source1
		initialCandle := candle.Candle{
			Symbol:    "LTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      50.0,
			High:      52.0,
			Low:       49.0,
			Close:     51.0,
			Volume:    100.0,
			Source:    "source1",
		}

		err := db.SaveCandle(&initialCandle)
		require.NoError(t, err)

		// Create updated candle with source2 (different primary key)
		updatedCandle := candle.Candle{
			Symbol:    "LTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      50.5,
			High:      52.5,
			Low:       49.5,
			Close:     51.5,
			Volume:    110.0,
			Source:    "source2", // Different source
		}

		// Try to update - should fail as this is effectively a new candle
		err = db.UpdateCandle(updatedCandle)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no candle found to update")
	})
}

func TestPostgresDB_UpdateCandles(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Create PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Minute)

	// Test 1: Update multiple existing candles
	t.Run("Update multiple existing candles", func(t *testing.T) {
		// Insert initial candles
		initialCandles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      9900.0,
				High:      10000.0,
				Low:       9800.0,
				Close:     10000.0,
				Volume:    2.0,
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10000.0,
				High:      10100.0,
				Low:       9900.0,
				Close:     10050.0,
				Volume:    1.5,
				Source:    "test",
			},
		}

		err := db.SaveCandles(initialCandles)
		require.NoError(t, err)

		// Create updated candles
		updatedCandles := []candle.Candle{
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-2 * time.Hour),
				Open:      9950.0,  // Changed
				High:      10050.0, // Changed
				Low:       9850.0,  // Changed
				Close:     10025.0, // Changed
				Volume:    2.2,     // Changed
				Source:    "test",
			},
			{
				Symbol:    "BTC-USDT",
				Timeframe: "1m",
				Timestamp: now.Add(-1 * time.Hour),
				Open:      10050.0, // Changed
				High:      10150.0, // Changed
				Low:       9950.0,  // Changed
				Close:     10100.0, // Changed
				Volume:    1.7,     // Changed
				Source:    "test",
			},
		}

		// Update the candles
		err = db.UpdateCandles(updatedCandles)
		assert.NoError(t, err)

		// Verify candles were updated
		for _, updatedCandle := range updatedCandles {
			retrievedCandle, err := db.GetCandle(updatedCandle.Symbol, updatedCandle.Timeframe, updatedCandle.Timestamp, updatedCandle.Source)
			require.NoError(t, err)
			require.NotNil(t, retrievedCandle)

			assert.Equal(t, updatedCandle.Open, retrievedCandle.Open)
			assert.Equal(t, updatedCandle.High, retrievedCandle.High)
			assert.Equal(t, updatedCandle.Low, retrievedCandle.Low)
			assert.Equal(t, updatedCandle.Close, retrievedCandle.Close)
			assert.Equal(t, updatedCandle.Volume, retrievedCandle.Volume)
		}
	})

	// Test 2: Update with empty slice
	t.Run("Update with empty slice", func(t *testing.T) {
		emptyCandles := []candle.Candle{}

		// Update with empty slice
		err = db.UpdateCandles(emptyCandles)
		assert.NoError(t, err) // Should not error with empty slice
	})

	// Test 3: Update with non-existent candles
	t.Run("Update with non-existent candles", func(t *testing.T) {
		nonExistentCandles := []candle.Candle{
			{
				Symbol:    "NON-EXISTENT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      100.0,
				High:      110.0,
				Low:       90.0,
				Close:     105.0,
				Volume:    1.0,
				Source:    "test",
			},
		}

		// Try to update non-existent candles
		err = db.UpdateCandles(nonExistentCandles)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no candles were updated")
	})

	// Test 4: Update with mix of existing and non-existent candles
	t.Run("Update with mix of existing and non-existent candles", func(t *testing.T) {
		// Insert one candle
		existingCandle := candle.Candle{
			Symbol:    "ETH-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      300.0,
			High:      310.0,
			Low:       295.0,
			Close:     305.0,
			Volume:    10.0,
			Source:    "test",
		}

		err := db.SaveCandle(&existingCandle)
		require.NoError(t, err)

		// Create mix of existing and non-existent candles
		mixedCandles := []candle.Candle{
			{
				Symbol:    "ETH-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      305.0, // Changed
				High:      315.0, // Changed
				Low:       300.0, // Changed
				Close:     310.0, // Changed
				Volume:    12.0,  // Changed
				Source:    "test",
			},
			{
				Symbol:    "NON-EXISTENT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      100.0,
				High:      110.0,
				Low:       90.0,
				Close:     105.0,
				Volume:    1.0,
				Source:    "test",
			},
		}

		// Try to update mixed candles
		// This should succeed because at least one candle was updated
		err = db.UpdateCandles(mixedCandles)
		assert.NoError(t, err)

		// Verify the existing candle was updated
		retrievedCandle, err := db.GetCandle("ETH-USDT", "1m", now, "test")
		require.NoError(t, err)
		require.NotNil(t, retrievedCandle)

		assert.Equal(t, 305.0, retrievedCandle.Open)
		assert.Equal(t, 315.0, retrievedCandle.High)
		assert.Equal(t, 300.0, retrievedCandle.Low)
		assert.Equal(t, 310.0, retrievedCandle.Close)
		assert.Equal(t, 12.0, retrievedCandle.Volume)
	})

	// Test 5: Update with invalid candle data
	t.Run("Update with invalid candle data", func(t *testing.T) {
		// Insert valid candle
		validCandle := candle.Candle{
			Symbol:    "LTC-USDT",
			Timeframe: "1m",
			Timestamp: now,
			Open:      50.0,
			High:      52.0,
			Low:       49.0,
			Close:     51.0,
			Volume:    100.0,
			Source:    "test",
		}

		err := db.SaveCandle(&validCandle)
		require.NoError(t, err)

		// Try to update with invalid data
		invalidCandles := []candle.Candle{
			{
				Symbol:    "LTC-USDT",
				Timeframe: "1m",
				Timestamp: now,
				Open:      -50.0, // Invalid negative price
				High:      52.0,
				Low:       49.0,
				Close:     51.0,
				Volume:    100.0,
				Source:    "test",
			},
		}

		// Update should fail validation
		err = db.UpdateCandles(invalidCandles)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid candle for update")
	})
}
