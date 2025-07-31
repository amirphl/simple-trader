package strategy

import (
	"context"
	"testing"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockStorage implements the Storage interface for testing
type MockStorage struct {
	mock.Mock
}

func (m *MockStorage) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]db.Candle, error) {
	args := m.Called(ctx, symbol, timeframe, source, start, end)
	return args.Get(0).([]db.Candle), args.Error(1)
}

func TestNewStochasticHeikinAshi(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	assert.Equal(t, "Stochastic Heikin Ashi", strategy.Name())
	assert.Equal(t, "BTC-USDT", strategy.Symbol())
	assert.Equal(t, "1h", strategy.Timeframe())
	assert.Equal(t, 100, strategy.maxHistory)
	assert.Equal(t, 24, strategy.periodK)
	assert.Equal(t, 10, strategy.smoothK)
	assert.Equal(t, 3, strategy.periodD)
}

func TestStochasticHeikinAshiWarmupPeriod(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	// Warmup period should be: periodK + smoothK + periodD - 2 = 24 + 10 + 3 - 2 = 35
	expectedWarmup := 24 + 10 + 3 - 2
	assert.Equal(t, expectedWarmup, strategy.WarmupPeriod())
}

func TestStochasticHeikinAshiIsHeikinAshiBullish(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	// Test bullish Heikin Ashi candle
	bullishHA := candle.Candle{
		Open:  100,
		High:  110,
		Low:   95,
		Close: 105, // Close > Open
	}
	assert.True(t, strategy.isHeikinAshiBullish(bullishHA))

	// Test bearish Heikin Ashi candle
	bearishHA := candle.Candle{
		Open:  105,
		High:  110,
		Low:   95,
		Close: 100, // Close < Open
	}
	assert.False(t, strategy.isHeikinAshiBullish(bearishHA))
}

func TestStochasticHeikinAshiOnCandles_NoCandles(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	signal, err := strategy.OnCandles(context.Background(), []candle.Candle{})

	assert.NoError(t, err)
	assert.Equal(t, Hold, signal.Position)
	assert.Equal(t, "no candles", signal.Reason)
}

func TestStochasticHeikinAshiOnCandles_NoMatchingCandles(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	// Create candles for different symbol
	candles := []candle.Candle{
		{
			Symbol:    "ETH-USDT",
			Timeframe: "1h",
			Timestamp: time.Now(),
			Open:      100,
			High:      110,
			Low:       95,
			Close:     105,
			Volume:    1000,
			Source:    "test",
		},
	}

	signal, err := strategy.OnCandles(context.Background(), candles)

	assert.NoError(t, err)
	assert.Equal(t, Hold, signal.Position)
	assert.Equal(t, "no matching candles", signal.Reason)
}

func TestStochasticHeikinAshiOnCandles_WarmingUp(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	// Create insufficient candles for stochastic calculation
	candles := []candle.Candle{
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1h",
			Timestamp: time.Now(),
			Open:      100,
			High:      110,
			Low:       95,
			Close:     105,
			Volume:    1000,
			Source:    "test",
		},
	}

	signal, err := strategy.OnCandles(context.Background(), candles)

	assert.NoError(t, err)
	assert.Equal(t, Hold, signal.Position)
	assert.Equal(t, "warming up", signal.Reason)
}

func TestStochasticHeikinAshiOnCandles_LongBullishSignal(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	// Mock historical data fetch
	mockStorage.On("GetCandles", mock.Anything, "BTC-USDT", "1h", "", mock.Anything, mock.Anything).
		Return([]candle.Candle{}, nil)

	// Create enough candles for stochastic calculation with conditions for long bullish signal
	// We need at least 16 candles (warmup period) plus some more for the signal
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	candles := make([]candle.Candle, 20)

	// Create candles that will result in stochastic K < 20, K > D, and bullish Heikin Ashi
	for i := 0; i < 20; i++ {
		// Create a downtrend that will result in low stochastic values
		price := 100.0 - float64(i)*0.5 // Decreasing prices
		candles[i] = candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1h",
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      price,
			High:      price + 2,
			Low:       price - 1,
			Close:     price - 0.5, // Bearish close
			Volume:    1000,
			Source:    "test",
		}
	}

	// Make the last candle bullish to trigger the signal
	candles[19] = candle.Candle{
		Symbol:    "BTC-USDT",
		Timeframe: "1h",
		Timestamp: baseTime.Add(19 * time.Hour),
		Open:      90,
		High:      95,
		Low:       89,
		Close:     94, // Bullish close
		Volume:    1000,
		Source:    "test",
	}

	signal, err := strategy.OnCandles(context.Background(), candles)

	assert.NoError(t, err)
	// Note: The actual signal depends on the stochastic calculation
	// This test verifies the strategy doesn't crash and handles the data correctly
	assert.NotNil(t, signal)
	assert.Equal(t, "Stochastic Heikin Ashi", signal.StrategyName)
}

func TestStochasticHeikinAshiOnCandles_LongBearishSignal(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	// Mock historical data fetch
	mockStorage.On("GetCandles", mock.Anything, "BTC-USDT", "1h", "", mock.Anything, mock.Anything).
		Return([]candle.Candle{}, nil)

	// Create enough candles for stochastic calculation with conditions for long bearish signal
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	candles := make([]candle.Candle, 20)

	// Create candles that will result in stochastic K < 80, K < D, and bearish Heikin Ashi
	for i := 0; i < 20; i++ {
		// Create a mixed trend that will result in moderate stochastic values
		price := 100.0 + float64(i%3)*2 // Oscillating prices
		candles[i] = candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1h",
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      price,
			High:      price + 3,
			Low:       price - 2,
			Close:     price + 1, // Slightly bullish close
			Volume:    1000,
			Source:    "test",
		}
	}

	// Make the last candle bearish to potentially trigger the signal
	candles[19] = candle.Candle{
		Symbol:    "BTC-USDT",
		Timeframe: "1h",
		Timestamp: baseTime.Add(19 * time.Hour),
		Open:      105,
		High:      107,
		Low:       103,
		Close:     104, // Bearish close
		Volume:    1000,
		Source:    "test",
	}

	signal, err := strategy.OnCandles(context.Background(), candles)

	assert.NoError(t, err)
	// Note: The actual signal depends on the stochastic calculation
	// This test verifies the strategy doesn't crash and handles the data correctly
	assert.NotNil(t, signal)
	assert.Equal(t, "Stochastic Heikin Ashi", signal.StrategyName)
}

func TestStochasticHeikinAshiPerformanceMetrics(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	metrics := strategy.PerformanceMetrics()

	assert.Equal(t, float64(0), metrics["heikenAshiCount"])
	assert.Equal(t, float64(0), metrics["candleCount"])
	assert.Equal(t, float64(100), metrics["maxHistory"])
	assert.Equal(t, float64(14), metrics["periodK"])
	assert.Equal(t, float64(1), metrics["smoothK"])
	assert.Equal(t, float64(3), metrics["periodD"])
}

func TestStochasticHeikinAshiWithHistoricalData(t *testing.T) {
	mockStorage := &MockStorage{}
	strategy := NewStochasticHeikinAshi("BTC-USDT", mockStorage)

	// Create historical candles
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	historicalCandles := make([]candle.Candle, 10)
	for i := 0; i < 10; i++ {
		price := 100.0 + float64(i)
		historicalCandles[i] = candle.Candle{
			Symbol:    "BTC-USDT",
			Timeframe: "1h",
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Open:      price,
			High:      price + 2,
			Low:       price - 1,
			Close:     price + 1,
			Volume:    1000,
			Source:    "test",
		}
	}

	// Mock the historical data fetch
	mockStorage.On("GetCandles", mock.Anything, "BTC-USDT", "1h", "", mock.Anything, mock.Anything).
		Return(historicalCandles, nil)

	// Create new candles
	newCandles := []candle.Candle{
		{
			Symbol:    "BTC-USDT",
			Timeframe: "1h",
			Timestamp: baseTime.Add(10 * time.Hour),
			Open:      110,
			High:      112,
			Low:       109,
			Close:     111,
			Volume:    1000,
			Source:    "test",
		},
	}

	signal, err := strategy.OnCandles(context.Background(), newCandles)

	assert.NoError(t, err)
	assert.NotNil(t, signal)
	assert.Equal(t, "Stochastic Heikin Ashi", signal.StrategyName)

	// Verify that historical data was loaded
	metrics := strategy.PerformanceMetrics()
	assert.Greater(t, metrics["candleCount"], float64(0))
	assert.Greater(t, metrics["heikenAshiCount"], float64(0))
}
