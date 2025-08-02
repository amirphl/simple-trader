// Package strategy
package strategy

import (
	"context"
	"sort"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/indicator"
	"github.com/amirphl/simple-trader/internal/utils"
)

// RSIStrategy implements a trading strategy based on the Relative Strength Index
type RSIStrategy struct {
	symbol     string
	Period     int
	Overbought float64
	Oversold   float64
	prices     []float64

	Storage db.Storage

	lastCandle     *candle.Candle
	lastRSI        float64
	initialized    bool
	maxHistorySize int // Maximum number of prices to keep in memory
}

// NewRSIStrategy creates a new RSI strategy with the given parameters
func NewRSIStrategy(symbol string, period int, overbought, oversold float64, storage db.Storage) *RSIStrategy {
	// Keep a reasonable history size - 10x the period is usually sufficient
	maxHistorySize := period * 10
	if maxHistorySize < 1000 {
		maxHistorySize = 1000 // Minimum size to ensure we have enough data
	}

	return &RSIStrategy{
		symbol:         symbol,
		Period:         period,
		Overbought:     overbought,
		Oversold:       oversold,
		Storage:        storage,
		prices:         make([]float64, 0, maxHistorySize),
		initialized:    false,
		maxHistorySize: maxHistorySize,
	}
}

// Name returns the name of the strategy
func (s *RSIStrategy) Name() string { return "RSI" }

// Symbol returns the symbol this strategy is configured for
func (s *RSIStrategy) Symbol() string { return s.symbol }

// Timeframe returns the timeframe this strategy is configured for
func (s *RSIStrategy) Timeframe() string { return "1m" }

// trimPrices ensures the prices array doesn't grow beyond maxHistorySize
func (s *RSIStrategy) trimPrices() {
	if len(s.prices) > s.maxHistorySize {
		// Keep the most recent data
		excess := len(s.prices) - s.maxHistorySize
		s.prices = s.prices[excess:]
	}
}

// OnCandles processes new candles and generates trading signals
func (s *RSIStrategy) OnCandles(ctx context.Context, oneMinCandles []candle.Candle) (Signal, error) {
	utils.GetLogger().Printf("Strategy | [%s RSI] Received %d 1m candles", s.symbol, len(oneMinCandles))

	// TODO: Handle duplicate candles
	// TODO: Handle missing candles from last candle in db to first received candle (somewhat possible because of UTC)
	// TODO: Handle missing candles inside oneMinCandles, validate, sort, truncate (impossible)
	if len(oneMinCandles) == 0 {
		utils.GetLogger().Printf("Strategy | [%s RSI] No candles received", s.symbol)
		return Signal{
			Time:         time.Now().UTC(),
			Position:     Hold,
			Reason:       "no candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
			Candle:       nil,
		}, nil
	}

	// Filter candles for this symbol
	var filteredCandles []candle.Candle
	for _, c := range oneMinCandles {
		if c.Symbol == s.symbol && c.Timeframe == "1m" && (s.lastCandle == nil || c.Timestamp.After(s.lastCandle.Timestamp)) {
			filteredCandles = append(filteredCandles, c)
		}
	}

	if len(filteredCandles) == 0 {
		utils.GetLogger().Printf("Strategy | [%s RSI] No matching candles", s.symbol)
		return Signal{
			Time:         time.Now().UTC(),
			Position:     Hold,
			Reason:       "no matching candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
			Candle:       nil,
		}, nil
	}

	s.lastCandle = &filteredCandles[len(filteredCandles)-1]

	// Sort candles by timestamp to ensure proper order
	sort.Slice(filteredCandles, func(i, j int) bool {
		return filteredCandles[i].Timestamp.Before(filteredCandles[j].Timestamp)
	})

	// Initialize with historical data if needed
	if len(s.prices) == 0 && !s.initialized {
		s.initialized = true // Mark as initialized to avoid fetching again

		// Calculate time range: from 1 year ago to 1 minute before the first candle
		endTime := filteredCandles[0].Timestamp.Truncate(time.Minute)
		startTime := endTime.AddDate(-1, 0, 0) // 1 year before

		utils.GetLogger().Printf("Strategy | [%s RSI] Fetching historical 1m candles from %s to %s\n",
			s.symbol, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Fetch historical 1m candles
		historicalCandles, err := s.Storage.GetCandles(ctx, s.symbol, "1m", "", startTime, endTime)
		if err != nil {
			utils.GetLogger().Printf("Strategy | [%s RSI] Error fetching historical candles from database: %v\n", s.symbol, err)
			// Continue with the current candles even if historical fetch fails
		} else if len(historicalCandles) > 0 {
			utils.GetLogger().Printf("Strategy | [%s RSI] Loaded %d historical candles from database\n", s.symbol, len(historicalCandles))

			// Sort historical candles by timestamp
			sort.Slice(historicalCandles, func(i, j int) bool {
				return historicalCandles[i].Timestamp.Before(historicalCandles[j].Timestamp)
			})

			// Add historical prices to the prices array
			for _, c := range historicalCandles {
				s.prices = append(s.prices, c.Close)
			}

			// Trim if we have too many prices
			s.trimPrices()
		}
	}

	// Add current candle prices to the array
	for _, c := range filteredCandles {
		s.prices = append(s.prices, c.Close)
	}

	// Trim prices to maintain a reasonable history size
	s.trimPrices()

	// Check if we have enough data for RSI calculation
	if len(s.prices) <= s.Period {
		utils.GetLogger().Printf("Strategy | [%s RSI] Not enough data for RSI calculation", s.symbol)
		return Signal{
			Time:         s.lastCandle.Timestamp,
			Position:     Hold,
			Reason:       "warming up",
			StrategyName: s.Name(),
			TriggerPrice: s.lastCandle.Close,
			Candle:       s.lastCandle,
		}, nil
	}

	// Use the more efficient CalculateLastRSI instead of calculating the entire array
	rsi, err := indicator.CalculateLastRSI(s.prices, s.Period)
	if err != nil {
		utils.GetLogger().Printf("Strategy | [%s RSI] Error calculating RSI: %v\n", s.symbol, err)
		return Signal{
			Time:         s.lastCandle.Timestamp,
			Position:     Hold,
			Reason:       "RSI calculation error",
			StrategyName: s.Name(),
			TriggerPrice: s.lastCandle.Close,
			Candle:       s.lastCandle,
		}, nil
	}

	if rsi > s.Oversold && s.lastRSI < s.Oversold && s.lastRSI != 0 {
		s.lastRSI = rsi
		utils.GetLogger().Printf("Strategy | [%s RSI] Signal changed to BUY - RSI: %.2f (below %v), Price: %.2f\n",
			s.symbol, rsi, s.Oversold, s.lastCandle.Close)
		return Signal{
			Time:         s.lastCandle.Timestamp,
			Position:     LongBullish,
			Reason:       "RSI oversold",
			StrategyName: s.Name(),
			TriggerPrice: s.lastCandle.Close,
			Candle:       s.lastCandle,
		}, nil
	}

	if rsi > s.Overbought && s.lastRSI < s.Overbought && s.lastRSI != 0 {
		s.lastRSI = rsi
		utils.GetLogger().Printf("Strategy | [%s RSI] Signal changed to SELL - RSI: %.2f (above %v), Price: %.2f\n",
			s.symbol, rsi, s.Overbought, s.lastCandle.Close)
		return Signal{
			Time:         s.lastCandle.Timestamp,
			Position:     LongBearish,
			Reason:       "RSI overbought",
			StrategyName: s.Name(),
			TriggerPrice: s.lastCandle.Close,
			Candle:       s.lastCandle,
		}, nil
	}

	s.lastRSI = rsi

	// Generate signal based on RSI value
	// var action, reason string
	// if rsi < s.Oversold {
	// 	action = "buy"
	// 	reason = "RSI oversold"

	// 	// Log signal change
	// 	if !s.lastSignalWasBuy {
	// 		utils.GetLogger().Printf("[%s RSI] Signal changed to BUY - RSI: %.2f (below %v), Price: %.2f\n",
	// 			s.symbol, rsi, s.Oversold, lastCandle.Close)
	// 		s.lastSignalWasBuy = true
	// 	}
	// } else if rsi > s.Overbought {
	// 	action = "sell"
	// 	reason = "RSI overbought"

	// 	// Log signal change
	// 	if s.lastSignalWasBuy {
	// 		utils.GetLogger().Printf("[%s RSI] Signal changed to SELL - RSI: %.2f (above %v), Price: %.2f\n",
	// 			s.symbol, rsi, s.Overbought, lastCandle.Close)
	// 		s.lastSignalWasBuy = false
	// 	}
	// } else {
	// 	action = "hold"
	// 	reason = "RSI neutral"
	// }

	utils.GetLogger().Printf("Strategy | [%s RSI] Holding - RSI: %.2f", s.symbol, rsi)

	return Signal{
		Time:         s.lastCandle.Timestamp,
		Position:     Hold,
		Reason:       "RSI neutral",
		StrategyName: s.Name(),
		TriggerPrice: s.lastCandle.Close,
		Candle:       s.lastCandle,
	}, nil
}

// PerformanceMetrics returns performance metrics for the strategy
func (s *RSIStrategy) PerformanceMetrics() map[string]float64 {
	return map[string]float64{
		"lastRSI":        s.lastRSI,
		"historySize":    float64(len(s.prices)),
		"maxHistorySize": float64(s.maxHistorySize),
		"period":         float64(s.Period),
		"overbought":     s.Overbought,
		"oversold":       s.Oversold,
	}
}

// WarmupPeriod returns the number of candles needed for warm-up
func (s *RSIStrategy) WarmupPeriod() int {
	return s.Period
}
