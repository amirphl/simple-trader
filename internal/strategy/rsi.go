// Package strategy
package strategy

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/indicator"
)

// Storage interface defines methods for retrieving candle data
type Storage interface {
	GetCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]candle.Candle, error)
}

// RSIStrategy implements a trading strategy based on the Relative Strength Index
type RSIStrategy struct {
	symbol     string
	Period     int
	Overbought float64
	Oversold   float64
	prices     []float64

	Storage Storage

	lastRSI          float64
	initialized      bool
	maxHistorySize   int  // Maximum number of prices to keep in memory
	lastSignalWasBuy bool // Track the last signal for signal flipping detection
}

// NewRSIStrategy creates a new RSI strategy with the given parameters
func NewRSIStrategy(symbol string, period int, overbought, oversold float64, storage Storage) *RSIStrategy {
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
	if len(oneMinCandles) == 0 {
		return Signal{
			Time:         time.Now().UTC(),
			Action:       "hold",
			Reason:       "no candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
		}, nil
	}

	// Filter candles for this symbol
	var filteredCandles []candle.Candle
	for _, c := range oneMinCandles {
		if c.Symbol == s.symbol && c.Timeframe == "1m" {
			filteredCandles = append(filteredCandles, c)
		}
	}

	if len(filteredCandles) == 0 {
		return Signal{
			Time:         time.Now().UTC(),
			Action:       "hold",
			Reason:       "no matching candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
		}, nil
	}

	// Sort candles by timestamp to ensure proper order
	sort.Slice(filteredCandles, func(i, j int) bool {
		return filteredCandles[i].Timestamp.Before(filteredCandles[j].Timestamp)
	})

	// Get the last candle for signal generation
	lastCandle := filteredCandles[len(filteredCandles)-1]

	// Initialize with historical data if needed
	if len(s.prices) == 0 && !s.initialized {
		s.initialized = true // Mark as initialized to avoid fetching again

		// Calculate time range: from 1 year ago to 1 minute before the first candle
		endTime := filteredCandles[0].Timestamp.Truncate(time.Minute)
		startTime := endTime.AddDate(-1, 0, 0) // 1 year before

		log.Printf("[%s RSI] Fetching historical 1m candles from %s to %s\n",
			s.symbol, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Fetch historical 1m candles
		historicalCandles, err := s.Storage.GetCandles(ctx, s.symbol, "1m", startTime, endTime)
		if err != nil {
			log.Printf("[%s RSI] Error fetching historical candles from database: %v\n", s.symbol, err)
			// Continue with the current candles even if historical fetch fails
		} else if len(historicalCandles) > 0 {
			log.Printf("[%s RSI] Loaded %d historical candles from database\n", s.symbol, len(historicalCandles))

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
		return Signal{
			Time:         lastCandle.Timestamp,
			Action:       "hold",
			Reason:       "warming up",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	// Use the more efficient CalculateLastRSI instead of calculating the entire array
	rsi, err := indicator.CalculateLastRSI(s.prices, s.Period)
	if err != nil {
		log.Printf("[%s RSI] Error calculating RSI: %v\n", s.symbol, err)
		return Signal{
			Time:         lastCandle.Timestamp,
			Action:       "hold",
			Reason:       "RSI calculation error",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	if rsi > s.Oversold && s.lastRSI < s.Oversold {
		s.lastRSI = rsi
		// Log signal change
		if !s.lastSignalWasBuy {
			log.Printf("[%s RSI] Signal changed to BUY - RSI: %.2f (below %v), Price: %.2f\n",
				s.symbol, rsi, s.Oversold, lastCandle.Close)
			s.lastSignalWasBuy = true
		}

		return Signal{
			Time:         lastCandle.Timestamp,
			Action:       "buy",
			Reason:       "RSI oversold",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	if rsi > s.Overbought && s.lastRSI < s.Overbought {
		s.lastRSI = rsi
		// Log signal change
		if s.lastSignalWasBuy {
			log.Printf("[%s RSI] Signal changed to SELL - RSI: %.2f (above %v), Price: %.2f\n",
				s.symbol, rsi, s.Overbought, lastCandle.Close)
			s.lastSignalWasBuy = false
		}
		return Signal{
			Time:         lastCandle.Timestamp,
			Action:       "sell",
			Reason:       "RSI overbought",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
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
	// 		log.Printf("[%s RSI] Signal changed to BUY - RSI: %.2f (below %v), Price: %.2f\n",
	// 			s.symbol, rsi, s.Oversold, lastCandle.Close)
	// 		s.lastSignalWasBuy = true
	// 	}
	// } else if rsi > s.Overbought {
	// 	action = "sell"
	// 	reason = "RSI overbought"

	// 	// Log signal change
	// 	if s.lastSignalWasBuy {
	// 		log.Printf("[%s RSI] Signal changed to SELL - RSI: %.2f (above %v), Price: %.2f\n",
	// 			s.symbol, rsi, s.Overbought, lastCandle.Close)
	// 		s.lastSignalWasBuy = false
	// 	}
	// } else {
	// 	action = "hold"
	// 	reason = "RSI neutral"
	// }

	return Signal{
		Time:         lastCandle.Timestamp,
		Action:       "hold",
		Reason:       "RSI neutral",
		StrategyName: s.Name(),
		TriggerPrice: lastCandle.Close,
		Candle:       lastCandle,
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
