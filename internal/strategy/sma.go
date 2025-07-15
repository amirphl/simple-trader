// Package strategy
package strategy

// import (
// 	"context"
// 	"log"
// 	"sort"
// 	"time"

// 	"github.com/amirphl/simple-trader/internal/candle"
// 	"github.com/amirphl/simple-trader/internal/indicator"
// )

// // SMAStrategy implements a trading strategy based on Simple Moving Average crossovers
// type SMAStrategy struct {
// 	symbol     string
// 	FastPeriod int
// 	SlowPeriod int
// 	prices     []float64

// 	Storage Storage

// 	lastFastSMA      float64
// 	lastSlowSMA      float64
// 	initialized      bool
// 	maxHistorySize   int  // Maximum number of prices to keep in memory
// 	lastSignalWasBuy bool // Track the last signal for signal flipping detection
// }

// // NewSMAStrategy creates a new SMA strategy with the given parameters
// func NewSMAStrategy(symbol string, fastPeriod, slowPeriod int, storage Storage) *SMAStrategy {
// 	// Keep a reasonable history size - 10x the slow period is usually sufficient
// 	maxHistorySize := slowPeriod * 10
// 	if maxHistorySize < 1000 {
// 		maxHistorySize = 1000 // Minimum size to ensure we have enough data
// 	}

// 	return &SMAStrategy{
// 		symbol:         symbol,
// 		FastPeriod:     fastPeriod,
// 		SlowPeriod:     slowPeriod,
// 		Storage:        storage,
// 		prices:         make([]float64, 0, maxHistorySize),
// 		initialized:    false,
// 		maxHistorySize: maxHistorySize,
// 	}
// }

// // Name returns the name of the strategy
// func (s *SMAStrategy) Name() string { return "SMA Crossover" }

// // Symbol returns the symbol this strategy is configured for
// func (s *SMAStrategy) Symbol() string { return s.symbol }

// // trimPrices ensures the prices array doesn't grow beyond maxHistorySize
// func (s *SMAStrategy) trimPrices() {
// 	if len(s.prices) > s.maxHistorySize {
// 		// Keep the most recent data
// 		excess := len(s.prices) - s.maxHistorySize
// 		s.prices = s.prices[excess:]
// 	}
// }

// // OnCandles processes new candles and generates trading signals
// func (s *SMAStrategy) OnCandles(candles []candle.Candle) (Signal, error) {
// 	if len(candles) == 0 {
// 		return Signal{
// 			Action:       "hold",
// 			Reason:       "no candles",
// 			StrategyName: s.Name(),
// 			Time:         time.Now().UTC(),
// 		}, nil
// 	}

// 	// Filter candles for this symbol
// 	var filteredCandles []candle.Candle
// 	for _, c := range candles {
// 		if c.Symbol == s.symbol {
// 			filteredCandles = append(filteredCandles, c)
// 		}
// 	}

// 	if len(filteredCandles) == 0 {
// 		return Signal{
// 			Action:       "hold",
// 			Reason:       "no matching candles",
// 			StrategyName: s.Name(),
// 			Time:         time.Now().UTC(),
// 		}, nil
// 	}

// 	// Sort candles by timestamp to ensure proper order
// 	sort.Slice(filteredCandles, func(i, j int) bool {
// 		return filteredCandles[i].Timestamp.Before(filteredCandles[j].Timestamp)
// 	})

// 	// Get the last candle for signal generation
// 	lastCandle := filteredCandles[len(filteredCandles)-1]

// 	// Initialize with historical data if needed
// 	if len(s.prices) == 0 && !s.initialized {
// 		s.initialized = true // Mark as initialized to avoid fetching again

// 		// Calculate time range: from 1 year ago to 1 minute before the first candle
// 		endTime := filteredCandles[0].Timestamp
// 		startTime := endTime.AddDate(-1, 0, 0) // 1 year before

// 		// Use a reasonable timeout for the database query
// 		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
// 		defer cancel()

// 		log.Printf("[%s SMA] Fetching historical 1m candles from %s to %s\n",
// 			s.symbol, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

// 		// Fetch historical 1m candles
// 		historicalCandles, err := s.Storage.GetCandles(ctx, s.symbol, "1m", "", startTime, endTime)
// 		if err != nil {
// 			log.Printf("[%s SMA] Error fetching historical candles: %v\n", s.symbol, err)
// 			// Continue with the current candles even if historical fetch fails
// 		} else if len(historicalCandles) > 0 {
// 			log.Printf("[%s SMA] Loaded %d historical candles\n", s.symbol, len(historicalCandles))

// 			// Sort historical candles by timestamp
// 			sort.Slice(historicalCandles, func(i, j int) bool {
// 				return historicalCandles[i].Timestamp.Before(historicalCandles[j].Timestamp)
// 			})

// 			// Add historical prices to the prices array
// 			for _, c := range historicalCandles {
// 				s.prices = append(s.prices, c.Close)
// 			}

// 			// Trim if we have too many prices
// 			s.trimPrices()
// 		}
// 	}

// 	// Add current candle prices to the array
// 	for _, c := range filteredCandles {
// 		s.prices = append(s.prices, c.Close)
// 	}

// 	// Trim prices to maintain a reasonable history size
// 	s.trimPrices()

// 	// Check if we have enough data for SMA calculation
// 	if len(s.prices) <= s.SlowPeriod {
// 		return Signal{
// 			Time:         lastCandle.Timestamp,
// 			Action:       "hold",
// 			Reason:       "warming up",
// 			StrategyName: s.Name(),
// 			TriggerPrice: lastCandle.Close,
// 			Candle:       lastCandle,
// 		}, nil
// 	}

// 	// Calculate both fast and slow SMAs
// 	periods := []int{s.FastPeriod, s.SlowPeriod}
// 	smaValues, err := indicator.CalculateMultipleLastSMA(s.prices, periods)
// 	if err != nil {
// 		log.Printf("[%s SMA] Error calculating SMA: %v\n", s.symbol, err)
// 		return Signal{
// 			Time:         lastCandle.Timestamp,
// 			Action:       "hold",
// 			Reason:       "SMA calculation error",
// 			StrategyName: s.Name(),
// 			TriggerPrice: lastCandle.Close,
// 			Candle:       lastCandle,
// 		}, nil
// 	}

// 	// Get the SMA values
// 	fastSMA := smaValues[s.FastPeriod]
// 	slowSMA := smaValues[s.SlowPeriod]

// 	// Store the current values for next comparison
// 	prevFastSMA := s.lastFastSMA
// 	prevSlowSMA := s.lastSlowSMA
// 	s.lastFastSMA = fastSMA
// 	s.lastSlowSMA = slowSMA

// 	// Generate signal based on SMA crossover
// 	var action, reason string

// 	// We need at least two data points to detect a crossover
// 	if len(s.prices) > s.SlowPeriod+1 {
// 		// Check for crossover
// 		if prevFastSMA <= prevSlowSMA && fastSMA > slowSMA {
// 			// Bullish crossover (fast crosses above slow)
// 			action = "buy"
// 			reason = "SMA bullish crossover"

// 			// Log signal change
// 			if !s.lastSignalWasBuy {
// 				log.Printf("[%s SMA] Signal changed to BUY - Fast SMA: %.2f crossed above Slow SMA: %.2f, Price: %.2f\n",
// 					s.symbol, fastSMA, slowSMA, lastCandle.Close)
// 				s.lastSignalWasBuy = true
// 			}
// 		} else if prevFastSMA >= prevSlowSMA && fastSMA < slowSMA {
// 			// Bearish crossover (fast crosses below slow)
// 			action = "sell"
// 			reason = "SMA bearish crossover"

// 			// Log signal change
// 			if s.lastSignalWasBuy {
// 				log.Printf("[%s SMA] Signal changed to SELL - Fast SMA: %.2f crossed below Slow SMA: %.2f, Price: %.2f\n",
// 					s.symbol, fastSMA, slowSMA, lastCandle.Close)
// 				s.lastSignalWasBuy = false
// 			}
// 		} else {
// 			action = "hold"
// 			reason = "no SMA crossover"
// 		}
// 	} else {
// 		action = "hold"
// 		reason = "insufficient data for crossover detection"
// 	}

// 	return Signal{
// 		Time:         lastCandle.Timestamp,
// 		Action:       action,
// 		Reason:       reason,
// 		StrategyName: s.Name(),
// 		TriggerPrice: lastCandle.Close,
// 		Candle:       lastCandle,
// 	}, nil
// }

// // PerformanceMetrics returns performance metrics for the strategy
// func (s *SMAStrategy) PerformanceMetrics() map[string]float64 {
// 	return map[string]float64{
// 		"lastFastSMA":    s.lastFastSMA,
// 		"lastSlowSMA":    s.lastSlowSMA,
// 		"historySize":    float64(len(s.prices)),
// 		"maxHistorySize": float64(s.maxHistorySize),
// 		"fastPeriod":     float64(s.FastPeriod),
// 		"slowPeriod":     float64(s.SlowPeriod),
// 	}
// }

// // WarmupPeriod returns the number of candles needed for warm-up
// func (s *SMAStrategy) WarmupPeriod() int {
// 	return s.SlowPeriod
// }
