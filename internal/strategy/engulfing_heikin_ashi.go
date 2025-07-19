// Package strategy
package strategy

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
)

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

type EngulfingHeikinAshi struct {
	symbol            string
	candles           []candle.Candle
	heikenAshiCandles []candle.Candle

	Storage Storage

	initialized bool
	maxHistory  int // Maximum number of candles to keep in memory
}

func NewEngulfingHeikinAshi(symbol string, storage Storage) *EngulfingHeikinAshi {
	return &EngulfingHeikinAshi{
		symbol:      symbol,
		Storage:     storage,
		initialized: false,
		maxHistory:  100, // Keep only the last 100 candles in memory
	}
}

// Name returns the name of the strategy
func (s *EngulfingHeikinAshi) Name() string { return "Engulfing Heikin Ashi" }

// Symbol returns the symbol this strategy is configured for
func (s *EngulfingHeikinAshi) Symbol() string { return s.symbol }

// Timeframe returns the timeframe this strategy is configured for
func (s *EngulfingHeikinAshi) Timeframe() string { return "1h" }

// trimCandles ensures we don't keep too many candles in memory
func (s *EngulfingHeikinAshi) trimCandles() {
	if len(s.candles) > s.maxHistory {
		excess := len(s.candles) - s.maxHistory
		s.candles = s.candles[excess:]
	}
	if len(s.heikenAshiCandles) > s.maxHistory {
		excess := len(s.heikenAshiCandles) - s.maxHistory
		s.heikenAshiCandles = s.heikenAshiCandles[excess:]
	}
}

// OnCandles processes new candles and generates trading signals
func (s *EngulfingHeikinAshi) OnCandles(ctx context.Context, oneHourCandles []candle.Candle) (Signal, error) {
	// TODO: Handle duplicate candles
	// TODO: Handle missing candles from last candle in db to first received candle (somewhat possible because of UTC)
	// TODO: Handle missing candles inside oneHourCandles, validate, sort, truncate (impossible)
	if len(oneHourCandles) == 0 {
		return Signal{
			Time:         time.Now().UTC(),
			Position:     Hold,
			Reason:       "no candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
			Candle:       nil,
		}, nil
	}

	var lastCandle *candle.Candle
	if len(s.candles) > 0 {
		lastCandle = &s.candles[len(s.candles)-1]
	}

	// Filter candles for this symbol
	var filteredCandles []candle.Candle
	for _, c := range oneHourCandles {
		if c.Symbol == s.symbol && c.Timeframe == "1h" && (lastCandle == nil || c.Timestamp.After(lastCandle.Timestamp)) {
			filteredCandles = append(filteredCandles, c)
		}
	}

	if len(filteredCandles) == 0 {
		return Signal{
			Time:         time.Now().UTC(),
			Position:     Hold,
			Reason:       "no matching candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
			Candle:       nil,
		}, nil
	}

	// Sort candles by timestamp to ensure proper order
	sort.Slice(filteredCandles, func(i, j int) bool {
		return filteredCandles[i].Timestamp.Before(filteredCandles[j].Timestamp)
	})

	// Initialize with historical data if needed
	if len(s.candles) == 0 && !s.initialized {
		s.initialized = true // Mark as initialized to avoid fetching again

		// Calculate time range: from 24 hours ago to 1 hour before the first candle
		// We only need a few candles for warmup, not a full year
		endTime := filteredCandles[0].Timestamp.Truncate(time.Hour)
		startTime := endTime.Add(-24 * time.Hour) // Just 24 hours should be enough

		log.Printf("Strategy | [%s Engulfing Heikin Ashi] Fetching historical 1h candles from %s to %s\n",
			s.symbol, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Fetch historical 1h candles
		historicalCandles, err := s.Storage.GetCandles(ctx, s.symbol, "1h", "", startTime, endTime)
		if err != nil {
			log.Printf("Strategy | [%s Engulfing Heikin Ashi] Error fetching historical candles from database: %v\n", s.symbol, err)
			// Continue with the current candles even if historical fetch fails
		} else if len(historicalCandles) > 0 {
			log.Printf("Strategy | [%s Engulfing Heikin Ashi] Loaded %d historical candles from database\n", s.symbol, len(historicalCandles))

			// Sort historical candles by timestamp
			sort.Slice(historicalCandles, func(i, j int) bool {
				return historicalCandles[i].Timestamp.Before(historicalCandles[j].Timestamp)
			})

			s.candles = append(s.candles, historicalCandles...)

			s.heikenAshiCandles = candle.GenerateHeikenAshiCandles(historicalCandles)
		}
	}

	var lastHeikenAshiCandle *candle.Candle
	if len(s.heikenAshiCandles) > 0 {
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Add current candle prices to the array
	for _, c := range filteredCandles {
		s.candles = append(s.candles, c)
		s.heikenAshiCandles = append(s.heikenAshiCandles, candle.GenerateNextHeikenAshiCandle(lastHeikenAshiCandle, c))
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Trim candles to prevent memory leaks
	s.trimCandles()

	// Get the latest regular candle for the signal
	lastCandle = &s.candles[len(s.candles)-1]

	// Check if we have enough data for Engulfing Heikin Ashi calculation
	if len(s.heikenAshiCandles) < 2 {
		return Signal{
			Time:         lastCandle.Timestamp,
			Position:     Hold,
			Reason:       "warming up",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	prevCandle := s.candles[len(s.candles)-2]
	currCandle := s.candles[len(s.candles)-1]
	// prevHA := s.heikenAshiCandles[len(s.heikenAshiCandles)-2]
	currHA := s.heikenAshiCandles[len(s.heikenAshiCandles)-1]

	// Buy signal: bullish engulfing pattern
	// 1. Current candle is bullish (close > open)
	// 2. Current open is near or below previous close
	// 3. Current close is above previous open
	// 4. Current body size is significantly larger than previous body size

	// Tolerance for open < previous.close
	openCloseTolerance := 0.0002 * prevCandle.Close // 0.02%
	openBelowPrevClose := currCandle.Open < prevCandle.Close || abs(currCandle.Open-prevCandle.Close) <= openCloseTolerance
	closeAbovePrevOpen := currCandle.Close > prevCandle.Open
	currBody := abs(currCandle.Close - currCandle.Open)
	prevBody := abs(prevCandle.Close - prevCandle.Open) // Fixed: Use close-open for both candles
	prevBearish := prevCandle.Close < prevCandle.Open
	bodyRatio := 0.0
	if prevBody > 0 {
		bodyRatio = currBody / prevBody
	}

	// Check for bullish engulfing pattern
	if currCandle.Close > currCandle.Open && // Current candle is bullish
		openBelowPrevClose &&
		closeAbovePrevOpen &&
		bodyRatio >= 1.8 &&
		prevBearish {

		return Signal{
			Time:         currCandle.Timestamp,
			Position:     LongBullish,
			Reason:       "bullish engulfing heikin ashi - long bullish",
			StrategyName: s.Name(),
			TriggerPrice: currCandle.Close,
			Candle:       lastCandle, // Use the actual candle, not the HA candle
		}, nil
	}

	// Alternative buy signal: strong bullish Heikin Ashi candle
	// with small or no lower shadow
	// if currHA.Close > currHA.Open && // Bullish candle
	// 	(currHA.Low >= currHA.Open || (currHA.Open-currHA.Low)/currBody < 0.1) && // Small or no lower shadow
	// 	currBody/((currHA.High-currHA.Low)+0.0001) > 0.7 { // Body is at least 70% of the range

	// 	return Signal{
	// 		Time:         currHA.Timestamp,
	// 		Position:     Long,
	// 		Reason:       "strong bullish heiken ashi candle",
	// 		StrategyName: s.Name(),
	// 		TriggerPrice: currHA.Close,
	// 		Candle:       lastCandle, // Use the actual candle, not the HA candle
	// 	}, nil
	// }

	// Sell signal: if current Heiken Ashi candle is bearish
	if currHA.Close < currHA.Open {
		return Signal{
			Time:         currHA.Timestamp,
			Position:     LongBearish,
			Reason:       "bearish heikin ashi - long bearish",
			StrategyName: s.Name(),
			TriggerPrice: currHA.Close,
			Candle:       lastCandle, // Use the actual candle, not the HA candle
		}, nil
	}

	return Signal{
		Time:         lastCandle.Timestamp,
		Position:     Hold,
		Reason:       "no pattern",
		StrategyName: s.Name(),
		TriggerPrice: lastCandle.Close,
		Candle:       lastCandle,
	}, nil
}

// PerformanceMetrics returns performance metrics for the strategy
func (s *EngulfingHeikinAshi) PerformanceMetrics() map[string]float64 {
	return map[string]float64{
		"heikenAshiCount": float64(len(s.heikenAshiCandles)),
		"candleCount":     float64(len(s.candles)),
		"maxHistory":      float64(s.maxHistory),
	}
}

// WarmupPeriod returns the number of candles needed for warm-up
func (s *EngulfingHeikinAshi) WarmupPeriod() int {
	return 2
}
