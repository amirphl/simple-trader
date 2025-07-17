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
}

func NewEngulfingHeikinAshi(symbol string, storage Storage) *EngulfingHeikinAshi {
	return &EngulfingHeikinAshi{
		symbol:      symbol,
		Storage:     storage,
		initialized: false,
	}
}

// Name returns the name of the strategy
func (s *EngulfingHeikinAshi) Name() string { return "Engulfing Heikin Ashi" }

// Symbol returns the symbol this strategy is configured for
func (s *EngulfingHeikinAshi) Symbol() string { return s.symbol }

// Timeframe returns the timeframe this strategy is configured for
func (s *EngulfingHeikinAshi) Timeframe() string { return "1h" }

// OnCandles processes new candles and generates trading signals
func (s *EngulfingHeikinAshi) OnCandles(ctx context.Context, oneHourCandles []candle.Candle) (Signal, error) {
	log.Printf("Strategy | [%s Engulfing Heikin Ashi] Received %d 1h candles", s.symbol, len(oneHourCandles))

	// TODO: Handle duplicate candles
	// TODO: Handle missing candles from last candle in db to first received candle (somewhat possible because of UTC)
	// TODO: Handle missing candles inside oneHourCandles, validate, sort, truncate (impossible)
	if len(oneHourCandles) == 0 {
		log.Printf("Strategy | [%s Engulfing Heikin Ashi] No candles received", s.symbol)
		return Signal{
			Time:         time.Now().UTC(),
			Action:       "hold",
			Reason:       "no candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
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
		log.Printf("Strategy | [%s Engulfing Heikin Ashi] No matching candles", s.symbol)
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

	// Initialize with historical data if needed
	if len(s.candles) == 0 && !s.initialized {
		s.initialized = true // Mark as initialized to avoid fetching again

		// Calculate time range: from 1 year ago to 1 hour before the first candle
		endTime := filteredCandles[0].Timestamp.Truncate(time.Hour)
		startTime := endTime.AddDate(-1, 0, 0) // 1 year before

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

	lastCandle = &s.candles[len(s.candles)-1]
	// Check if we have enough data for Engulfing Heikin Ashi calculation
	if len(s.heikenAshiCandles) < 2 {
		log.Printf("Strategy | [%s Engulfing Heikin Ashi] Not enough Heiken Ashi candles for pattern detection", s.symbol)
		return Signal{
			Time:         lastCandle.Timestamp,
			Action:       "hold",
			Reason:       "warming up",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       *lastCandle,
		}, nil
	}

	prevHA := s.heikenAshiCandles[len(s.heikenAshiCandles)-2]
	currHA := s.heikenAshiCandles[len(s.heikenAshiCandles)-1]

	// Buy signal: bullish engulfing pattern
	// current.open < previous.close (0.02 % tolerance is acceptable)
	// && current.close > previous.open
	// && current body (close - open) / previous body (open - close) >= 1.8

	// Tolerance for open < previous.close
	openCloseTolerance := 0.0002 * prevHA.Close // 0.02%
	openBelowPrevClose := currHA.Open < prevHA.Close || abs(currHA.Open-prevHA.Close) <= openCloseTolerance
	closeAbovePrevOpen := currHA.Close > prevHA.Open
	currBody := abs(currHA.Close - currHA.Open)
	prevBody := abs(prevHA.Open - prevHA.Close)
	bodyRatio := 0.0
	if prevBody != 0 {
		bodyRatio = currBody / prevBody
	}

	if openBelowPrevClose && closeAbovePrevOpen && bodyRatio >= 1.8 {
		log.Printf("Strategy | [%s Engulfing Heikin Ashi] BUY signal: bullish engulfing detected", s.symbol)
		return Signal{
			Time:         currHA.Timestamp,
			Action:       "buy",
			Reason:       "bullish engulfing heiken ashi",
			StrategyName: s.Name(),
			TriggerPrice: currHA.Close,
			Candle:       currHA,
		}, nil
	}

	// Sell signal: if current Heiken Ashi candle is bearish
	if currHA.Close < currHA.Open {
		log.Printf("Strategy | [%s Engulfing Heikin Ashi] SELL signal: bearish heiken ashi candle", s.symbol)
		return Signal{
			Time:         currHA.Timestamp,
			Action:       "sell",
			Reason:       "bearish heiken ashi candle",
			StrategyName: s.Name(),
			TriggerPrice: currHA.Close,
			Candle:       currHA,
		}, nil
	}

	log.Printf("Strategy | [%s Engulfing Heikin Ashi] Holding", s.symbol)
	return Signal{
		Time:         lastCandle.Timestamp,
		Action:       "hold",
		Reason:       "no pattern",
		StrategyName: s.Name(),
		TriggerPrice: lastCandle.Close,
		Candle:       *lastCandle,
	}, nil
}

// PerformanceMetrics returns performance metrics for the strategy
func (s *EngulfingHeikinAshi) PerformanceMetrics() map[string]float64 {
	return map[string]float64{
		"heikenAshiCount": float64(len(s.heikenAshiCandles)),
		"candleCount":     float64(len(s.candles)),
	}
}

// WarmupPeriod returns the number of candles needed for warm-up
func (s *EngulfingHeikinAshi) WarmupPeriod() int {
	return 2
}
