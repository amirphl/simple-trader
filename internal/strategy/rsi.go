// Package strategy
package strategy

import (
	"context"
	"log"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/indicator"
)

// TODO: Filter on exchange

type Storage interface {
	GetCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]candle.Candle, error)
}

type RSIStrategy struct {
	Symbol     string
	Period     int
	Overbought float64
	Oversold   float64
	prices     []float64

	Storage Storage

	lastRSI     float64
	initialized bool
}

func NewRSIStrategy(symbol string, period int, overbought, oversold float64, storage Storage) *RSIStrategy {
	return &RSIStrategy{
		Symbol:      symbol,
		Period:      period,
		Overbought:  overbought,
		Oversold:    oversold,
		Storage:     storage,
		initialized: false,
	}
}

func (s *RSIStrategy) Name() string { return "RSI" }

func (s *RSIStrategy) OnCandles(candles []candle.Candle) (Signal, error) {
	if len(candles) == 0 {
		return Signal{Action: "hold", Reason: "no candles", StrategyName: "RSI", Time: time.Now().UTC()}, nil
	}

	var lastCandle *candle.Candle
	var filteredCandles []candle.Candle

	// Filter candles for this symbol
	for _, c := range candles {
		if c.Symbol != s.Symbol {
			continue
		}
		filteredCandles = append(filteredCandles, c)
	}

	if len(filteredCandles) == 0 {
		return Signal{Action: "hold", Reason: "no matching candles", StrategyName: "RSI", Time: time.Now().UTC()}, nil
	}

	// Sort candles by timestamp to ensure proper order
	// sortCandlesByTimestamp(filteredCandles)

	// Get the first and last candle for this symbol
	firstCandle := filteredCandles[0]
	lastCandle = &filteredCandles[len(filteredCandles)-1]

	// If this is the first time receiving candles, fetch historical data
	if len(s.prices) == 0 && !s.initialized {
		s.initialized = true // Mark as initialized to avoid fetching again

		// Calculate time range: from 1 year ago to 1 minute before the first candle
		endTime := firstCandle.Timestamp
		startTime := endTime.AddDate(-1, 0, 0) // 1 year before

		ctx := context.Background() // Create a context for database operations // TODO: withTimeout

		log.Printf("[%s RSI] Fetching historical 1m candles from %s to %s\n",
			s.Symbol, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Fetch historical 1m candles
		historicalCandles, err := s.Storage.GetCandles(ctx, s.Symbol, "1m", startTime, endTime)
		if err != nil {
			log.Printf("[%s RSI] Error fetching historical candles: %v\n", s.Symbol, err)
			// Continue with the current candles even if historical fetch fails
		} else if len(historicalCandles) > 0 {
			log.Printf("[%s RSI] Loaded %d historical candles\n", s.Symbol, len(historicalCandles))

			// Sort historical candles by timestamp
			// sortCandlesByTimestamp(historicalCandles)

			// Add historical prices to the prices array
			for _, c := range historicalCandles {
				s.prices = append(s.prices, c.Close)
			}
		}
	}

	// Add current candle prices to the array
	for _, c := range filteredCandles {
		s.prices = append(s.prices, c.Close)
	}

	if len(s.prices) <= s.Period {
		return Signal{Time: lastCandle.Timestamp, Action: "hold", Reason: "warming up", StrategyName: "RSI", TriggerPrice: lastCandle.Close, Candle: *lastCandle}, nil
	}

	// Use the more efficient CalculateLastRSI instead of calculating the entire array
	rsi, err := indicator.CalculateLastRSI(s.prices, s.Period)
	if err != nil {
		return Signal{Time: lastCandle.Timestamp, Action: "hold", Reason: "RSI calculation error", StrategyName: "RSI", TriggerPrice: lastCandle.Close, Candle: *lastCandle}, nil
	}

	s.lastRSI = rsi
	if rsi < s.Oversold {
		return Signal{Time: lastCandle.Timestamp, Action: "buy", Reason: "RSI oversold", StrategyName: "RSI", TriggerPrice: lastCandle.Close, Candle: *lastCandle}, nil
	} else if rsi > s.Overbought {
		return Signal{Time: lastCandle.Timestamp, Action: "sell", Reason: "RSI overbought", StrategyName: "RSI", TriggerPrice: lastCandle.Close, Candle: *lastCandle}, nil
	}

	return Signal{Time: lastCandle.Timestamp, Action: "hold", Reason: "RSI neutral", StrategyName: "RSI", TriggerPrice: lastCandle.Close, Candle: *lastCandle}, nil
}

func (s *RSIStrategy) PerformanceMetrics() map[string]float64 {
	return map[string]float64{}
}

func (s *RSIStrategy) WarmupPeriod() int {
	return s.Period
}

// Helper function to sort candles by timestamp
func sortCandlesByTimestamp(candles []candle.Candle) {
	// Sort candles by timestamp to ensure proper order
	for i := 0; i < len(candles); i++ {
		for j := i + 1; j < len(candles); j++ {
			if candles[j].Timestamp.Before(candles[i].Timestamp) {
				candles[i], candles[j] = candles[j], candles[i]
			}
		}
	}
}
