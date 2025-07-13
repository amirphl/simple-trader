package strategy

import (
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/config"
)

// Strategy is the interface for all trading strategies.
type Strategy interface {
	Name() string
	Symbol() string
	OnCandles([]candle.Candle) (Signal, error) // Accepts candles, returns a signal
	PerformanceMetrics() map[string]float64    // Returns performance metrics after backtest
	WarmupPeriod() int                         // Returns the number of candles needed for warm-up
}

type Signal struct {
	Time         time.Time
	Action       string        // "buy", "sell", "hold"
	Reason       string        // indicator/pattern/price action
	StrategyName string        // TODO: FILL
	TriggerPrice float64       // TODO: FILL
	Candle       candle.Candle // TODO: FILL
}

// TODO:
func New(cfg config.Config, storage Storage) []Strategy {
	strats := []Strategy{}

	for _, stratName := range cfg.Strategies {
		var strat Strategy

		switch stratName {
		case "ema":
			// strat = strategy.NewEMACrossoverStrategy(10, 30)
		case "rsi":
			strat = NewRSIStrategy("DOGE-USDT", 14, 70, 30, storage) // TODO: Make it configutable.
		case "macd":
			// strat = strategy.NewMACDStrategy(12, 26, 9)
		case "composite":
			// strat = strategy.NewCompositeStrategy(
			// 	strategy.NewSMACrossoverStrategy(10, 30),
			// 	strategy.NewRSIStrategy(14, 70, 30),
			// 	strategy.NewMACDStrategy(12, 26, 9),
			// )
		case "rsi-obos":
			// strat = strategy.NewRSIObOsStrategy(14, 70, 30)
		default:
			// strat = strategy.NewSMACrossoverStrategy(10, 30)
		}

		strats = append(strats, strat)
	}

	return strats
}
