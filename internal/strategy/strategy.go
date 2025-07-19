package strategy

import (
	"context"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/config"
)

// Storage interface defines methods for retrieving candle data
type Storage interface {
	GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]candle.Candle, error)
}

// Strategy is the interface for all trading strategies.
type Strategy interface {
	Name() string
	Symbol() string
	Timeframe() string
	OnCandles(ctx context.Context, oneMinCandles []candle.Candle) (Signal, error) // Accepts candles, returns a signal
	PerformanceMetrics() map[string]float64                                       // Returns performance metrics after backtest
	WarmupPeriod() int                                                            // Returns the number of candles needed for warm-up
}

type Position int8

const (
	LongBullish  Position = 1
	LongBearish  Position = 2
	ShortBullish Position = -2
	ShortBearish Position = -1
	Hold         Position = 0
)

type Signal struct {
	Time         time.Time      `json:"time"`
	Position     Position       `json:"position"`
	Reason       string         `json:"reason"`        // indicator/pattern/price action
	StrategyName string         `json:"strategy_name"` // TODO: FILL
	TriggerPrice float64        `json:"trigger_price"` // TODO: FILL
	Candle       *candle.Candle `json:"candle"`        // TODO: FILL
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
			strat = NewRSIStrategy("ADA-USDT", 14, 70, 30, storage) // TODO: Make it configutable.
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
		case "Engulfing Heikin Ashi":
			strat = NewEngulfingHeikinAshi("DOGE-USDT", storage)
		default:
			// strat = strategy.NewSMACrossoverStrategy(10, 30)
		}

		strats = append(strats, strat)
	}

	return strats
}
