package strategy

import (
	"context"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/strategy/engulfing_heikin_ashi"
	"github.com/amirphl/simple-trader/internal/strategy/four_stochastic_heikin_ashi"
	"github.com/amirphl/simple-trader/internal/strategy/four_stochastic_heikin_ashi_scaled"
	"github.com/amirphl/simple-trader/internal/strategy/signal"
	"github.com/amirphl/simple-trader/internal/strategy/stochastic_heikin_ashi"
)

// Strategy is the interface for all trading strategies.
type Strategy interface {
	Name() string
	Symbol() string
	Timeframe() string
	OnCandles(ctx context.Context, oneMinCandles []candle.Candle) (signal.Signal, error) // Accepts candles, returns a signal
	PerformanceMetrics() map[string]float64                                              // Returns performance metrics after backtest
	WarmupPeriod() int                                                                   // Returns the number of candles needed for warm-up
}

// TODO:
func New(cfg config.Config, storage db.Storage) []Strategy {
	strats := []Strategy{}

	for _, stratName := range cfg.Strategies {
		switch stratName {
		case "Engulfing Heikin Ashi":
			for _, symbol := range cfg.Symbols {
				strats = append(strats, engulfing_heikin_ashi.NewEngulfingHeikinAshi(symbol, storage))
			}
		case "Stochastic Heikin Ashi":
			for _, symbol := range cfg.Symbols {
				strats = append(strats, stochastic_heikin_ashi.NewStochasticHeikinAshi(symbol, storage))
			}
		case "Four Stochastic Heikin Ashi":
			for _, symbol := range cfg.Symbols {
				strats = append(strats, four_stochastic_heikin_ashi.NewFourStochasticHeikinAshi(symbol, storage))
			}
		case "Four Stochastic Heikin Ashi Scaled":
			for _, symbol := range cfg.Symbols {
				strats = append(strats, four_stochastic_heikin_ashi_scaled.NewFourStochasticHeikinAshiScaled(symbol, storage))
			}
		default:
			// strat = strategy.NewSMACrossoverStrategy(10, 30)
		}
	}

	return strats
}
