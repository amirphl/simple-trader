package strategy

import (
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
)

// Strategy is the interface for all trading strategies.
type Strategy interface {
	Name() string
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
