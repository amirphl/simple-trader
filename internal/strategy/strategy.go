package strategy

import (
	"time"
)

// Strategy is the interface for all trading strategies.
type Strategy interface {
	Name() string
	OnCandle(candle any) (Signal, error)    // Accepts a candle, returns a signal
	PerformanceMetrics() map[string]float64 // Returns performance metrics after backtest
	WarmupPeriod() int                      // Returns the number of candles needed for warm-up
}

type Signal struct {
	Time   time.Time
	Action string // "buy", "sell", "hold"
	Reason string // indicator/pattern/price action
}
