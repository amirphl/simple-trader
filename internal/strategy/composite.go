// Package strategy
package strategy

// import (
// 	"context"

// 	"github.com/amirphl/simple-trader/internal/candle"
// )

// type CompositeStrategy struct {
// 	strategies []Strategy
// }

// func NewCompositeStrategy(strategies ...Strategy) *CompositeStrategy {
// 	return &CompositeStrategy{strategies: strategies}
// }

// func (s *CompositeStrategy) Name() string { return "Composite" }

// func (s *CompositeStrategy) OnCandle(candles []candle.Candle) (Signal, error) {
// 	size := len(candles)
// 	buy, sell := 0, 0
// 	for _, strat := range s.strategies {
// 		sig, _ := strat.OnCandles(context.Background(), candles) // TODO: context.Background() is not good, we need to pass a context that is cancelled when the strategy is stopped
// 		if sig.Action == "buy" {
// 			buy++
// 		} else if sig.Action == "sell" {
// 			sell++
// 		}
// 	}
// 	if buy == len(s.strategies) {
// 		return Signal{Time: candles[size-1].Timestamp, Action: "buy", Reason: "all strategies buy"}, nil
// 	} else if sell == len(s.strategies) {
// 		return Signal{Time: candles[size-1].Timestamp, Action: "sell", Reason: "all strategies sell"}, nil
// 	}
// 	return Signal{Time: candles[size-1].Timestamp, Action: "hold", Reason: "mixed/hold"}, nil
// }

// func (s *CompositeStrategy) PerformanceMetrics() map[string]float64 {
// 	return map[string]float64{}
// }

// func (s *CompositeStrategy) WarmupPeriod() int {
// 	max := 0
// 	for _, strat := range s.strategies {
// 		if w := strat.WarmupPeriod(); w > max {
// 			max = w
// 		}
// 	}
// 	return max
// }
