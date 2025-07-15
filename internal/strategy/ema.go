// Package strategy
package strategy

// import (
// 	"fmt"

// 	"github.com/amirphl/simple-trader/internal/candle"
// )

// type EMACrossoverStrategy struct {
// 	Fast       int
// 	Slow       int
// 	prices     []float64
// 	fastEMA    []float64
// 	slowEMA    []float64
// 	lastSignal string
// }

// func NewEMACrossoverStrategy(fast, slow int) *EMACrossoverStrategy {
// 	return &EMACrossoverStrategy{Fast: fast, Slow: slow}
// }

// func (s *EMACrossoverStrategy) Name() string { return "EMA Crossover" }

// func (s *EMACrossoverStrategy) OnCandle(c any) (Signal, error) {
// 	candleObj, ok := c.(candle.Candle)
// 	if !ok {
// 		return Signal{}, fmt.Errorf("EMACrossoverStrategy: expected candle.Candle, got %T", c)
// 	}
// 	s.prices = append(s.prices, candleObj.Close)
// 	if len(s.prices) < s.Slow {
// 		return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "warming up"}, nil
// 	}
// 	fast := ema(s.prices, s.Fast, s.fastEMA)
// 	slow := ema(s.prices, s.Slow, s.slowEMA)
// 	s.fastEMA = append(s.fastEMA, fast)
// 	s.slowEMA = append(s.slowEMA, slow)
// 	var action string
// 	if len(s.fastEMA) > 1 && len(s.slowEMA) > 1 {
// 		prevFast := s.fastEMA[len(s.fastEMA)-2]
// 		prevSlow := s.slowEMA[len(s.slowEMA)-2]
// 		if prevFast < prevSlow && fast > slow {
// 			action = "buy"
// 		} else if prevFast > prevSlow && fast < slow {
// 			action = "sell"
// 		} else {
// 			action = "hold"
// 		}
// 	} else {
// 		action = "hold"
// 	}
// 	s.lastSignal = action
// 	return Signal{Time: candleObj.Timestamp, Action: action, Reason: "EMA crossover"}, nil
// }

// func ema(prices []float64, period int, prevEMA []float64) float64 {
// 	k := 2.0 / float64(period+1)
// 	if len(prices) < period {
// 		return 0
// 	}
// 	if len(prevEMA) == 0 {
// 		sum := 0.0
// 		for i := len(prices) - period; i < len(prices); i++ {
// 			sum += prices[i]
// 		}
// 		return sum / float64(period)
// 	}
// 	return prices[len(prices)-1]*k + prevEMA[len(prevEMA)-1]*(1-k)
// }

// func (s *EMACrossoverStrategy) PerformanceMetrics() map[string]float64 {
// 	return map[string]float64{}
// }

// func (s *EMACrossoverStrategy) WarmupPeriod() int {
// 	return s.Slow
// }
