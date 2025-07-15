// Package strategy
package strategy

// import (
// 	"fmt"

// 	"github.com/amirphl/simple-trader/internal/candle"
// 	"github.com/amirphl/simple-trader/internal/indicator"
// )

// type RSIObOsStrategy struct {
// 	Period     int
// 	Overbought float64
// 	Oversold   float64
// 	lastRSI    float64
// 	lastSignal int // 1 for buy, -1 for sell, 0 for none
// 	prices     []float64
// }

// func NewRSIObOsStrategy(period int, overbought, oversold float64) *RSIObOsStrategy {
// 	return &RSIObOsStrategy{
// 		Period:     period,
// 		Overbought: overbought,
// 		Oversold:   oversold,
// 	}
// }

// // Evaluate takes closes and returns 1 for buy, -1 for sell, 0 for hold
// func (s *RSIObOsStrategy) Evaluate(closes []float64) int {
// 	if len(closes) < s.Period {
// 		return 0
// 	}
// 	rsi := indicator.CalculateRSI(closes, s.Period)
// 	if len(rsi) == 0 {
// 		return 0
// 	}
// 	currentRSI := rsi[len(rsi)-1]
// 	prevRSI := s.lastRSI
// 	s.lastRSI = currentRSI

// 	// Cross above oversold
// 	if prevRSI <= s.Oversold && currentRSI > s.Oversold {
// 		s.lastSignal = 1
// 		return 1 // Buy
// 	}
// 	// Cross below overbought
// 	if prevRSI >= s.Overbought && currentRSI < s.Overbought {
// 		s.lastSignal = -1
// 		return -1 // Sell
// 	}
// 	return 0 // Hold
// }

// // OnCandle implements the Strategy interface
// func (s *RSIObOsStrategy) OnCandle(c any) (Signal, error) {
// 	candleObj, ok := c.(candle.Candle)
// 	if !ok {
// 		return Signal{}, fmt.Errorf("RSIObOsStrategy: expected candle.Candle, got %T", c)
// 	}
// 	s.prices = append(s.prices, candleObj.Close)
// 	if len(s.prices) < s.Period {
// 		return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "warming up"}, nil
// 	}
// 	rsi := indicator.CalculateRSI(s.prices, s.Period)
// 	if len(rsi) == 0 {
// 		return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "no RSI"}, nil
// 	}
// 	currentRSI := rsi[len(rsi)-1]
// 	prevRSI := s.lastRSI
// 	s.lastRSI = currentRSI
// 	if prevRSI <= s.Oversold && currentRSI > s.Oversold {
// 		s.lastSignal = 1
// 		return Signal{Time: candleObj.Timestamp, Action: "buy", Reason: fmt.Sprintf("RSI cross above oversold (%.2f)", s.Oversold)}, nil
// 	}
// 	if prevRSI >= s.Overbought && currentRSI < s.Overbought {
// 		s.lastSignal = -1
// 		return Signal{Time: candleObj.Timestamp, Action: "sell", Reason: fmt.Sprintf("RSI cross below overbought (%.2f)", s.Overbought)}, nil
// 	}
// 	return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "RSI neutral"}, nil
// }

// func (s *RSIObOsStrategy) PerformanceMetrics() map[string]float64 {
// 	return map[string]float64{}
// }

// func (s *RSIObOsStrategy) WarmupPeriod() int {
// 	return s.Period
// }

// func (s *RSIObOsStrategy) Name() string {
// 	return "RSI Overbought/Oversold"
// }
