// Package strategy
package strategy

import (
	"fmt"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/indicator"
)

type RSIStrategy struct {
	Period     int
	Overbought float64
	Oversold   float64
	prices     []float64
	lastRSI    float64
}

func NewRSIStrategy(period int, overbought, oversold float64) *RSIStrategy {
	return &RSIStrategy{Period: period, Overbought: overbought, Oversold: oversold}
}

func (s *RSIStrategy) Name() string { return "RSI" }

func (s *RSIStrategy) OnCandle(c any) (Signal, error) {
	candleObj, ok := c.(candle.Candle)
	if !ok {
		return Signal{}, fmt.Errorf("RSIStrategy: expected candle.Candle, got %T", c)
	}
	
	s.prices = append(s.prices, candleObj.Close)
	if len(s.prices) <= s.Period {
		return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "warming up"}, nil
	}
	
	// Use the more efficient CalculateLastRSI instead of calculating the entire array
	rsi, err := indicator.CalculateLastRSI(s.prices, s.Period)
	if err != nil {
		return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "RSI calculation error"}, nil
	}
	
	s.lastRSI = rsi
	if rsi < s.Oversold {
		return Signal{Time: candleObj.Timestamp, Action: "buy", Reason: "RSI oversold"}, nil
	} else if rsi > s.Overbought {
		return Signal{Time: candleObj.Timestamp, Action: "sell", Reason: "RSI overbought"}, nil
	}
	
	return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "RSI neutral"}, nil
}

func (s *RSIStrategy) PerformanceMetrics() map[string]float64 {
	return map[string]float64{}
}

func (s *RSIStrategy) WarmupPeriod() int {
	return s.Period
}