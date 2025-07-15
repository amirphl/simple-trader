package strategy

import (
	"fmt"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/indicator"
)

type MACDStrategy struct {
	Fast       int
	Slow       int
	Signal     int
	prices     []float64
	macd       []float64
	signalLine []float64
}

func NewMACDStrategy(fast, slow, signal int) *MACDStrategy {
	return &MACDStrategy{Fast: fast, Slow: slow, Signal: signal}
}

func (s *MACDStrategy) Name() string { return "MACD" }

func (s *MACDStrategy) OnCandle(c any) (Signal, error) {
	candleObj, ok := c.(candle.Candle)
	if !ok {
		return Signal{}, fmt.Errorf("MACDStrategy: expected candle.Candle, got %T", c)
	}
	s.prices = append(s.prices, candleObj.Close)
	macd, sig, _ := indicator.CalculateMACD(s.prices, s.Fast, s.Slow, s.Signal)
	s.macd = macd
	s.signalLine = sig
	if len(macd) < 2 || len(sig) < 2 {
		return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "warming up"}, nil
	}
	prevMACD := macd[len(macd)-2]
	prevSig := sig[len(sig)-2]
	curMACD := macd[len(macd)-1]
	curSig := sig[len(sig)-1]
	if prevMACD < prevSig && curMACD > curSig {
		return Signal{Time: candleObj.Timestamp, Action: "buy", Reason: "MACD crossover up"}, nil
	} else if prevMACD > prevSig && curMACD < curSig {
		return Signal{Time: candleObj.Timestamp, Action: "sell", Reason: "MACD crossover down"}, nil
	}
	return Signal{Time: candleObj.Timestamp, Action: "hold", Reason: "MACD neutral"}, nil
}

func (s *MACDStrategy) PerformanceMetrics() map[string]float64 {
	return map[string]float64{}
}

func (s *MACDStrategy) WarmupPeriod() int {
	return s.Slow
}
