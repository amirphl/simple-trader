// Package strategy
package strategy

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/indicator"
	"github.com/amirphl/simple-trader/internal/utils"
)

type StochasticHeikinAshi struct {
	symbol            string
	candles           []candle.Candle
	heikenAshiCandles []candle.Candle
	stochasticResult  *indicator.StochasticResult

	Storage db.Storage

	initialized bool
	maxHistory  int // Maximum number of candles to keep in memory

	// Stochastic parameters
	periodK int
	smoothK int
	periodD int

	// Strategy parameters
	overbought float64
	oversold   float64

	// State machine for managing trading positions
	stateMachine *StateMachine
}

func NewStochasticHeikinAshi(symbol string, storage db.Storage) *StochasticHeikinAshi {
	return &StochasticHeikinAshi{
		symbol:  symbol,
		Storage: storage,
		stochasticResult: &indicator.StochasticResult{
			K: []float64{},
			D: []float64{},
		},
		initialized:  false,
		maxHistory:   100, // Keep only the last 100 candles in memory
		periodK:      24,
		smoothK:      10,
		periodD:      3,
		overbought:   80.0,
		oversold:     20.0,
		stateMachine: NewStateMachine(symbol),
	}
}

// Name returns the name of the strategy
func (s *StochasticHeikinAshi) Name() string { return "Stochastic Heikin Ashi" }

// Symbol returns the symbol this strategy is configured for
func (s *StochasticHeikinAshi) Symbol() string { return s.symbol }

// Timeframe returns the timeframe this strategy is configured for
func (s *StochasticHeikinAshi) Timeframe() string { return "1h" }

func (s *StochasticHeikinAshi) PeriodK() int { return s.periodK }

func (s *StochasticHeikinAshi) SmoothK() int { return s.smoothK }

func (s *StochasticHeikinAshi) PeriodD() int { return s.periodD }

// trimCandles ensures we don't keep too many candles in memory
func (s *StochasticHeikinAshi) trimCandles() {
	if len(s.candles) > s.maxHistory {
		excess := len(s.candles) - s.maxHistory
		s.candles = s.candles[excess:]
	}
	if len(s.heikenAshiCandles) > s.maxHistory {
		excess := len(s.heikenAshiCandles) - s.maxHistory
		s.heikenAshiCandles = s.heikenAshiCandles[excess:]
	}
	if len(s.stochasticResult.K) > s.maxHistory {
		excess := len(s.stochasticResult.K) - s.maxHistory
		s.stochasticResult.K = s.stochasticResult.K[excess:]
	}
	if len(s.stochasticResult.D) > s.maxHistory {
		excess := len(s.stochasticResult.D) - s.maxHistory
		s.stochasticResult.D = s.stochasticResult.D[excess:]
	}
}

// isHeikinAshiBullish checks if a Heikin Ashi candle is bullish
func (s *StochasticHeikinAshi) isHeikinAshiBullish(haCandle candle.Candle) bool {
	return haCandle.Close > haCandle.Open
}

// isHeikinAshiBearish checks if a Heiken Ashi candle is bearish
func (s *StochasticHeikinAshi) isHeikinAshiBearish(haCandle candle.Candle) bool {
	return haCandle.Close < haCandle.Open
}

// isStochasticOversold checks if the stochastic is below oversold level
func (s *StochasticHeikinAshi) isStochasticOversold() bool {
	if len(s.stochasticResult.K) == 0 {
		return false
	}

	latestIdx := len(s.stochasticResult.K) - 1
	kValue := s.stochasticResult.K[latestIdx]

	if math.IsNaN(kValue) {
		return false
	}

	return kValue < s.oversold
}

// isStochasticOverbought checks if the stochastic is above overbought level
func (s *StochasticHeikinAshi) isStochasticOverbought() bool {
	if len(s.stochasticResult.K) == 0 {
		return false
	}

	latestIdx := len(s.stochasticResult.K) - 1
	kValue := s.stochasticResult.K[latestIdx]

	if math.IsNaN(kValue) {
		return false
	}

	return kValue > s.overbought
}

// OnCandles processes new candles and generates trading signals
func (s *StochasticHeikinAshi) OnCandles(ctx context.Context, oneHourCandles []candle.Candle) (Signal, error) {
	if len(oneHourCandles) == 0 {
		return Signal{
			Time:         time.Now().UTC(),
			Position:     Hold,
			Reason:       "no candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
			Candle:       nil,
		}, nil
	}

	var lastCandle *candle.Candle
	if len(s.candles) > 0 {
		lastCandle = &s.candles[len(s.candles)-1]
	}

	// Filter candles for this symbol
	var filteredCandles []candle.Candle
	for _, c := range oneHourCandles {
		if c.Symbol == s.symbol && c.Timeframe == "1h" && (lastCandle == nil || c.Timestamp.After(lastCandle.Timestamp)) {
			filteredCandles = append(filteredCandles, c)
		}
	}

	if len(filteredCandles) == 0 {
		return Signal{
			Time:         time.Now().UTC(),
			Position:     Hold,
			Reason:       "no matching candles",
			StrategyName: s.Name(),
			TriggerPrice: 0,
			Candle:       nil,
		}, nil
	}

	// Sort candles by timestamp to ensure proper order
	sort.Slice(filteredCandles, func(i, j int) bool {
		return filteredCandles[i].Timestamp.Before(filteredCandles[j].Timestamp)
	})

	// Initialize with historical data if needed
	if len(s.candles) == 0 && !s.initialized {
		s.initialized = true // Mark as initialized to avoid fetching again

		// Calculate time range: from 24 hours ago to 1 hour before the first candle
		endTime := filteredCandles[0].Timestamp.Truncate(time.Hour)
		startTime := endTime.Add(-24 * time.Hour) // Just 24 hours should be enough

		utils.GetLogger().Printf("Strategy | [%s Stochastic Heikin Ashi] Fetching historical 1h candles from %s to %s\n",
			s.symbol, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Fetch historical 1h candles
		historicalCandles, err := s.Storage.GetCandles(ctx, s.symbol, "1h", "", startTime, endTime)
		if err != nil {
			utils.GetLogger().Printf("Strategy | [%s Stochastic Heikin Ashi] Error fetching historical candles from database: %v\n", s.symbol, err)
			// Continue with the current candles even if historical fetch fails
		} else if len(historicalCandles) > 0 {
			utils.GetLogger().Printf("Strategy | [%s Stochastic Heikin Ashi] Loaded %d historical candles from database\n", s.symbol, len(historicalCandles))

			// Sort historical candles by timestamp
			sort.Slice(historicalCandles, func(i, j int) bool {
				return historicalCandles[i].Timestamp.Before(historicalCandles[j].Timestamp)
			})

			cc := candle.DBCandlesToCandles(historicalCandles)
			s.candles = append(s.candles, cc...)

			s.heikenAshiCandles = candle.GenerateHeikenAshiCandles(cc)

			stochasticResult, err := indicator.CalculateStochastic(cc, s.periodK, s.smoothK, s.periodD)
			if err != nil {
				utils.GetLogger().Printf("Strategy | [%s Stochastic Heikin Ashi] Error calculating stochastic: %v\n", s.symbol, err)
			} else {
				s.stochasticResult = stochasticResult
			}
		}
	}

	var lastHeikenAshiCandle *candle.Candle
	if len(s.heikenAshiCandles) > 0 {
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Add current candle prices to the array
	for _, c := range filteredCandles {
		kValue, dValue, err := indicator.UpdateStochastic(s.stochasticResult, s.candles, c, s.periodK, s.smoothK, s.periodD)
		if err != nil {
			utils.GetLogger().Printf("Strategy | [%s Stochastic Heikin Ashi] Error updating stochastic: %v\n", s.symbol, err)
			// Continue with the current candles even if stochastic update fails
		} else {
			s.stochasticResult.K = append(s.stochasticResult.K, kValue)
			s.stochasticResult.D = append(s.stochasticResult.D, dValue)
		}

		s.candles = append(s.candles, c) // NOTE: modified inside UpdateStochastic

		s.heikenAshiCandles = append(s.heikenAshiCandles, candle.GenerateNextHeikenAshiCandle(lastHeikenAshiCandle, c))
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Trim candles to prevent memory leaks
	s.trimCandles()

	// Get the latest regular candle for the signal
	lastCandle = &s.candles[len(s.candles)-1]

	// Check if we have enough data for Stochastic calculation
	minRequiredCandles := s.periodK + s.smoothK + s.periodD - 2
	if len(s.candles) < minRequiredCandles {
		return Signal{
			Time:         lastCandle.Timestamp,
			Position:     Hold,
			Reason:       "warming up",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	// Get the latest Stochastic values
	latestIdx := len(s.stochasticResult.K) - 1
	if latestIdx < 0 {
		return Signal{
			Time:         lastCandle.Timestamp,
			Position:     Hold,
			Reason:       "no stochastic data",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	kValue := s.stochasticResult.K[latestIdx]
	dValue := s.stochasticResult.D[latestIdx]

	// Check if we have valid Stochastic values
	if math.IsNaN(kValue) || math.IsNaN(dValue) {
		return Signal{
			Time:         lastCandle.Timestamp,
			Position:     Hold,
			Reason:       "stochastic values not ready",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	// Get the latest Heikin Ashi candle
	currHA := s.heikenAshiCandles[len(s.heikenAshiCandles)-1]

	// State Machine Logic
	currentState := s.stateMachine.GetCurrentState()

	switch currentState {
	case NoPositionState:
		// Check for buy signal: stochastic oversold + bullish crossover + bullish Heikin Ashi
		if s.isStochasticOversold() && kValue > dValue && s.isHeikinAshiBullish(currHA) {
			s.stateMachine.TransitionTo(LongBullishState, "stochastic_oversold_bullish_crossover_bullish_ha", LongBullish, "buy signal triggered")
			return Signal{
				Time:         lastCandle.Timestamp,
				Position:     LongBullish,
				Reason:       "stochastic oversold + bullish crossover + bullish heikin ashi",
				StrategyName: s.Name(),
				TriggerPrice: lastCandle.Close,
				Candle:       lastCandle,
			}, nil
		}

	case LongBullishState:
		// Check for sell signal: stochastic overbought + bearish crossover
		if s.isStochasticOverbought() && kValue < dValue {
			s.stateMachine.TransitionTo(NoPositionState, "stochastic_overbought_bearish_crossover", LongBearish, "exit signal triggered")
			return Signal{
				Time:         lastCandle.Timestamp,
				Position:     LongBearish,
				Reason:       "stochastic overbought + bearish crossover",
				StrategyName: s.Name(),
				TriggerPrice: lastCandle.Close,
				Candle:       lastCandle,
			}, nil
		}
	}

	// Default: Hold
	return Signal{
		Time:         lastCandle.Timestamp,
		Position:     Hold,
		Reason:       "no signal conditions met",
		StrategyName: s.Name(),
		TriggerPrice: lastCandle.Close,
		Candle:       lastCandle,
	}, nil
}

// PerformanceMetrics returns performance metrics for the strategy
func (s *StochasticHeikinAshi) PerformanceMetrics() map[string]float64 {
	stateMetrics := s.stateMachine.GetStateMetrics()

	metrics := map[string]float64{
		"heikenAshiCount":  float64(len(s.heikenAshiCandles)),
		"candleCount":      float64(len(s.candles)),
		"maxHistory":       float64(s.maxHistory),
		"periodK":          float64(s.periodK),
		"smoothK":          float64(s.smoothK),
		"periodD":          float64(s.periodD),
		"overbought":       s.overbought,
		"oversold":         s.oversold,
		"currentState":     float64(len(string(s.stateMachine.GetCurrentState()))), // Convert state to numeric for metrics
		"totalTransitions": float64(stateMetrics["total_transitions"].(int)),
		"stateHistorySize": float64(stateMetrics["history_size"].(int)),
	}

	return metrics
}

// WarmupPeriod returns the number of candles needed for warm-up
func (s *StochasticHeikinAshi) WarmupPeriod() int {
	// Need enough candles for Stochastic calculation
	return s.periodK + s.smoothK + s.periodD - 2
}
