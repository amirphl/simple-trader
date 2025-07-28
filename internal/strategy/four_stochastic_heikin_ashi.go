// Package strategy
package strategy

import (
	"context"
	"log"
	"math"
	"sort"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/indicator"
)

// FourStochasticHeikinAshi implements a trading strategy using four stochastic indicators
// with Heiken Ashi confirmation for spot trading
type FourStochasticHeikinAshi struct {
	symbol            string
	candles           []candle.Candle
	heikenAshiCandles []candle.Candle

	// Four stochastic indicators with different configurations
	stochastic20  *indicator.StochasticResult // Red - length: 20, k: 3, d: 3
	stochastic40  *indicator.StochasticResult // Blue - length: 40, k: 3, d: 3
	stochastic60  *indicator.StochasticResult // Orange - length: 60, k: 3, d: 3
	stochastic100 *indicator.StochasticResult // Green - length: 100, k: 3, d: 3

	Storage Storage

	initialized bool
	maxHistory  int // Maximum number of candles to keep in memory

	// Strategy parameters
	overbought float64
	oversold   float64

	// State machine for managing trading positions
	stateMachine *StateMachine
}

// NewFourStochasticHeikinAshi creates a new four stochastic strategy
func NewFourStochasticHeikinAshi(symbol string, storage Storage) *FourStochasticHeikinAshi {
	return &FourStochasticHeikinAshi{
		symbol:  symbol,
		Storage: storage,
		stochastic20: &indicator.StochasticResult{
			K: []float64{},
			D: []float64{},
		},
		stochastic40: &indicator.StochasticResult{
			K: []float64{},
			D: []float64{},
		},
		stochastic60: &indicator.StochasticResult{
			K: []float64{},
			D: []float64{},
		},
		stochastic100: &indicator.StochasticResult{
			K: []float64{},
			D: []float64{},
		},
		initialized:  false,
		maxHistory:   200, // Keep more history for the longer stochastic
		overbought:   80.0,
		oversold:     20.0,
		stateMachine: NewStateMachine(symbol),
	}
}

// Name returns the name of the strategy
func (s *FourStochasticHeikinAshi) Name() string { return "Four Stochastic Heikin Ashi" }

// Symbol returns the symbol this strategy is configured for
func (s *FourStochasticHeikinAshi) Symbol() string { return s.symbol }

// Timeframe returns the timeframe this strategy is configured for
func (s *FourStochasticHeikinAshi) Timeframe() string { return "15m" }

// trimCandles ensures we don't keep too many candles in memory
func (s *FourStochasticHeikinAshi) trimCandles() {
	if len(s.candles) > s.maxHistory {
		excess := len(s.candles) - s.maxHistory
		s.candles = s.candles[excess:]
	}
	if len(s.heikenAshiCandles) > s.maxHistory {
		excess := len(s.heikenAshiCandles) - s.maxHistory
		s.heikenAshiCandles = s.heikenAshiCandles[excess:]
	}

	// Trim stochastic arrays
	for _, stoch := range []*indicator.StochasticResult{s.stochastic20, s.stochastic40, s.stochastic60, s.stochastic100} {
		if len(stoch.K) > s.maxHistory {
			excess := len(stoch.K) - s.maxHistory
			stoch.K = stoch.K[excess:]
			stoch.D = stoch.D[excess:]
		}
	}
}

// isHeikinAshiBullish checks if a Heiken Ashi candle is bullish
func (s *FourStochasticHeikinAshi) isHeikinAshiBullish(haCandle candle.Candle) bool {
	return haCandle.Close > haCandle.Open
}

// isHeikinAshiBearish checks if a Heiken Ashi candle is bearish
func (s *FourStochasticHeikinAshi) isHeikinAshiBearish(haCandle candle.Candle) bool {
	return haCandle.Close < haCandle.Open
}

// areAllStochasticsOversold checks if all four stochastic indicators are below oversold level
func (s *FourStochasticHeikinAshi) areAllStochasticsOversold() bool {
	if len(s.stochastic20.K) == 0 || len(s.stochastic40.K) == 0 ||
		len(s.stochastic60.K) == 0 || len(s.stochastic100.K) == 0 {
		return false
	}

	latestIdx := len(s.stochastic20.K) - 1

	// Check if all stochastic K values are below oversold level
	k20 := s.stochastic20.K[latestIdx]
	k40 := s.stochastic40.K[latestIdx]
	k60 := s.stochastic60.K[latestIdx]
	k100 := s.stochastic100.K[latestIdx]

	// Check for NaN values
	if math.IsNaN(k20) || math.IsNaN(k40) || math.IsNaN(k60) || math.IsNaN(k100) {
		return false
	}

	return k20 < s.oversold && k40 < s.oversold && k60 < s.oversold && k100 < s.oversold
}

// isGreenStochasticOverbought checks if the green stochastic (length 100) is overbought
func (s *FourStochasticHeikinAshi) isGreenStochasticOverbought() bool {
	if len(s.stochastic100.K) == 0 {
		return false
	}

	latestIdx := len(s.stochastic100.K) - 1
	k100 := s.stochastic100.K[latestIdx]

	if math.IsNaN(k100) {
		return false
	}

	return k100 > s.overbought
}

// isGreenStochasticAbove20 checks if the green stochastic (length 100) is above 20
func (s *FourStochasticHeikinAshi) isGreenStochasticAbove20() bool {
	if len(s.stochastic100.K) == 0 {
		return false
	}

	latestIdx := len(s.stochastic100.K) - 1
	k100 := s.stochastic100.K[latestIdx]

	if math.IsNaN(k100) {
		return false
	}

	return k100 > 20.0
}

// isGreenStochasticBelow20 checks if the green stochastic (length 100) is below 20
func (s *FourStochasticHeikinAshi) isGreenStochasticBelow20() bool {
	if len(s.stochastic100.K) == 0 {
		return false
	}

	latestIdx := len(s.stochastic100.K) - 1
	k100 := s.stochastic100.K[latestIdx]

	if math.IsNaN(k100) {
		return false
	}

	return k100 < 20.0
}

// OnCandles processes new candles and generates trading signals
func (s *FourStochasticHeikinAshi) OnCandles(ctx context.Context, oneHourCandles []candle.Candle) (Signal, error) {
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
		if c.Symbol == s.symbol && c.Timeframe == s.Timeframe() && (lastCandle == nil || c.Timestamp.After(lastCandle.Timestamp)) {
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

		// Calculate time range: from 200 hours ago to 1 hour before the first candle
		// We need more data for the 100-period stochastic
		endTime := filteredCandles[0].Timestamp.Truncate(time.Hour)
		startTime := endTime.Add(-200 * time.Hour) // 200 hours for the longest stochastic

		log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Fetching historical 1h candles from %s to %s\n",
			s.symbol, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Fetch historical 1h candles
		historicalCandles, err := s.Storage.GetCandles(ctx, s.symbol, "1h", "", startTime, endTime)
		if err != nil {
			log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error fetching historical candles from database: %v\n", s.symbol, err)
			// Continue with the current candles even if historical fetch fails
		} else if len(historicalCandles) > 0 {
			log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Loaded %d historical candles from database\n", s.symbol, len(historicalCandles))

			// Sort historical candles by timestamp
			sort.Slice(historicalCandles, func(i, j int) bool {
				return historicalCandles[i].Timestamp.Before(historicalCandles[j].Timestamp)
			})

			s.candles = append(s.candles, historicalCandles...)

			s.heikenAshiCandles = candle.GenerateHeikenAshiCandles(historicalCandles)

			// Calculate all four stochastic indicators
			stoch20, err := indicator.CalculateStochastic(historicalCandles, 20, 3, 3)
			if err != nil {
				log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error calculating stochastic 20: %v\n", s.symbol, err)
			} else {
				s.stochastic20 = stoch20
			}

			stoch40, err := indicator.CalculateStochastic(historicalCandles, 40, 3, 3)
			if err != nil {
				log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error calculating stochastic 40: %v\n", s.symbol, err)
			} else {
				s.stochastic40 = stoch40
			}

			stoch60, err := indicator.CalculateStochastic(historicalCandles, 60, 3, 3)
			if err != nil {
				log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error calculating stochastic 60: %v\n", s.symbol, err)
			} else {
				s.stochastic60 = stoch60
			}

			stoch100, err := indicator.CalculateStochastic(historicalCandles, 100, 3, 3)
			if err != nil {
				log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error calculating stochastic 100: %v\n", s.symbol, err)
			} else {
				s.stochastic100 = stoch100
			}
		}
	}

	var lastHeikenAshiCandle *candle.Candle
	if len(s.heikenAshiCandles) > 0 {
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Add current candle prices to the array and update all stochastic indicators
	for _, c := range filteredCandles {
		// Update stochastic 20
		k20, d20, err := indicator.UpdateStochastic(s.stochastic20, s.candles, c, 20, 3, 3)
		if err != nil {
			log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error updating stochastic 20: %v\n", s.symbol, err)
		} else {
			s.stochastic20.K = append(s.stochastic20.K, k20)
			s.stochastic20.D = append(s.stochastic20.D, d20)
		}

		// Update stochastic 40
		k40, d40, err := indicator.UpdateStochastic(s.stochastic40, s.candles, c, 40, 3, 3)
		if err != nil {
			log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error updating stochastic 40: %v\n", s.symbol, err)
		} else {
			s.stochastic40.K = append(s.stochastic40.K, k40)
			s.stochastic40.D = append(s.stochastic40.D, d40)
		}

		// Update stochastic 60
		k60, d60, err := indicator.UpdateStochastic(s.stochastic60, s.candles, c, 60, 3, 3)
		if err != nil {
			log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error updating stochastic 60: %v\n", s.symbol, err)
		} else {
			s.stochastic60.K = append(s.stochastic60.K, k60)
			s.stochastic60.D = append(s.stochastic60.D, d60)
		}

		// Update stochastic 100
		k100, d100, err := indicator.UpdateStochastic(s.stochastic100, s.candles, c, 100, 3, 3)
		if err != nil {
			log.Printf("Strategy | [%s Four Stochastic Heikin Ashi] Error updating stochastic 100: %v\n", s.symbol, err)
		} else {
			s.stochastic100.K = append(s.stochastic100.K, k100)
			s.stochastic100.D = append(s.stochastic100.D, d100)
		}

		s.candles = append(s.candles, c)

		s.heikenAshiCandles = append(s.heikenAshiCandles, candle.GenerateNextHeikenAshiCandle(lastHeikenAshiCandle, c))
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Trim candles to prevent memory leaks
	s.trimCandles()

	// Get the latest regular candle for the signal
	lastCandle = &s.candles[len(s.candles)-1]

	// Check if we have enough data for all stochastic calculations
	minRequiredCandles := 100 + 3 + 3 - 2 // Longest stochastic period
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

	// Get the latest Heikin Ashi candle
	currHA := s.heikenAshiCandles[len(s.heikenAshiCandles)-1]

	// Check if we have valid stochastic values for all indicators
	if len(s.stochastic20.K) == 0 || len(s.stochastic40.K) == 0 ||
		len(s.stochastic60.K) == 0 || len(s.stochastic100.K) == 0 {
		return Signal{
			Time:         lastCandle.Timestamp,
			Position:     Hold,
			Reason:       "stochastic values not ready",
			StrategyName: s.Name(),
			TriggerPrice: lastCandle.Close,
			Candle:       lastCandle,
		}, nil
	}

	// State Machine Logic
	currentState := s.stateMachine.GetCurrentState()

	switch currentState {
	case NoPositionState:
		// Check for buy signal: all stochastics oversold + bullish Heiken Ashi
		if s.areAllStochasticsOversold() && s.isHeikinAshiBullish(currHA) {
			s.stateMachine.TransitionTo(LongBullishPositionUnderOversoldState, "all_stochastics_oversold_bullish_ha", LongBullish, "buy signal triggered")
			return Signal{
				Time:         lastCandle.Timestamp,
				Position:     LongBullish,
				Reason:       "all stochastics oversold + bullish heikin ashi",
				StrategyName: s.Name(),
				TriggerPrice: lastCandle.Close,
				Candle:       lastCandle,
			}, nil
		}

		// Check if all stochastics are oversold but Heiken Ashi is not bullish yet
		if s.areAllStochasticsOversold() {
			s.stateMachine.TransitionTo(LongBullishPositionUnderOversoldBeforeBuyState, "all_stochastics_oversold_waiting_bullish_ha", Hold, "waiting for bullish heikin ashi")
		}

	case LongBullishPositionUnderOversoldBeforeBuyState:
		// Check if Heiken Ashi has become bullish
		if s.isHeikinAshiBullish(currHA) {
			s.stateMachine.TransitionTo(LongBullishPositionUnderOversoldState, "bullish_ha_detected", LongBullish, "buy signal triggered")
			return Signal{
				Time:         lastCandle.Timestamp,
				Position:     LongBullish,
				Reason:       "bullish heikin ashi after all stochastics oversold",
				StrategyName: s.Name(),
				TriggerPrice: lastCandle.Close,
				Candle:       lastCandle,
			}, nil
		}

		// If stochastics are no longer oversold, go back to NoPosition
		// if !s.areAllStochasticsOversold() {
		// 	s.stateMachine.TransitionTo(NoPosition, "stochastics_no_longer_oversold", Hold, "returning to monitoring")
		// }

	case LongBullishPositionUnderOversoldState:
		// Check if green stochastic has crossed above 20
		if s.isGreenStochasticAbove20() {
			s.stateMachine.TransitionTo(LongBullishPositionUpperOversoldState, "green_stoch_crossed_above_20", Hold, "position maintained, monitoring for exit")
		}

	case LongBullishPositionUpperOversoldState:
		// Check if green stochastic has crossed back below 20
		if s.isGreenStochasticBelow20() {
			s.stateMachine.TransitionTo(NoPositionState, "green_stoch_crossed_below_20", LongBearish, "exit signal triggered")
			return Signal{
				Time:         lastCandle.Timestamp,
				Position:     LongBearish,
				Reason:       "green stochastic crossed below 20",
				StrategyName: s.Name(),
				TriggerPrice: lastCandle.Close,
				Candle:       lastCandle,
			}, nil
		}

		// Check if green stochastic has crossed above 80
		if s.isGreenStochasticOverbought() {
			// If Heiken Ashi is bearish, sell immediately
			if s.isHeikinAshiBearish(currHA) {
				s.stateMachine.TransitionTo(NoPositionState, "green_stoch_overbought_bearish_ha", LongBearish, "exit signal triggered")
				return Signal{
					Time:         lastCandle.Timestamp,
					Position:     LongBearish,
					Reason:       "green stochastic overbought + bearish heikin ashi",
					StrategyName: s.Name(),
					TriggerPrice: lastCandle.Close,
					Candle:       lastCandle,
				}, nil
			}
			// If Heiken Ashi is bullish, wait for bearish candle
			s.stateMachine.TransitionTo(LongBearishPositionAboveOverboughtState, "green_stoch_overbought_bullish_ha", Hold, "waiting for bearish heiken ashi")
		}

	case LongBearishPositionAboveOverboughtState:
		// Check if Heiken Ashi has become bearish
		if s.isHeikinAshiBearish(currHA) {
			s.stateMachine.TransitionTo(NoPositionState, "bearish_ha_detected", LongBearish, "exit signal triggered")
			return Signal{
				Time:         lastCandle.Timestamp,
				Position:     LongBearish,
				Reason:       "bearish heiken ashi after green stochastic overbought",
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
func (s *FourStochasticHeikinAshi) PerformanceMetrics() map[string]float64 {
	stateMetrics := s.stateMachine.GetStateMetrics()

	metrics := map[string]float64{
		"heikenAshiCount":  float64(len(s.heikenAshiCandles)),
		"candleCount":      float64(len(s.candles)),
		"maxHistory":       float64(s.maxHistory),
		"overbought":       s.overbought,
		"oversold":         s.oversold,
		"stoch20Length":    float64(len(s.stochastic20.K)),
		"stoch40Length":    float64(len(s.stochastic40.K)),
		"stoch60Length":    float64(len(s.stochastic60.K)),
		"stoch100Length":   float64(len(s.stochastic100.K)),
		"currentState":     float64(len(string(s.stateMachine.GetCurrentState()))), // Convert state to numeric for metrics
		"totalTransitions": float64(stateMetrics["total_transitions"].(int)),
		"stateHistorySize": float64(stateMetrics["history_size"].(int)),
	}

	// Add state-specific metrics
	if s.stateMachine.IsInState(LongBearishPositionAboveOverboughtState) {
		metrics["waitingForBearishHA"] = 1.0
	} else {
		metrics["waitingForBearishHA"] = 0.0
	}

	// Add new state metrics
	if s.stateMachine.IsInState(LongBullishPositionUnderOversoldBeforeBuyState) {
		metrics["waitingForBullishHA"] = 1.0
	} else {
		metrics["waitingForBullishHA"] = 0.0
	}

	return metrics
}

// WarmupPeriod returns the number of candles needed for warm-up
func (s *FourStochasticHeikinAshi) WarmupPeriod() int {
	// Need enough candles for the longest stochastic calculation (100 + 3 + 3 - 2)
	return 100 + 3 + 3 - 2
}
