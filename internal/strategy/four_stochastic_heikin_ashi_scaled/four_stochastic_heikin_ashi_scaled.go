// Package four_stochastic_heikin_ashi_scaled
package four_stochastic_heikin_ashi_scaled

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/indicator"
	"github.com/amirphl/simple-trader/internal/strategy/position"
	"github.com/amirphl/simple-trader/internal/strategy/signal"
	"github.com/amirphl/simple-trader/internal/strategy/state_machine"
	"github.com/amirphl/simple-trader/internal/utils"
)

// FourStochasticHeikinAshiScaled is a clone of FourStochasticHeikinAshi that supports N-part scaling
// and generates additional long signals instead of exit when 100 stochastic crosses back below 20.
type FourStochasticHeikinAshiScaled struct {
	symbol            string
	candles           []candle.Candle
	heikenAshiCandles []candle.Candle

	// Four stochastic indicators with different configurations
	stochastic20  *indicator.StochasticResult // Red - length: 20, k: 3, d: 3
	stochastic40  *indicator.StochasticResult // Blue - length: 40, k: 3, d: 3
	stochastic60  *indicator.StochasticResult // Orange - length: 60, k: 3, d: 3
	stochastic100 *indicator.StochasticResult // Green - length: 100, k: 3, d: 3

	Storage db.Storage

	initialized bool
	maxHistory  int // Maximum number of candles to keep in memory

	// Strategy parameters
	overbought float64
	oversold   float64

	// State machine for managing trading positions
	stateMachine *state_machine.StateMachine

	// Scaling parameters
	// maxParts   int // N parts to split fund into
	partsCount int // how many entry signals emitted in current cycle
}

// NewFourStochasticHeikinAshiScaled creates a new scaled strategy
func NewFourStochasticHeikinAshiScaled(symbol string, storage db.Storage) *FourStochasticHeikinAshiScaled {
	return &FourStochasticHeikinAshiScaled{
		symbol:        symbol,
		Storage:       storage,
		stochastic20:  &indicator.StochasticResult{K: []float64{}, D: []float64{}},
		stochastic40:  &indicator.StochasticResult{K: []float64{}, D: []float64{}},
		stochastic60:  &indicator.StochasticResult{K: []float64{}, D: []float64{}},
		stochastic100: &indicator.StochasticResult{K: []float64{}, D: []float64{}},
		initialized:   false,
		maxHistory:    200, // Keep more history for the longer stochastic
		overbought:    80.0,
		oversold:      20.0,
		stateMachine:  state_machine.NewStateMachine(symbol),
		partsCount:    0,
	}
}

// Name returns the name of the strategy
func (s *FourStochasticHeikinAshiScaled) Name() string { return "Four Stochastic Heikin Ashi Scaled" }

// Symbol returns the symbol this strategy is configured for
func (s *FourStochasticHeikinAshiScaled) Symbol() string { return s.symbol }

// Timeframe returns the timeframe this strategy is configured for
func (s *FourStochasticHeikinAshiScaled) Timeframe() string { return "1h" }

// WarmupPeriod returns the number of candles needed for warm-up
func (s *FourStochasticHeikinAshiScaled) WarmupPeriod() int {
	// Need enough candles for the longest stochastic calculation (100 + 3 + 3 - 2)
	return 100 + 3 + 3 - 2
}

// PerformanceMetrics returns performance metrics for the strategy
func (s *FourStochasticHeikinAshiScaled) PerformanceMetrics() map[string]float64 {
	return map[string]float64{}
}

// trimCandles ensures we don't keep too many candles in memory
func (s *FourStochasticHeikinAshiScaled) trimCandles() {
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
func (s *FourStochasticHeikinAshiScaled) isHeikinAshiBullish(haCandle candle.Candle) bool {
	return haCandle.Close > haCandle.Open
}

// isHeikinAshiBearish checks if a Heiken Ashi candle is bearish
func (s *FourStochasticHeikinAshiScaled) isHeikinAshiBearish(haCandle candle.Candle) bool {
	return haCandle.Close < haCandle.Open
}

// areAllStochasticsBelowOversold checks if all four stochastic indicators are below oversold level
func (s *FourStochasticHeikinAshiScaled) areAllStochasticsBelowOversold() bool {
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

// is20StochasticAboveOverbought checks if the 20 stochastic (length 20) is above overbought (80)
func (s *FourStochasticHeikinAshiScaled) is20StochasticAboveOverbought() bool {
	if len(s.stochastic20.K) == 0 {
		return false
	}
	latestIdx := len(s.stochastic20.K) - 1
	k20 := s.stochastic20.K[latestIdx]
	if math.IsNaN(k20) {
		return false
	}
	return k20 > s.overbought
}

// is100StochasticAboveOverbought checks if the 100 stochastic (length 100) is above overbought (80)
func (s *FourStochasticHeikinAshiScaled) is100StochasticAboveOverbought() bool {
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

// is100StochasticAboveOversold checks if the 100 stochastic (length 100) is above oversold (20)
func (s *FourStochasticHeikinAshiScaled) is100StochasticAboveOversold() bool {
	if len(s.stochastic100.K) == 0 {
		return false
	}
	latestIdx := len(s.stochastic100.K) - 1
	k100 := s.stochastic100.K[latestIdx]
	if math.IsNaN(k100) {
		return false
	}
	return k100 > s.oversold
}

// is100StochasticBelowOversold checks if the 100 stochastic (length 100) is below oversold (20)
func (s *FourStochasticHeikinAshiScaled) is100StochasticBelowOversold() bool {
	if len(s.stochastic100.K) == 0 {
		return false
	}
	latestIdx := len(s.stochastic100.K) - 1
	k100 := s.stochastic100.K[latestIdx]
	if math.IsNaN(k100) {
		return false
	}
	return k100 < s.oversold
}

// tryEmitLong emits a LongBullish signal if parts are available and increments the counter
func (s *FourStochasticHeikinAshiScaled) tryEmitLong(lastCandle *candle.Candle, reason string) (signal.Signal, bool) {
	// if s.partsCount >= s.maxParts {
	// 	return signal.Signal{Time: time.Now().UTC(), Position: position.Hold, Reason: "no parts available", StrategyName: s.Name()}, false
	// }
	s.partsCount++
	return signal.Signal{
		Time:         lastCandle.Timestamp,
		Position:     position.LongBullish,
		Reason:       reason,
		StrategyName: s.Name(),
		TriggerPrice: lastCandle.Close,
		Candle:       lastCandle,
	}, true
}

// resetParts resets the parts counter (call on full exit)
func (s *FourStochasticHeikinAshiScaled) resetParts() {
	s.partsCount = 0
}

// OnCandles processes new candles and generates trading signals
func (s *FourStochasticHeikinAshiScaled) OnCandles(ctx context.Context, inputCandles []candle.Candle) (signal.Signal, error) {
	if len(inputCandles) == 0 {
		return signal.Signal{Time: time.Now().UTC(), Position: position.Hold, Reason: "no candles", StrategyName: s.Name()}, nil
	}

	var lastCandle *candle.Candle
	if len(s.candles) > 0 {
		lastCandle = &s.candles[len(s.candles)-1]
	}

	// Filter candles for this symbol by symbol, timeframe, and timestamp
	var filteredCandles []candle.Candle
	for _, c := range inputCandles {
		if c.Symbol == s.symbol && c.Timeframe == s.Timeframe() && (lastCandle == nil || c.Timestamp.After(lastCandle.Timestamp)) {
			filteredCandles = append(filteredCandles, c)
		}
	}
	if len(filteredCandles) == 0 {
		return signal.Signal{Time: time.Now().UTC(), Position: position.Hold, Reason: "no matching candles", StrategyName: s.Name()}, nil
	}

	// Sort candles by timestamp to ensure proper order
	sort.Slice(filteredCandles, func(i, j int) bool { return filteredCandles[i].Timestamp.Before(filteredCandles[j].Timestamp) })

	// Initialize with historical data if needed
	if len(s.candles) == 0 && !s.initialized {
		s.initialized = true // Mark as initialized to avoid fetching again
		endTime := filteredCandles[0].Timestamp.Truncate(time.Hour)
		startTime := endTime.Add(-200 * time.Hour) // 200 hours for the longest stochastic

		utils.GetLogger().Printf("Strategy | [%s Four Stochastic Heikin Ashi Scaled] Fetching historical %s candles from %s to %s\n",
			s.symbol, s.Timeframe(), startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

		// Fetch historical 1h candles
		historicalCandles, err := s.Storage.GetCandles(ctx, s.symbol, s.Timeframe(), "", startTime, endTime)
		if err != nil {
			return signal.Signal{}, err
		} else if len(historicalCandles) > 0 {
			// Sort historical candles by timestamp
			sort.Slice(historicalCandles, func(i, j int) bool { return historicalCandles[i].Timestamp.Before(historicalCandles[j].Timestamp) })
			cc := candle.DBCandlesToCandles(historicalCandles)
			s.candles = append(s.candles, cc...)
			s.heikenAshiCandles = candle.GenerateHeikenAshiCandles(cc)
			stoch20, err := indicator.CalculateStochastic(cc, 20, 3, 3)
			if err != nil {
				return signal.Signal{}, err
			}
			stoch40, err := indicator.CalculateStochastic(cc, 40, 3, 3)
			if err != nil {
				return signal.Signal{}, err
			}
			stoch60, err := indicator.CalculateStochastic(cc, 60, 3, 3)
			if err != nil {
				return signal.Signal{}, err
			}
			stoch100, err := indicator.CalculateStochastic(cc, 100, 3, 3)
			if err != nil {
				return signal.Signal{}, err
			}
			s.stochastic20 = stoch20
			s.stochastic40 = stoch40
			s.stochastic60 = stoch60
			s.stochastic100 = stoch100
		}
	}

	var lastHeikenAshiCandle *candle.Candle
	if len(s.heikenAshiCandles) > 0 {
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Add current candle prices to the array and update all stochastic indicators
	for _, c := range filteredCandles {
		k20, d20, err := indicator.UpdateStochastic(s.stochastic20, s.candles, c, 20, 3, 3)
		if err != nil {
			return signal.Signal{}, err
		}

		k40, d40, err := indicator.UpdateStochastic(s.stochastic40, s.candles, c, 40, 3, 3)
		if err != nil {
			return signal.Signal{}, err
		}

		k60, d60, err := indicator.UpdateStochastic(s.stochastic60, s.candles, c, 60, 3, 3)
		if err != nil {
			return signal.Signal{}, err
		}

		k100, d100, err := indicator.UpdateStochastic(s.stochastic100, s.candles, c, 100, 3, 3)
		if err != nil {
			return signal.Signal{}, err
		}

		s.stochastic20.K = append(s.stochastic20.K, k20)
		s.stochastic20.D = append(s.stochastic20.D, d20)

		s.stochastic40.K = append(s.stochastic40.K, k40)
		s.stochastic40.D = append(s.stochastic40.D, d40)

		s.stochastic60.K = append(s.stochastic60.K, k60)
		s.stochastic60.D = append(s.stochastic60.D, d60)

		s.stochastic100.K = append(s.stochastic100.K, k100)
		s.stochastic100.D = append(s.stochastic100.D, d100)

		s.candles = append(s.candles, c)

		nextHeikenAshiCandle := candle.GenerateNextHeikenAshiCandle(lastHeikenAshiCandle, c)
		s.heikenAshiCandles = append(s.heikenAshiCandles, nextHeikenAshiCandle)
		lastHeikenAshiCandle = &s.heikenAshiCandles[len(s.heikenAshiCandles)-1]
	}

	// Trim candles to prevent memory leaks
	s.trimCandles()

	// Get the latest regular candle for the signal
	lastCandle = &s.candles[len(s.candles)-1]

	// Check if we have enough data for all stochastic calculations
	minRequiredCandles := 100 + 3 + 3 - 2 // Longest stochastic period
	if len(s.candles) < minRequiredCandles {
		return signal.Signal{Time: lastCandle.Timestamp, Position: position.Hold, Reason: "warming up", StrategyName: s.Name(), TriggerPrice: lastCandle.Close, Candle: lastCandle}, nil
	}

	// Get the latest Heikin Ashi candle
	ha := s.heikenAshiCandles[len(s.heikenAshiCandles)-1]

	// Check if we have valid stochastic values for all indicators
	if len(s.stochastic20.K) == 0 || len(s.stochastic40.K) == 0 ||
		len(s.stochastic60.K) == 0 || len(s.stochastic100.K) == 0 {
		return signal.Signal{
			Time:         lastCandle.Timestamp,
			Position:     position.Hold,
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
		// Reset parts on fresh cycle
		s.resetParts()
		// Check for buy signal: all stochastics below oversold + bullish Heiken Ashi
		if s.areAllStochasticsBelowOversold() && s.isHeikinAshiBullish(ha) {
			s.stateMachine.TransitionTo(AllBelowOversold, "all_stochastics_below_oversold_and_bullish_ha", position.LongBullish, "long signal triggered")
			if sig, ok := s.tryEmitLong(lastCandle, "all stochastics below oversold and bullish heikin ashi"); ok {
				return sig, nil
			}
		}
		// Check if all stochastics are below oversold but Heiken Ashi is not bullish yet
		if s.areAllStochasticsBelowOversold() {
			s.stateMachine.TransitionTo(WaitingForBullishHeikenAshiToEnterLong, "all_stochastics_below_oversold_waiting_bullish_ha", position.Hold, "waiting for bullish heikin ashi to enter long")
		}

	case WaitingForBullishHeikenAshiToEnterLong:
		// Check if Heiken Ashi has become bullish
		if s.isHeikinAshiBullish(ha) {
			var nextState state_machine.State
			if s.areAllStochasticsBelowOversold() {
				nextState = AllBelowOversold
			} else {
				nextState = WaitingForEitherExitSignalOrAnotherLongSignal
			}
			s.stateMachine.TransitionTo(nextState, "bullish_ha_detected", position.LongBullish, "long signal triggered")
			if sig, ok := s.tryEmitLong(lastCandle, "bullish heikin ashi after all stochastics below oversold"); ok {
				return sig, nil
			}
		}

	case AllBelowOversold:
		if !s.areAllStochasticsBelowOversold() {
			s.stateMachine.TransitionTo(WaitingForEitherExitSignalOrAnotherLongSignal, "all_below_oversold_to_waiting_for_exit_or_another_long", position.Hold, "waiting for exit signal or another long signal")
		}

	case WaitingForEitherExitSignalOrAnotherLongSignal:
		// Check for buy signal: all stochastics below oversold + bullish Heiken Ashi
		if s.areAllStochasticsBelowOversold() && s.isHeikinAshiBullish(ha) {
			s.stateMachine.TransitionTo(AllBelowOversold, "all_stochastics_below_oversold_and_bullish_ha", position.LongBullish, "long signal triggered")
			if sig, ok := s.tryEmitLong(lastCandle, "all stochastics below oversold and bullish heikin ashi"); ok {
				return sig, nil
			}
		}
		// Check if all stochastics are below oversold but Heiken Ashi is not bullish yet
		if s.areAllStochasticsBelowOversold() {
			s.stateMachine.TransitionTo(WaitingForBullishHeikenAshiToEnterLong, "all_stochastics_below_oversold_waiting_bullish_ha", position.Hold, "waiting for bullish heikin ashi to enter long")
		}

		// Check if 20 stochastic has crossed above 80
		if s.is20StochasticAboveOverbought() {
			// If Heiken Ashi is bearish, sell immediately (full exit)
			if s.isHeikinAshiBearish(ha) {
				s.stateMachine.TransitionTo(NoPositionState, "20_stoch_overbought_bearish_ha", position.LongBearish, "exit signal triggered")
				s.resetParts()
				return signal.Signal{Time: lastCandle.Timestamp, Position: position.LongBearish, Reason: "20 stochastic overbought + bearish heikin ashi", StrategyName: s.Name(), TriggerPrice: lastCandle.Close, Candle: lastCandle}, nil
			}
			// If Heiken Ashi is bullish, wait for bearish candle
			s.stateMachine.TransitionTo(WaitingForBearishHeikenAshiToExitPosition, "20_stoch_overbought_bullish_ha", position.Hold, "waiting for bearish heiken ashi to exit position")
		}

	case WaitingForBearishHeikenAshiToExitPosition:
		if s.isHeikinAshiBearish(ha) {
			s.stateMachine.TransitionTo(NoPositionState, "20_stoch_overbought_bearish_ha", position.LongBearish, "exit signal triggered")
			s.resetParts()
			return signal.Signal{Time: lastCandle.Timestamp, Position: position.LongBearish, Reason: "20 stochastic overbought + bearish heikin ashi", StrategyName: s.Name(), TriggerPrice: lastCandle.Close, Candle: lastCandle}, nil
		}
	}

	// Default: Hold
	return signal.Signal{Time: lastCandle.Timestamp, Position: position.Hold, Reason: "no signal conditions met", StrategyName: s.Name(), TriggerPrice: lastCandle.Close, Candle: lastCandle}, nil
}
