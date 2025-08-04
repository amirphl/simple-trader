// Package four_stochastic_heikin_ashi_scaled
package four_stochastic_heikin_ashi_scaled

import "github.com/amirphl/simple-trader/internal/strategy/state_machine"

const (
	// NoPositionState - No position is held, monitoring for entry signals
	NoPositionState state_machine.State = "No Position"

	// WaitingForBullishHeikenAshiToEnterLong - Waiting for bullish Heiken Ashi when all stochastics are under 20
	WaitingForBullishHeikenAshiToEnterLong state_machine.State = "Waiting for bullish Heiken Ashi to enter long"

	// WaitingForEitherExitSignalOrAnotherLongSignal - Waiting for either exit signal or another long signal
	WaitingForEitherExitSignalOrAnotherLongSignal state_machine.State = "Waiting for either exit signal or another long signal"

	// AllBelowOversold - All stochastics are below oversold
	AllBelowOversold state_machine.State = "All Below Oversold"

	// WaitingForBearishHeikenAshiToExitPosition - Waiting for bearish Heiken Ashi to exit position
	WaitingForBearishHeikenAshiToExitPosition state_machine.State = "Waiting for bearish Heiken Ashi to exit position"
)
