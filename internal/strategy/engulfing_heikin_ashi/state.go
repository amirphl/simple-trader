// Package engulfing_heikin_ashi
package engulfing_heikin_ashi

import "github.com/amirphl/simple-trader/internal/strategy/state_machine"

const (
	// NoPositionState - No position is held, monitoring for entry signals
	NoPositionState state_machine.State = "No Position"

	// WaitingForExitSignal - Waiting for exit signal
	WaitingForExitSignal state_machine.State = "Waiting for exit signal"
)
