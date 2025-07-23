// Package strategy
package strategy

import (
	"fmt"
	"time"
)

// State represents the current state of a trading strategy
type State string

const (
	// NoPosition - No position is held, monitoring for entry signals
	NoPosition State = "NoPosition"

	// LongBullishPositionUnder20BeforeBuy - Waiting for bullish Heiken Ashi when all stochastics are under 20
	LongBullishPositionUnder20BeforeBuy State = "LongBullishPositionUnder20BeforeBuy"

	// LongBullishPositionUnder20 - Long position when all stochastics are under 20
	LongBullishPositionUnder20 State = "LongBullishPositionUnder20"

	// LongBullishPositionUpper20 - Long position when green stochastic has crossed above 20
	LongBullishPositionUpper20 State = "LongBullishPositionUpper20"

	// LongBearishPositionAbove80 - Waiting for bearish Heiken Ashi when green stochastic is above 80
	LongBearishPositionAbove80 State = "LongBearishPositionAbove80"
)

// StateTransition represents a transition from one state to another
type StateTransition struct {
	FromState State
	ToState   State
	Condition string
	Signal    Position
	Reason    string
	Timestamp time.Time
}

// StateMachine manages the state transitions for a trading strategy
type StateMachine struct {
	currentState   State
	transitions    []StateTransition
	symbol         string
	lastTransition time.Time
	stateHistory   []StateTransition
	maxHistorySize int
}

// NewStateMachine creates a new state machine for a trading strategy
func NewStateMachine(symbol string) *StateMachine {
	return &StateMachine{
		currentState:   NoPosition,
		transitions:    make([]StateTransition, 0),
		symbol:         symbol,
		stateHistory:   make([]StateTransition, 0),
		maxHistorySize: 1000, // Keep last 1000 transitions
	}
}

// GetCurrentState returns the current state
func (sm *StateMachine) GetCurrentState() State {
	return sm.currentState
}

// TransitionTo changes the state and logs the transition
func (sm *StateMachine) TransitionTo(newState State, condition string, signal Position, reason string) {
	oldState := sm.currentState
	now := time.Now()

	transition := StateTransition{
		FromState: oldState,
		ToState:   newState,
		Condition: condition,
		Signal:    signal,
		Reason:    reason,
		Timestamp: now,
	}

	// Add to history
	sm.stateHistory = append(sm.stateHistory, transition)
	sm.transitions = append(sm.transitions, transition)

	// Trim history if too long
	if len(sm.stateHistory) > sm.maxHistorySize {
		sm.stateHistory = sm.stateHistory[1:]
	}

	// Update current state
	sm.currentState = newState
	sm.lastTransition = now

	// Log the transition
	// log.Printf("Strategy | [%s State Machine] %s -> %s | Condition: %s | Signal: %s | Reason: %s",
	// 	sm.symbol, oldState, newState, condition, signal, reason)
}

// GetStateHistory returns the state transition history
func (sm *StateMachine) GetStateHistory() []StateTransition {
	return sm.stateHistory
}

// GetLastTransition returns the last transition
func (sm *StateMachine) GetLastTransition() *StateTransition {
	if len(sm.transitions) == 0 {
		return nil
	}
	return &sm.transitions[len(sm.transitions)-1]
}

// GetStateMetrics returns metrics about the state machine
func (sm *StateMachine) GetStateMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"current_state":     sm.currentState,
		"total_transitions": len(sm.transitions),
		"history_size":      len(sm.stateHistory),
		"last_transition":   sm.lastTransition,
	}

	// Count transitions by state
	stateCounts := make(map[State]int)
	for _, transition := range sm.transitions {
		stateCounts[transition.ToState]++
	}
	metrics["state_counts"] = stateCounts

	return metrics
}

// IsInState checks if the state machine is in a specific state
func (sm *StateMachine) IsInState(state State) bool {
	return sm.currentState == state
}

// GetStateDuration returns how long the state machine has been in the current state
func (sm *StateMachine) GetStateDuration() time.Duration {
	if sm.lastTransition.IsZero() {
		return 0
	}
	return time.Since(sm.lastTransition)
}

// Reset resets the state machine to initial state
func (sm *StateMachine) Reset() {
	sm.TransitionTo(NoPosition, "reset", Hold, "state machine reset")
}

// String returns a string representation of the state machine
func (sm *StateMachine) String() string {
	return fmt.Sprintf("StateMachine{symbol: %s, currentState: %s, transitions: %d}",
		sm.symbol, sm.currentState, len(sm.transitions))
}
