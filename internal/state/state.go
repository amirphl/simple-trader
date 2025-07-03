package state

// StateManager interface for persisting and recovering bot state.
type StateManager interface {
	SaveState(state map[string]any) error
	LoadState() (map[string]any, error)
}

