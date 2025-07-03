// Package market
package market

import "time"

// OrderBook represents the L2 orderbook snapshot.
type OrderBook struct {
	Symbol    string
	Bids      [][2]float64 // price, quantity
	Asks      [][2]float64
	Timestamp time.Time
}

// Tick represents a trade tick.
type Tick struct {
	Symbol    string
	Price     float64
	Quantity  float64
	Side      string // "buy" or "sell"
	Timestamp time.Time
}

// MarketManager interface for managing orderbook and tick storage.
type MarketManager interface {
	SaveOrderBook(ob OrderBook) error
	SaveTick(tick Tick) error
	GetOrderBooks(symbol string, start, end time.Time) ([]OrderBook, error)
	GetTicks(symbol string, start, end time.Time) ([]Tick, error)
}
