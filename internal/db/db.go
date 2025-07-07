// Package db
package db

import (
	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/journal"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
)

// DB is the interface for all persistent storage.
type DB interface {
	candle.Storage
	order.OrderManager
	market.MarketManager
	journal.Journaler
	// Add more as needed
}
