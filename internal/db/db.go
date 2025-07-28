// Package db
package db

import (
	"database/sql"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/journal"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/order"
)

// Storage is the interface for all persistent storage.
type Storage interface {
	GetDB() *sql.DB
	candle.Storage
	order.OrderManager
	market.MarketManager
	journal.Journaler
	// Add more as needed
}
