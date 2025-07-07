package db

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaCompatibility(t *testing.T) {
	// Set up test database
	testDB, cleanup := SetupTestDB(t)
	defer cleanup()

	// Get the PostgresDB instance
	db, err := testDB.GetTestPostgresDB()
	require.NoError(t, err)

	// Check if we can query the candles table
	_, err = db.db.Exec("SELECT * FROM candles LIMIT 1")
	assert.NoError(t, err, "Should be able to query the candles table")

	// Check if the timeframe column exists in the candles table
	var columnName string
	err = db.db.QueryRow(`
		SELECT column_name 
		FROM information_schema.columns 
		WHERE table_name='candles' AND column_name='timeframe'
	`).Scan(&columnName)
	assert.NoError(t, err, "The 'timeframe' column should exist in the candles table")
	assert.Equal(t, "timeframe", columnName)

	// Check if the primary key is correctly defined
	var constraintDef string
	err = db.db.QueryRow(`
		SELECT pg_get_constraintdef(con.oid) 
		FROM pg_constraint con
		INNER JOIN pg_class rel ON rel.oid = con.conrelid
		WHERE rel.relname = 'candles' AND con.contype = 'p'
	`).Scan(&constraintDef)
	assert.NoError(t, err, "Should be able to retrieve primary key definition")

	// The unique constraint in PostgreSQL is often implemented as part of the primary key
	// So let's check if we can insert a record with a duplicate key and it fails appropriately
	_, err = db.db.Exec(`
		INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume, source)
		VALUES ('BTC-USDT', '1m', '2022-01-01 00:00:00', 10000, 10100, 9900, 10050, 1.5, 'test')
	`)
	assert.NoError(t, err, "Should be able to insert a record")

	_, err = db.db.Exec(`
		INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume, source)
		VALUES ('BTC-USDT', '1m', '2022-01-01 00:00:00', 10000, 10100, 9900, 10050, 1.5, 'test')
	`)
	assert.Error(t, err, "Should not be able to insert a duplicate record")
	assert.True(t, strings.Contains(err.Error(), "duplicate key value violates unique constraint"),
		"Error should be about duplicate key violation, got: %v", err)
}
