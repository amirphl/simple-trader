package db

import (
	"testing"

	dbconf "github.com/amirphl/simple-trader/internal/db/conf"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// Example test function showing how to use the test helpers
func TestUsageExample(t *testing.T) {
	// Set up test database
	cfg, cleanup := dbconf.NewTestConfig(t)
	require.NotNil(t, cfg)
	defer cleanup() // This will be called when the test finishes

	// Create PostgresDB instance for testing
	_, err := New(*cfg)
	if err != nil {
		t.Fatalf("Failed to create PostgresDB: %v", err)
	}

	// Run your tests using postgresDB...
	// For example:
	// err = postgresDB.SaveCandles(testCandles)
	// assert.NoError(t, err)
}
