// Package conf
package conf

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Config holds test database connection and metadata
type Config struct {
	Name      string
	DB        *sql.DB
	ConnStr   string
	AdminDB   *sql.DB
	SchemaSQL string
}

// NewTestConfig creates a new database with a random name and applies the schema.
// It returns a TestDB struct with connection details and a cleanup function.
func NewTestConfig(t *testing.T) (*Config, func()) {
	t.Helper()

	const (
		// Default connection parameters for test database
		testHost     = "localhost"
		testPort     = 5432
		testUser     = "postgres"
		testPassword = "postgres" // Change this if your local postgres has a different password
	)

	// Connect to postgres to create the test database
	adminConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable",
		testHost, testPort, testUser, testPassword)

	adminDB, err := sql.Open("postgres", adminConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to postgres: %v", err)
	}

	// Check if PostgreSQL is running
	err = adminDB.Ping()
	if err != nil {
		adminDB.Close()
		t.Skipf("Skipping test: PostgreSQL is not running or not accessible: %v", err)
		return nil, func() {}
	}

	// Generate random database name to avoid conflicts
	dbName := fmt.Sprintf("test_db_%d", rand.Int31())

	// Create the test database
	_, err = adminDB.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		adminDB.Close()
		t.Fatalf("Failed to create test database: %v", err)
	}

	// Read schema SQL file
	schemaPath := filepath.Join("scripts", "schema.sql")
	// Try to find schema.sql in parent directories if not found
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		// Try one level up
		schemaPath = filepath.Join("..", "..", "scripts", "schema.sql")
		if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
			// Try two levels up
			schemaPath = filepath.Join("..", "..", "..", "scripts", "schema.sql")
		}
	}

	schemaSQLBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		adminDB.Close()
		t.Fatalf("Failed to read schema.sql: %v", err)
	}
	originalSchema := string(schemaSQLBytes)

	// Connect to the test database
	dbConnStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		testHost, testPort, testUser, testPassword, dbName)

	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		adminDB.Close()
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Check if TimescaleDB is available
	var hasTimescaleDB bool
	err = db.QueryRow("SELECT 1 FROM pg_available_extensions WHERE name = 'timescaledb'").Scan(&hasTimescaleDB)
	if err != nil {
		t.Logf("Warning: Failed to check for TimescaleDB extension: %v", err)
	}

	// Try to create extension for TimescaleDB if available
	if hasTimescaleDB {
		_, err = db.Exec("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
		if err != nil {
			t.Logf("Warning: Failed to create TimescaleDB extension: %v", err)
			// hasTimescaleDB = false
		}
	} else {
		t.Logf("Warning: TimescaleDB extension is not available, continuing without it")
	}

	// Filter out TimescaleDB statements if TimescaleDB is not available
	var statements []string
	for stmt := range strings.SplitSeq(originalSchema, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}

		if !hasTimescaleDB && strings.Contains(strings.ToLower(stmt), "create_hypertable") {
			t.Logf("Skipping TimescaleDB statement: %s", stmt)
			continue
		}

		statements = append(statements, stmt)
	}

	// Apply schema statements one by one
	for _, stmt := range statements {
		_, err = db.Exec(stmt)
		if err != nil {
			db.Close()
			adminDB.Close()
			t.Fatalf("Failed to apply schema statement: %s\nError: %v", stmt, err)
		}
	}

	testDB := &Config{
		Name:      dbName,
		DB:        db,
		ConnStr:   dbConnStr,
		AdminDB:   adminDB,
		SchemaSQL: originalSchema,
	}

	// Return cleanup function
	cleanup := func() {
		db.Close()

		// Drop the test database
		_, err := adminDB.Exec(fmt.Sprintf("DROP DATABASE %s WITH (FORCE)", dbName))
		if err != nil {
			t.Logf("Warning: Failed to drop test database %s: %v", dbName, err)
		}

		adminDB.Close()
	}

	return testDB, cleanup
}
