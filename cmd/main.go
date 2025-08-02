package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"math/rand"

	"github.com/amirphl/simple-trader/internal/backtest"
	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	dbconfig "github.com/amirphl/simple-trader/internal/db/conf"
	"github.com/amirphl/simple-trader/internal/exchange"
	"github.com/amirphl/simple-trader/internal/livetrading"
	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/strategy"
	"github.com/amirphl/simple-trader/internal/utils"
	"github.com/lib/pq"
)

func main() {
	// Initialize random seed for jitter in retry logic
	rand.Seed(time.Now().UnixNano())

	cfg := config.MustLoadConfig()
	utils.GetLogger().Println("Starting Simple Trader in mode:", cfg.Mode)

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		utils.GetLogger().Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Run migrations if enabled
	if cfg.RunMigration {
		if err := runMigrations(ctx, cfg.DBConnStr); err != nil {
			// TODO: gracefull
			utils.GetLogger().Fatalf("Failed to run migrations: %v", err)
		}
	}

	// Initialize database connection
	dbCfg, err := dbconfig.NewConfig(cfg.DBConnStr, cfg.DBMaxOpen, cfg.DBMaxIdle)
	if err != nil {
		// TODO: gracefull
		utils.GetLogger().Fatalf("Failed to create DB config: %v", err)
	}

	storage, err := db.New(*dbCfg)
	if err != nil {
		// TODO: gracefull
		utils.GetLogger().Fatalf("Failed to initialize database: %v", err)
	}
	utils.GetLogger().Println("Connected to Postgres/TimescaleDB")

	// Set up notification system
	telegramNotifier := notifier.NewTelegramNotifier(cfg.TelegramToken, cfg.TelegramChatID, cfg.ProxyURL, cfg.NotificationRetries, cfg.NotificationDelay)

	// Create exchange connection
	// wallexExchange := exchange.NewWallexExchange(cfg.WallexAPIKey, telegramNotifier)
	mockWallexExchange := exchange.NewMockWallexExchange(cfg.WallexAPIKey, telegramNotifier)

	// Create strategies
	strats := strategy.New(cfg, storage)
	if len(strats) == 0 {
		// TODO: gracefull
		utils.GetLogger().Fatalf("No valid strategies configured. Check your configuration.")
	}

	// Handle different modes
	switch cfg.Mode {
	case "live":
		livetrading.RunLiveTrading(ctx, cfg, strats, storage, mockWallexExchange, telegramNotifier)
	case "backtest":
		if cfg.MultiSymbolBacktest {
			backtest.RunMultiSymbolBacktest(ctx, cfg, storage)
		} else {
			backtest.RunBacktest(ctx, cfg, strats, storage)
		}
	default:
		// TODO: gracefull
		utils.GetLogger().Fatalf("Unsupported mode: %s", cfg.Mode)
	}

	<-sigCh
	utils.GetLogger().Println("Graceful shutdown initiated...")

	// Allow some time for cleanup
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer shutdownCancel()

	// Wait for context to be done (either timeout or cancel)
	<-shutdownCtx.Done()
	utils.GetLogger().Println("Shutdown complete")
}

// runMigrations creates the database if it doesn't exist and runs the schema.sql script
func runMigrations(ctx context.Context, connStr string) error {
	utils.GetLogger().Println("Running database migrations...")

	// Parse connection string to extract database name
	u, err := url.Parse(connStr)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	dbName := strings.TrimPrefix(u.Path, "/")
	if dbName == "" {
		return fmt.Errorf("database name not found in connection string")
	}

	// Create a connection string to the postgres database to create our database
	baseConnStr := fmt.Sprintf("postgres://%s:%s@%s/postgres%s",
		u.User.Username(),
		func() string {
			p, _ := u.User.Password()
			return p
		}(),
		u.Host,
		func() string {
			if u.RawQuery != "" {
				return "?" + u.RawQuery
			}
			return ""
		}())

	// Connect to the postgres database
	baseDB, err := sql.Open("postgres", baseConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	defer baseDB.Close()

	// Check if our database exists
	var exists bool
	err = baseDB.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if database exists: %w", err)
	}

	// Create the database if it doesn't exist
	if !exists {
		utils.GetLogger().Printf("Creating database %s...", dbName)
		_, err = baseDB.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName)))
		if err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}
	}

	// Connect to our database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// ISSUE:
	// Read the schema.sql file
	schemaSQL, err := os.ReadFile("/home/amirphl/sources/simple-trader/scripts/schema.sql")
	if err != nil {
		return fmt.Errorf("failed to read schema.sql: %w", err)
	}

	// Execute the schema.sql script
	_, err = db.ExecContext(ctx, string(schemaSQL))
	if err != nil {
		return fmt.Errorf("failed to execute schema.sql: %w", err)
	}

	utils.GetLogger().Println("Database migrations completed successfully")
	return nil
}
