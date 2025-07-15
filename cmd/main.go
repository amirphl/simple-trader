package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"math"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/db/conf"
	"github.com/amirphl/simple-trader/internal/exchange"
	"github.com/amirphl/simple-trader/internal/journal"
	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/position"
	"github.com/amirphl/simple-trader/internal/strategy"
	"github.com/lib/pq"
)

// orderStatusChecker periodically checks the status of open orders
// and updates the database accordingly
func orderStatusChecker(ctx context.Context, dbAdapter db.DB, ex exchange.Exchange, checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	log.Println("Starting order status checker")

	for {
		select {
		case <-ctx.Done():
			log.Println("Order status checker stopped")
			return
		case <-ticker.C:
			// Get all open orders from database
			orders, err := dbAdapter.GetOpenOrders(ctx)
			if err != nil {
				log.Printf("Failed to fetch open orders: %v", err)
				continue
			}

			if len(orders) == 0 {
				continue
			}

			log.Printf("Checking status of %d open orders", len(orders))

			for _, o := range orders {
				orderResp, err := ex.GetOrderStatus(ctx, o.OrderID)
				if err != nil {
					log.Printf("Error fetching order status for %s: %v", o.OrderID, err)
					continue
				}

				switch orderResp.Status {
				case "FILLED":
					log.Printf("Order %s filled", o.OrderID)
					// TODO: Tx
					dbAdapter.LogEvent(ctx, journal.Event{
						Time:        time.Now(),
						Type:        "order",
						Description: "status_check_order_filled",
						Data:        map[string]any{"order": orderResp},
					})
					if err := dbAdapter.CloseOrder(ctx, o.OrderID); err != nil {
						log.Printf("Failed to close order %s: %v", o.OrderID, err)
					}
				case "CANCELED", "EXPIRED", "REJECTED":
					log.Printf("Order %s %s", o.OrderID, orderResp.Status)
					// TODO: Tx
					dbAdapter.LogEvent(ctx, journal.Event{
						Time:        time.Now(),
						Type:        "order",
						Description: "status_check_order_canceled_or_expired",
						Data:        map[string]any{"order": orderResp},
					})
					if err := dbAdapter.CloseOrder(ctx, o.OrderID); err != nil {
						log.Printf("Failed to close order %s: %v", o.OrderID, err)
					}
				}
			}
		}
	}
}

func main() {
	// Load configuration
	cfg := config.MustLoadConfig()
	log.Println("Starting Simple Trader in mode:", cfg.Mode)

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}()

	// Run migrations if enabled
	if cfg.RunMigration {
		if err := runMigrations(ctx, cfg.DBConnStr); err != nil {
			log.Fatalf("Failed to run migrations: %v", err)
		}
	}

	// Initialize database connection
	dbConfig, err := conf.NewConfig(cfg.DBConnStr, cfg.DBMaxOpen, cfg.DBMaxIdle)
	if err != nil {
		log.Fatalf("Failed to create DB config: %v", err)
	}

	dbadapter, err := db.New(*dbConfig)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	log.Println("Connected to Postgres/TimescaleDB")

	// Create aggregator for candle processing
	aggregator := candle.NewAggregator(dbadapter)

	// Set up notification system
	telegramNotifier := notifier.NewTelegramNotifier(cfg.TelegramToken, cfg.TelegramChatID, cfg.ProxyURL, cfg.NotificationRetries, cfg.NotificationDelay)

	// Create exchange connection
	ex := exchange.NewWallexExchange(cfg.WallexAPIKey, telegramNotifier)

	// Create strategies
	strats := strategy.New(cfg, dbadapter)
	if len(strats) == 0 {
		log.Fatalf("No valid strategies configured. Check your configuration.")
	}

	// Define default timeframe
	const defaultTimeframe = "1m"

	switch cfg.Mode {
	case "live":
		runLiveTrading(ctx, cfg, dbadapter, aggregator, ex, strats, telegramNotifier)
	case "backtest":
		runBacktest(ctx, cfg, dbadapter, ex, strats, defaultTimeframe)
	default:
		log.Fatalf("Unsupported mode: %s", cfg.Mode)
	}

	// Wait for shutdown signal
	<-sigCh
	log.Println("Graceful shutdown initiated...")

	// Allow some time for cleanup
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Wait for context to be done (either timeout or cancel)
	<-shutdownCtx.Done()
	log.Println("Shutdown complete")
}

// runMigrations creates the database if it doesn't exist and runs the schema.sql script
func runMigrations(ctx context.Context, connStr string) error {
	log.Println("Running database migrations...")

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
		log.Printf("Creating database %s...", dbName)
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

	// Read the schema.sql file
	schemaSQL, err := os.ReadFile("scripts/schema.sql")
	if err != nil {
		return fmt.Errorf("failed to read schema.sql: %w", err)
	}

	// Execute the schema.sql script
	_, err = db.ExecContext(ctx, string(schemaSQL))
	if err != nil {
		return fmt.Errorf("failed to execute schema.sql: %w", err)
	}

	log.Println("Database migrations completed successfully")
	return nil
}

// runLiveTrading handles the live trading mode
func runLiveTrading(
	ctx context.Context,
	cfg config.Config,
	dbadapter db.DB,
	aggregator candle.Aggregator,
	ex exchange.Exchange,
	strats []strategy.Strategy,
	notifier notifier.Notifier,
) {
	// Setup exchanges map
	exchanges := map[string]candle.Exchange{
		"wallex": ex,
	}

	// Create ingestion config with flexible timeframe support
	ingestionCfg := candle.DefaultIngestionConfig()
	ingestionCfg.Symbols = cfg.Symbols

	// Create and start ingestion service
	ingestionSvc := candle.NewIngestionService(ctx, dbadapter, aggregator, exchanges, ingestionCfg)
	if err := ingestionSvc.Start(); err != nil {
		log.Fatalf("Failed to start candle ingestion service: %v", err)
	}
	log.Println("Candle ingestion service started with flexible timeframe support")

	// Setup recovery in case of panic
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in live trading: %v", r)
			// Try to notify about the panic
			notifier.Send(fmt.Sprintf("PANIC in trading system: %v", r))
			if ingestionSvc != nil {
				ingestionSvc.Stop()
			}
		}
	}()

	// Start order status checker
	checkInterval := time.Minute
	go orderStatusChecker(ctx, dbadapter, ex, checkInterval)

	// Print initial ingestion stats
	printIngestionStats(ingestionSvc)

	// Monitor and report stats periodically
	statsInterval := 3 * time.Minute
	go monitorIngestionStats(ctx, ingestionSvc, statsInterval)

	// Start trading for each strategy
	var wg sync.WaitGroup
	for _, strat := range strats {
		wg.Add(1)
		go func(s strategy.Strategy) {
			defer wg.Done()
			runTradingLoop(ctx, s, ingestionSvc, dbadapter, ex, notifier, cfg)
		}(strat)
	}

	// Wait for all trading loops to complete
	wg.Wait()
}

// monitorIngestionStats periodically prints ingestion statistics
func monitorIngestionStats(ctx context.Context, ingestionSvc candle.IngestionService, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			printIngestionStats(ingestionSvc)
		}
	}
}

// printIngestionStats prints the current ingestion statistics
func printIngestionStats(ingestionSvc candle.IngestionService) {
	stats := ingestionSvc.GetIngestionStats()
	log.Println("Candle Ingestion Stats:")
	for symbol, symbolStats := range stats {
		log.Printf("  %s:", symbol)
		for timeframe, timeframeStats := range symbolStats {
			log.Printf("    %s: %+v", timeframe, timeframeStats)
		}
	}
}

// runTradingLoop handles the trading loop for a specific strategy
func runTradingLoop(
	ctx context.Context,
	strat strategy.Strategy,
	ingestionSvc candle.IngestionService,
	dbadapter db.DB,
	ex exchange.Exchange,
	notifier notifier.Notifier,
	cfg config.Config,
) {
	log.Printf("Starting trading loop with %s strategy", strat.Name())

	// Subscribe to candle updates
	candleCh := ingestionSvc.Subscribe()
	defer ingestionSvc.UnSubscribe(candleCh)

	// Load position manager
	pos := position.New(cfg, strat.Name(), strat.Symbol(), dbadapter, ex, notifier)

	// Live performance tracking
	lastDay := time.Now().Day()
	dailyPnL := 0.0
	tradingDisabled := false

	// Warmup tracking
	warmupPeriod := strat.WarmupPeriod()
	candlesSeen := 0

	// Status check ticker
	statusTicker := time.NewTicker(60 * time.Second)
	defer statusTicker.Stop()

	for {
		select {
		case candles, ok := <-candleCh:
			if !ok {
				log.Printf("[%s] Candle channel closed", strat.Name())
				return
			}

			if len(candles) == 0 {
				continue
			}

			// Process candles with strategy
			signal, err := strat.OnCandles(ctx, candles)
			if err != nil {
				log.Printf("[%s] Error processing candles: %v", strat.Name(), err)
				continue
			}

			// Update warmup counter
			candlesSeen += len(candles)

			// Skip trading during warmup period
			if candlesSeen < warmupPeriod {
				continue
			}

			// Skip if trading is disabled
			if tradingDisabled {
				continue
			}

			// Process signal with position manager
			if signal.Action != "hold" {
				pos.OnSignal(ctx, signal)
			}

		case <-statusTicker.C:
			// Reset daily loss counter on new day
			currentDay := time.Now().Day()
			if currentDay != lastDay {
				tradingDisabled = false
				dailyPnL = 0.0
				lastDay = currentDay

				// Log state change: new trading day
				// TODO: Tx
				dbadapter.LogEvent(ctx, journal.Event{
					Time:        time.Now(),
					Type:        "state",
					Description: "new_trading_day",
					Data: map[string]any{
						"symbol":    strat.Symbol(),
						"timeframe": "1m", // Default timeframe
						"day":       lastDay,
					},
				})
			}

			// Check if max daily loss reached
			if cfg.MaxDailyLoss > 0 && dailyPnL <= -cfg.MaxDailyLoss {
				if !tradingDisabled {
					log.Printf("[%s] Trading disabled due to max daily loss reached", strat.Name())
					dbadapter.LogEvent(ctx, journal.Event{
						Time:        time.Now(),
						Type:        "state",
						Description: "trading_disabled_max_daily_loss",
						Data:        map[string]any{"strategy_name": strat.Name()},
					})
					tradingDisabled = true
				}
			}

			// Update daily PnL from position manager
			// TODO:
			todayPnL, err := pos.DailyPnL()
			if err != nil {
				log.Printf("[%s] Error getting daily PnL: %v", strat.Name(), err)
			} else {
				dailyPnL = todayPnL
			}

		case <-ctx.Done():
			log.Printf("[%s] Trading loop stopped", strat.Name())
			return
		}
	}
}

// runBacktest handles the backtest mode
func runBacktest(
	ctx context.Context,
	cfg config.Config,
	dbadapter db.DB,
	ex exchange.Exchange,
	strats []strategy.Strategy,
	timeframe string,
) {
	for _, symbol := range cfg.Symbols {
		// Load candles for backtesting
		candles, err := loadBacktestCandles(ctx, dbadapter, ex, symbol, timeframe, cfg.BacktestFrom.Time, cfg.BacktestTo.Time)
		if err != nil {
			log.Fatalf("Error loading candles for backtest: %v", err)
		}

		log.Printf("Loaded %d candles for backtest [%s-%s]",
			len(candles), cfg.BacktestFrom.Time.Format(time.RFC3339), cfg.BacktestTo.Time.Format(time.RFC3339))

		// Run backtest for each strategy
		for _, strat := range strats {
			backtestResults := runStrategyBacktest(strat, candles, cfg)

			// Print backtest results
			printBacktestResults(strat, backtestResults)

			// Save backtest results to CSV
			saveBacktestResults(backtestResults)
		}
	}
}

// loadBacktestCandles loads candles for backtesting, downloading from exchange if necessary
func loadBacktestCandles(
	ctx context.Context,
	dbadapter db.DB,
	ex exchange.Exchange,
	symbol, timeframe string,
	from, to time.Time,
) ([]candle.Candle, error) {
	// Try to load candles from database first
	candles, err := dbadapter.GetCandles(ctx, symbol, timeframe, "", from, to)
	if err != nil {
		return nil, fmt.Errorf("error loading candles from database: %w", err)
	}

	// If no candles found in database, download from exchange
	if len(candles) == 0 {
		log.Printf("No historical candles found in DB for %s, downloading from exchange...", symbol)

		// Download candles in chunks to avoid hitting API limits
		currTime := from
		maxChunkDays := 30

		for currTime.Before(to) {
			next := currTime.Add(time.Duration(maxChunkDays) * 24 * time.Hour)
			if next.After(to) {
				next = to
			}

			// Create a context with timeout for the API call
			downloadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			downloadedCandles, err := ex.FetchCandles(downloadCtx, symbol, timeframe, currTime.Unix(), next.Unix())
			cancel()

			if err != nil {
				return nil, fmt.Errorf("error fetching candles from %s to %s: %w",
					currTime.Format(time.RFC3339), next.Format(time.RFC3339), err)
			}

			// Save downloaded candles to database
			if len(downloadedCandles) > 0 {
				saveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				// NOTE: 1m truncated
				err = dbadapter.SaveCandles(saveCtx, downloadedCandles)
				cancel()

				if err != nil {
					return nil, fmt.Errorf("error saving candles to database: %w", err)
				}

				log.Printf("Downloaded and saved %d candles [%s-%s]",
					len(downloadedCandles), currTime.Format(time.RFC3339), next.Format(time.RFC3339))
			} else {
				log.Printf("No candles available from %s to %s",
					currTime.Format(time.RFC3339), next.Format(time.RFC3339))
			}

			currTime = next
		}

		// Load the downloaded candles
		candles, err = dbadapter.GetCandles(ctx, symbol, timeframe, "", from, to)
		if err != nil {
			return nil, fmt.Errorf("error loading downloaded candles: %w", err)
		}
	}

	return candles, nil
}

// BacktestResults holds the results of a backtest
type BacktestResults struct {
	Equity      float64
	MaxEquity   float64
	MaxDrawdown float64
	Wins        int
	Losses      int
	Trades      int
	WinPnls     []float64
	LossPnls    []float64
	EquityCurve []float64
	TradeLog    []TradeLogEntry
	Metrics     map[string]float64
}

// TradeLogEntry represents a single trade in the backtest
type TradeLogEntry struct {
	Entry     float64
	Exit      float64
	PnL       float64
	EntryTime time.Time
	ExitTime  time.Time
}

// runStrategyBacktest runs a backtest for a specific strategy
func runStrategyBacktest(strat strategy.Strategy, candles []candle.Candle, cfg config.Config) BacktestResults {
	var results BacktestResults
	results.Metrics = make(map[string]float64)

	// Initialize backtest variables
	var signals []strategy.Signal
	var lastBuyPrice float64
	var active bool
	var dailyPnL float64
	var lastDay int = -1
	var trailingStop float64

	// Risk parameters
	stopLossPercent := cfg.StopLossPercent
	trailingStopPercent := cfg.TrailingStopPercent
	maxDailyLoss := cfg.MaxDailyLoss
	slippage := cfg.SlippagePercent / 100.0
	commission := cfg.CommissionPercent / 100.0

	// Process each candle
	for _, c := range candles {
		// Reset daily PnL on new day
		if lastDay != c.Timestamp.Day() {
			dailyPnL = 0
			trailingStop = 0
			lastDay = c.Timestamp.Day()
		}

		// Get signal from strategy
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		sig, _ := strat.OnCandles(ctx, []candle.Candle{c}) // TODO: context.Background() is not good, we need to pass a context that is cancelled when the strategy is stopped
		signals = append(signals, sig)

		// Process buy signal
		if sig.Action == "buy" && !active && (maxDailyLoss == 0 || dailyPnL > -maxDailyLoss) {
			// Simulate slippage and commission on entry
			entryPrice := c.Close * (1 + slippage)
			entryPrice += entryPrice * commission
			lastBuyPrice = entryPrice
			active = true
			results.Trades++

			// Record trade entry
			results.TradeLog = append(results.TradeLog, TradeLogEntry{
				Entry:     entryPrice,
				EntryTime: c.Timestamp,
			})
			trailingStop = 0
		} else if active {
			// Calculate stop loss price
			stopLossPrice := lastBuyPrice * (1 - stopLossPercent/100)
			price := c.Close

			// Simulate slippage and commission on exit
			exitPrice := price * (1 - slippage)
			exitPrice -= exitPrice * commission

			pnl := 0.0
			stopTriggered := false
			trailingTriggered := false

			// Check stop loss
			if stopLossPercent > 0 && price <= stopLossPrice {
				exitPrice = stopLossPrice * (1 - slippage)
				exitPrice -= exitPrice * commission
				pnl = exitPrice - lastBuyPrice
				stopTriggered = true
			} else if trailingStopPercent > 0 {
				// Update trailing stop
				profit := price - lastBuyPrice
				if profit > trailingStop {
					trailingStop = profit
				}

				// Check if trailing stop triggered
				trailingStopPrice := lastBuyPrice + trailingStop - (lastBuyPrice * trailingStopPercent / 100)
				if price <= trailingStopPrice {
					exitPrice = trailingStopPrice * (1 - slippage)
					exitPrice -= exitPrice * commission
					pnl = exitPrice - lastBuyPrice
					trailingTriggered = true
				}
			}

			// Process sell signal
			if sig.Action == "sell" && !stopTriggered && !trailingTriggered {
				pnl = exitPrice - lastBuyPrice
			}

			// Close position if sell signal or stop triggered
			if sig.Action == "sell" || stopTriggered || trailingTriggered {
				results.Equity += pnl
				dailyPnL += pnl

				// Update trade log
				lastTrade := &results.TradeLog[len(results.TradeLog)-1]
				lastTrade.Exit = exitPrice
				lastTrade.ExitTime = c.Timestamp
				lastTrade.PnL = pnl

				// Update win/loss stats
				if pnl > 0 {
					results.Wins++
					results.WinPnls = append(results.WinPnls, pnl)
				} else {
					results.Losses++
					results.LossPnls = append(results.LossPnls, pnl)
				}

				active = false
				trailingStop = 0
			}
		}

		// Update equity curve and drawdown
		if results.Equity > results.MaxEquity {
			results.MaxEquity = results.Equity
		}

		dd := results.MaxEquity - results.Equity
		if dd > results.MaxDrawdown {
			results.MaxDrawdown = dd
		}

		results.EquityCurve = append(results.EquityCurve, results.Equity)
	}

	// Calculate performance metrics
	calculatePerformanceMetrics(&results)

	// Add strategy-specific metrics
	stratMetrics := strat.PerformanceMetrics()
	for k, v := range stratMetrics {
		results.Metrics[k] = v
	}

	return results
}

// calculatePerformanceMetrics calculates performance metrics for backtest results
func calculatePerformanceMetrics(results *BacktestResults) {
	// Calculate win rate
	if results.Trades > 0 {
		results.Metrics["win_rate"] = float64(results.Wins) / float64(results.Trades)
	}

	// Calculate average win and loss
	avgWin, avgLoss := 0.0, 0.0

	for _, w := range results.WinPnls {
		avgWin += w
	}

	for _, l := range results.LossPnls {
		avgLoss += l
	}

	if len(results.WinPnls) > 0 {
		avgWin /= float64(len(results.WinPnls))
		results.Metrics["avg_win"] = avgWin
	}

	if len(results.LossPnls) > 0 {
		avgLoss /= float64(len(results.LossPnls))
		results.Metrics["avg_loss"] = avgLoss
	}

	// Calculate profit factor
	if avgLoss != 0 {
		results.Metrics["profit_factor"] = -avgWin / avgLoss
	}

	// Calculate Sharpe ratio and expectancy
	allPnls := append(results.WinPnls, results.LossPnls...)
	meanPnl, stdPnl := 0.0, 0.0

	for _, p := range allPnls {
		meanPnl += p
	}

	if len(allPnls) > 0 {
		meanPnl /= float64(len(allPnls))
		results.Metrics["mean_pnl"] = meanPnl

		for _, p := range allPnls {
			stdPnl += (p - meanPnl) * (p - meanPnl)
		}

		stdPnl = math.Sqrt(stdPnl / float64(len(allPnls)))
		results.Metrics["std_pnl"] = stdPnl

		if stdPnl > 0 {
			results.Metrics["sharpe"] = meanPnl / stdPnl
		}
	}

	// Calculate expectancy
	if results.Trades > 0 {
		winRate := results.Metrics["win_rate"]
		results.Metrics["expectancy"] = (winRate*avgWin + (1-winRate)*avgLoss)
	}
}

// printBacktestResults prints the results of a backtest
func printBacktestResults(strat strategy.Strategy, results BacktestResults) {
	log.Printf("Backtest Results (%s):\n", strat.Name())
	log.Printf("  Trades=%d, Wins=%d, Losses=%d, WinRate=%.2f%%\n",
		results.Trades, results.Wins, results.Losses, results.Metrics["win_rate"]*100)
	log.Printf("  PnL=%.2f, MaxDrawdown=%.2f\n",
		results.Equity, results.MaxDrawdown)
	log.Printf("  AvgWin=%.2f, AvgLoss=%.2f, ProfitFactor=%.2f\n",
		results.Metrics["avg_win"], results.Metrics["avg_loss"], results.Metrics["profit_factor"])
	log.Printf("  Sharpe=%.2f, Expectancy=%.2f\n",
		results.Metrics["sharpe"], results.Metrics["expectancy"])

	// Print strategy-specific metrics
	if len(results.Metrics) > 0 {
		fmt.Println("  Strategy Metrics:")
		for k, v := range results.Metrics {
			// Skip metrics we've already printed
			if k != "win_rate" && k != "avg_win" && k != "avg_loss" &&
				k != "profit_factor" && k != "sharpe" && k != "expectancy" {
				fmt.Printf("    %s: %.4f\n", k, v)
			}
		}
	}

	// Print trade log summary
	fmt.Println("Trade Log Summary:")
	maxTrades := 10 // Limit the number of trades to print
	for i, t := range results.TradeLog {
		if i >= maxTrades {
			fmt.Printf("  ... and %d more trades\n", len(results.TradeLog)-maxTrades)
			break
		}
		fmt.Printf("  Trade %d: Entry=%.2f at %s, Exit=%.2f at %s, PnL=%.2f\n",
			i+1, t.Entry, t.EntryTime.Format(time.RFC3339),
			t.Exit, t.ExitTime.Format(time.RFC3339), t.PnL)
	}
}

// saveBacktestResults saves backtest results to CSV files
func saveBacktestResults(results BacktestResults) {
	// Save trade log to CSV
	tradeRows := [][]string{{"Trade#", "Entry", "EntryTime", "Exit", "ExitTime", "PnL"}}
	for i, t := range results.TradeLog {
		tradeRows = append(tradeRows, []string{
			fmt.Sprintf("%d", i+1),
			fmt.Sprintf("%.2f", t.Entry),
			t.EntryTime.Format(time.RFC3339),
			fmt.Sprintf("%.2f", t.Exit),
			t.ExitTime.Format(time.RFC3339),
			fmt.Sprintf("%.2f", t.PnL),
		})
	}

	// Save equity curve to CSV
	equityRows := [][]string{{"Step", "Equity"}}
	for i, eq := range results.EquityCurve {
		equityRows = append(equityRows, []string{
			fmt.Sprintf("%d", i+1),
			fmt.Sprintf("%.2f", eq),
		})
	}

	// Write files
	saveCSV("backtest_trades.csv", tradeRows)
	saveCSV("backtest_equity.csv", equityRows)
}

// saveCSV saves data to a CSV file
func saveCSV(filename string, rows [][]string) error {
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("Error creating CSV file %s: %v", filename, err)
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	for _, row := range rows {
		if err := w.Write(row); err != nil {
			log.Printf("Error writing to CSV file %s: %v", filename, err)
			return err
		}
	}

	log.Printf("Saved results to %s", filename)
	return nil
}
