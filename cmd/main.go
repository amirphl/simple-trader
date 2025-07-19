package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"strconv"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	dbconfig "github.com/amirphl/simple-trader/internal/db/conf"
	"github.com/amirphl/simple-trader/internal/exchange"
	"github.com/amirphl/simple-trader/internal/journal"
	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/position"
	"github.com/amirphl/simple-trader/internal/strategy"
	"github.com/lib/pq"
)

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
	dbCfg, err := dbconfig.NewConfig(cfg.DBConnStr, cfg.DBMaxOpen, cfg.DBMaxIdle)
	if err != nil {
		log.Fatalf("Failed to create DB config: %v", err)
	}

	storage, err := db.New(*dbCfg)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	log.Println("Connected to Postgres/TimescaleDB")

	// Set up notification system
	telegramNotifier := notifier.NewTelegramNotifier(cfg.TelegramToken, cfg.TelegramChatID, cfg.ProxyURL, cfg.NotificationRetries, cfg.NotificationDelay)

	// Create exchange connection
	ex := exchange.NewWallexExchange(cfg.WallexAPIKey, telegramNotifier)

	// Create strategies
	strats := strategy.New(cfg, storage)
	if len(strats) == 0 {
		log.Fatalf("No valid strategies configured. Check your configuration.")
	}

	switch cfg.Mode {
	case "live":
		runLiveTrading(ctx, cfg, strats, storage, ex, telegramNotifier)
	case "backtest":
		runBacktest(ctx, cfg, strats, storage)
	default:
		log.Fatalf("Unsupported mode: %s", cfg.Mode)
	}

	<-sigCh
	log.Println("Graceful shutdown initiated...")

	// Allow some time for cleanup
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	strats []strategy.Strategy,
	storage db.DB,
	ex exchange.Exchange,
	notifier notifier.Notifier,
) {
	// --- Wallex websocket tick streaming to positions ---
	tickChans := make(map[string]<-chan exchange.WallexTrade)
	websocketChannels := make(map[string]exchange.TradeChannel)

	// Market depth and market cap state managers
	depthState := exchange.NewMarketDepthState()
	marketCapState := exchange.NewMarketCapState()

	// Market depth and market cap watchers
	depthWatchers := make(map[string]exchange.DepthWatcher)
	marketCapWatchers := make(map[string]exchange.MarketCapWatcher)

	for _, symbol := range cfg.Symbols {
		// Trade channel
		tradeChan := exchange.NewWallexTradeChannel()
		tradeChan.Start(ctx, symbol)
		websocketChannels[symbol] = tradeChan

		// Subscribe to the channel for this symbol
		ch, err := tradeChan.Subscribe(symbol, 100)
		if err != nil {
			log.Fatalf("Failed to subscribe to %s websocket: %v", symbol, err)
		}
		tickChans[symbol] = ch

		// Market depth watchers (buy and sell)
		buyDepthWatcher := exchange.NewWallexDepthWatcher(depthState, symbol, "buyDepth")
		sellDepthWatcher := exchange.NewWallexDepthWatcher(depthState, symbol, "sellDepth")
		buyDepthWatcher.Start(ctx)
		sellDepthWatcher.Start(ctx)
		depthWatchers[symbol+"_buy"] = buyDepthWatcher
		depthWatchers[symbol+"_sell"] = sellDepthWatcher

		// Market cap watcher
		marketCapWatcher := exchange.NewWallexMarketCapWatcher(marketCapState, symbol)
		marketCapWatcher.Start(ctx)
		marketCapWatchers[symbol] = marketCapWatcher
	}

	// Start data logger for all symbols
	go dataLogger(ctx, cfg.Symbols, tickChans, depthState, marketCapState)

	// Create ingestion config with flexible timeframe support
	ingestionCfg := candle.DefaultIngestionConfig(cfg.Symbols, ex)
	ingester := candle.NewCandleIngester(storage)
	ingestionSvc := candle.NewIngestionService(ctx, ingester, ingestionCfg)
	if err := ingestionSvc.Start(); err != nil {
		log.Fatalf("runLiveTrading | Failed to start candle ingestion service: %v", err)
	}
	log.Println("runLiveTrading | Candle ingestion service started")

	// Setup recovery in case of panic
	defer func() {
		if r := recover(); r != nil {
			log.Printf("runLiveTrading | Recovered from panic in live trading: %v", r)
			// Try to notify about the panic
			notifier.Send(fmt.Sprintf("PANIC in trading system: %v", r))
			if ingestionSvc != nil {
				ingestionSvc.Stop()
			}
		}
	}()

	// Start order status checker
	const checkInterval = time.Minute
	go orderStatusChecker(ctx, storage, ex, checkInterval)

	// Monitor and report stats periodically
	const statsInterval = 3 * time.Minute
	go monitorIngestionStats(ctx, ingestionSvc, statsInterval)

	// Start trading for each strategy
	var wg sync.WaitGroup
	for _, strat := range strats {
		wg.Add(1)
		go func(s strategy.Strategy) {
			defer wg.Done()
			if err := runTradingLoop(ctx, cfg, s, storage, ex, notifier, websocketChannels, depthState, marketCapState); err != nil {
				log.Printf("runLiveTrading | Error running trading loop for %s: %v", s.Name(), err)
			}
		}(strat)
	}

	// Wait for all trading loops to complete
	wg.Wait()
}

// orderStatusChecker periodically checks the status of open orders
// and updates the database accordingly
func orderStatusChecker(ctx context.Context, storage db.DB, ex exchange.Exchange, checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	log.Println("orderStatusChecker |Starting order status checker")

	for {
		select {
		case <-ctx.Done():
			log.Println("orderStatusChecker | Order status checker stopped")
			return
		case <-ticker.C:
			// Get all open orders from database
			orders, err := storage.GetOpenOrders(ctx)
			if err != nil {
				log.Printf("orderStatusChecker | Failed to fetch open orders: %v", err)
				continue
			}

			if len(orders) == 0 {
				continue
			}

			log.Printf("orderStatusChecker | Checking status of %d open orders", len(orders))

			for _, o := range orders {
				orderResp, err := ex.GetOrderStatus(ctx, o.OrderID)
				if err != nil {
					log.Printf("orderStatusChecker | Error fetching order status for %s: %v", o.OrderID, err)
					continue
				}

				switch orderResp.Status {
				case "FILLED":
					log.Printf("orderStatusChecker | Order %s filled", o.OrderID)
					// TODO: Tx
					storage.LogEvent(ctx, journal.Event{
						Time:        time.Now(),
						Type:        "order",
						Description: "status_check_order_filled",
						Data:        map[string]any{"order": orderResp},
					})
					if err := storage.CloseOrder(ctx, o.OrderID); err != nil {
						log.Printf("orderStatusChecker | Failed to close order %s: %v", o.OrderID, err)
					}
				case "CANCELED", "EXPIRED", "REJECTED":
					log.Printf("orderStatusChecker | Order %s %s", o.OrderID, orderResp.Status)
					// TODO: Tx
					storage.LogEvent(ctx, journal.Event{
						Time:        time.Now(),
						Type:        "order",
						Description: "status_check_order_canceled_or_expired",
						Data:        map[string]any{"order": orderResp},
					})
					if err := storage.CloseOrder(ctx, o.OrderID); err != nil {
						log.Printf("orderStatusChecker | Failed to close order %s: %v", o.OrderID, err)
					}
				}
			}
		}
	}
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
	log.Println("monitorIngestionStats | Candle Ingestion Stats:")
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
	cfg config.Config,
	strat strategy.Strategy,
	storage db.DB,
	ex exchange.Exchange,
	notifier notifier.Notifier,
	websocketChannels map[string]exchange.TradeChannel,
	depthState exchange.MarketDepthStateManager,
	marketCapState exchange.MarketCapStateManager,
) error {
	log.Printf("runTradingLoop | Starting trading loop with %s strategy", strat.Name())

	// Load position manager
	pos, err := position.Load(strat.Name(), strat.Symbol(), storage, ex, notifier)
	if err != nil {
		log.Printf("runTradingLoop | Failed to start trading loop with %s strategy: %w", strat.Name(), err)
		return err
	}

	if pos.IsActive() {
		log.Printf("runTradingLoop | Position is active, stopping trading loop with %s strategy", strat.Name())
		return fmt.Errorf("position is active, stopping trading loop with %s strategy", strat.Name())
	}

	symbol := strat.Symbol()
	websocketChan, ok := websocketChannels[symbol]
	if !ok {
		log.Printf("runTradingLoop | No websocket channel found for symbol %s", symbol)
		return fmt.Errorf("no websocket channel found for symbol %s", symbol)
	}

	// Subscribe to the websocket channel for this strategy
	strategyID := fmt.Sprintf("%s-%s", strat.Name(), symbol)
	tickCh, err := websocketChan.Subscribe(strategyID, 100)
	if err != nil {
		log.Printf("runTradingLoop | Failed to subscribe to websocket for %s: %v", strategyID, err)
		return fmt.Errorf("failed to subscribe to websocket for %s: %w", strategyID, err)
	}
	defer websocketChan.Unsubscribe(strategyID)

	var wg sync.WaitGroup
	wg.Add(1)

	// Goroutine for feeding ticks
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case tick, ok := <-tickCh:
				if !ok {
					log.Printf("runTradingLoop | [%s] Tick channel closed", strat.Name())
					return
				}
				// Create market depth state for this tick
				posDepthState := make(map[string]exchange.OrderBook)

				// Get latest market depth data if available from exchange state
				buyDepth, buyOk := depthState.Get(symbol, "buyDepth")
				sellDepth, sellOk := depthState.Get(symbol, "sellDepth")

				if buyOk && buyDepth != nil {
					posDepthState[fmt.Sprintf("%s@buyDepth", exchange.NormalizeSymbol(symbol))] = *buyDepth
				}

				if sellOk && sellDepth != nil {
					posDepthState[fmt.Sprintf("%s@sellDepth", exchange.NormalizeSymbol(symbol))] = *sellDepth
				}

				// Get latest market cap data if available
				marketCap, _ := marketCapState.Get(symbol)

				pos.OnTick(ctx, wallexTradeToTick(tick), posDepthState, marketCap)
			}
		}
	}()

	// Goroutine for feeding signals (candle/strategy)
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Existing signal/candle logic (statusTicker, candle fetching, etc.)
		lastDay := time.Now().Day()
		dailyPnL := 0.0
		tradingDisabled := false
		dur := candle.GetTimeframeDuration(strat.Timeframe())
		statusTicker := time.NewTicker(30 * time.Second)
		defer statusTicker.Stop()
		var lastProcessed time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-statusTicker.C:
				currentDay := time.Now().Day()
				if currentDay != lastDay {
					tradingDisabled = false
					dailyPnL = 0.0
					lastDay = currentDay
					storage.LogEvent(ctx, journal.Event{
						Time:        time.Now(),
						Type:        "state",
						Description: "new_trading_day",
						Data: map[string]any{
							"symbol": strat.Symbol(),
							"day":    lastDay,
						},
					})
				}
				if cfg.MaxDailyLoss > 0 && dailyPnL <= -cfg.MaxDailyLoss {
					if !tradingDisabled {
						log.Printf("runTradingLoop | [%s] Trading disabled due to max daily loss reached", strat.Name())
						storage.LogEvent(ctx, journal.Event{
							Time:        time.Now(),
							Type:        "state",
							Description: "trading_disabled_max_daily_loss",
							Data: map[string]any{
								"strategy_name": strat.Name(),
								"day":           lastDay,
							},
						})
						tradingDisabled = true
					}
				}
				todayPnL, err := pos.DailyPnL()
				if err != nil {
					log.Printf("runTradingLoop | [%s] Error getting daily PnL: %v", strat.Name(), err)
				} else {
					dailyPnL = todayPnL
				}
				now := time.Now().UTC()
				var from time.Time
				if lastProcessed.IsZero() {
					from = now.AddDate(-1, 0, 0).Truncate(dur)
				} else {
					from = lastProcessed.Add(time.Second)
				}
				to := now
				candles, err := storage.GetCandles(ctx, symbol, strat.Timeframe(), "", from, to)
				if err != nil {
					log.Printf("runTradingLoop | [%s] Error fetching new candles: %v", strat.Name(), err)
					continue
				}
				if len(candles) == 0 {
					continue
				}
				signal, err := strat.OnCandles(ctx, candles)
				if err != nil {
					log.Printf("runTradingLoop | [%s] Error processing candles: %v", strat.Name(), err)
					continue
				}
				pos.OnSignalV2(ctx, signal)
				lastProcessed = candles[len(candles)-1].Timestamp
			}
		}
	}()

	wg.Wait()
	return nil
}

// runBacktest handles the backtest mode
func runBacktest(
	ctx context.Context,
	cfg config.Config,
	strats []strategy.Strategy,
	storage db.DB,
) {
	for _, strat := range strats {
		// Load candles for backtesting
		candles, err := loadBacktestCandles(ctx, storage, strat.Symbol(), strat.Timeframe(), cfg.BacktestFrom.Time, cfg.BacktestTo.Time, cfg.ProxyURL)
		if err != nil {
			log.Fatalf("runBacktest | Error loading candles for backtest: %v", err)
		}

		log.Printf("runBacktest | Loaded %d candles for backtest [%s-%s]",
			len(candles), cfg.BacktestFrom.Time.Format(time.RFC3339), cfg.BacktestTo.Time.Format(time.RFC3339))

		backtestResults := runStrategyBacktest(strat, candles, cfg)

		// Print backtest results
		printBacktestResults(strat, backtestResults)

		// Save backtest results to CSV
		saveBacktestResults(backtestResults)

	}
}

// loadBacktestCandles loads candles for backtesting, downloading from public API if necessary
func loadBacktestCandles(
	ctx context.Context,
	storage db.DB,
	symbol, timeframe string,
	from, to time.Time,
	proxyURL string,
) ([]candle.Candle, error) {
	// Try to load candles from database first
	candles, err := storage.GetCandles(ctx, symbol, timeframe, "", from, to.Add(-time.Nanosecond)) // ensure exclusive upper bound
	if err != nil {
		return nil, fmt.Errorf("loadBacktestCandles | error loading candles from database: %w", err)
	}

	// If no candles found in database, download from public API
	if len(candles) == 0 {
		log.Printf("loadBacktestCandles | No historical candles found in DB for %s, downloading from public API...", symbol)

		// Download candles in chunks to avoid hitting API limits
		currTime := from
		maxChunkDays := 14 // Download two weeks at a time
		allDownloadedCandles := make([]candle.Candle, 0)

		// Create a rate limiter - 1 request per 1 seconds to avoid rate limits
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		for currTime.Before(to) {
			next := currTime.Add(time.Duration(maxChunkDays) * 24 * time.Hour)
			if next.After(to) {
				next = to
			}

			// Wait for rate limiter
			<-ticker.C

			// Create a context with timeout for the API call
			downloadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			downloadedCandles, err := downloadCandlesFromPublicAPI(downloadCtx, symbol, timeframe, currTime, next, proxyURL)
			cancel()

			if err != nil {
				return nil, fmt.Errorf("error fetching candles from %s to %s: %w",
					currTime.Format(time.RFC3339), next.Format(time.RFC3339), err)
			}

			log.Printf("loadBacktestCandles | Downloaded %d candles for %s from %s to %s",
				len(downloadedCandles), symbol, currTime.Format("2006-01-02"), next.Format("2006-01-02"))

			// Add downloaded candles to our collection
			allDownloadedCandles = append(allDownloadedCandles, downloadedCandles...)

			currTime = next
		}

		if len(allDownloadedCandles) == 0 {
			return nil, fmt.Errorf("no candles available for %s from %s to %s",
				symbol, from.Format(time.RFC3339), to.Format(time.RFC3339))
		}

		// Process downloaded candles (sort, trim, generate missing, eliminate duplicates)
		processedCandles := processCandles(allDownloadedCandles, symbol, timeframe, from, to)

		// Save processed candles to database
		if len(processedCandles) > 0 {
			saveCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err = storage.SaveCandles(saveCtx, processedCandles)
			cancel()

			if err != nil {
				return nil, fmt.Errorf("error saving candles to database: %w", err)
			}

			log.Printf("loadBacktestCandles | Saved %d processed candles to database", len(processedCandles))
		}

		// Load the saved candles
		candles, err = storage.GetCandles(ctx, symbol, timeframe, "", from, to.Add(-time.Nanosecond))
		if err != nil {
			return nil, fmt.Errorf("loadBacktestCandles | error loading downloaded candles: %w", err)
		}
	}

	// Filter out any candle with timestamp >= to (exclusive upper bound)
	var filtered []candle.Candle
	for _, c := range candles {
		if c.Timestamp.Before(to) {
			filtered = append(filtered, c)
		}
	}

	return filtered, nil
}

// downloadCandlesFromPublicAPI downloads candles from a public cryptocurrency API
func downloadCandlesFromPublicAPI(ctx context.Context, symbol, timeframe string, start, end time.Time, proxyURL string) ([]candle.Candle, error) {
	// Map timeframe to API format
	var interval string
	switch timeframe {
	case "1m":
		interval = "1m"
	case "5m":
		interval = "5m"
	case "15m":
		interval = "15m"
	case "30m":
		interval = "30m"
	case "1h":
		interval = "1h"
	case "4h":
		interval = "4h"
	case "1d":
		interval = "1d"
	default:
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}

	// Convert symbol to uppercase and format for API
	apiSymbol := strings.ToUpper(strings.ReplaceAll(symbol, "-", ""))

	// Calculate start and end timestamps in milliseconds
	startMs := start.UnixNano() / int64(time.Millisecond)
	endMs := end.UnixNano() / int64(time.Millisecond)

	// Construct API URL (using Binance public API as an example)
	apiURL := fmt.Sprintf(
		"https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&startTime=%d&endTime=%d", // TODO: limit=1000
		apiSymbol, interval, startMs, endMs,
	)
	log.Printf("downloadCandlesFromPublicAPI | API URL: %s", apiURL)

	// Create HTTP client with timeout and optional proxy
	transport := &http.Transport{}
	if proxyURL != "" {
		proxyParsed, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyParsed)
	}
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: transport,
	}

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}
	// Set a browser-like User-Agent
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json")
	// Send request
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
	}

	// Read and log response body for debugging
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	// Parse response body
	var rawCandles [][]any
	if err := json.Unmarshal(bodyBytes, &rawCandles); err != nil {
		log.Printf("downloadCandlesFromPublicAPI | JSON decode error: %v", err)
		return nil, fmt.Errorf("error decoding response: %w", err)
	}

	// Convert raw candles to our Candle struct
	candles := make([]candle.Candle, 0, len(rawCandles))
	for _, raw := range rawCandles {
		if len(raw) < 6 {
			continue // Skip invalid entries
		}

		// Robustly parse timestamp
		var timestamp int64
		switch v := raw[0].(type) {
		case float64:
			timestamp = int64(v)
		case string:
			timestamp, err = strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.Printf("downloadCandlesFromPublicAPI | Error parsing timestamp string: %v", err)
				continue
			}
		default:
			log.Printf("downloadCandlesFromPublicAPI | Unexpected timestamp type: %T", v)
			continue
		}

		// Robustly parse open, high, low, close, volume
		parseNum := func(val any) float64 {
			switch n := val.(type) {
			case float64:
				return n
			case string:
				f, err := strconv.ParseFloat(n, 64)
				if err != nil {
					log.Printf("downloadCandlesFromPublicAPI | Error parsing float string: %v", err)
					return 0
				}
				return f
			default:
				log.Printf("downloadCandlesFromPublicAPI | Unexpected number type: %T", n)
				return 0
			}
		}
		open := parseNum(raw[1])
		high := parseNum(raw[2])
		low := parseNum(raw[3])
		close := parseNum(raw[4])
		volume := parseNum(raw[5])

		// Create candle
		c := candle.Candle{
			Timestamp: time.Unix(timestamp/1000, 0).UTC(),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
			Symbol:    symbol,
			Timeframe: timeframe,
			Source:    "binance",
		}

		candles = append(candles, c)
	}

	return candles, nil
}

// processCandles sorts, trims, generates missing candles, and eliminates duplicates
func processCandles(candles []candle.Candle, symbol, timeframe string, start, to time.Time) []candle.Candle {
	if len(candles) == 0 {
		return candles
	}

	// 1. Sort candles by timestamp
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp.Before(candles[j].Timestamp)
	})

	// 2. Eliminate duplicates using a map
	candleMap := make(map[time.Time]candle.Candle)
	for _, c := range candles {
		// Truncate timestamp to timeframe
		duration := candle.GetTimeframeDuration(timeframe)
		c.Timestamp = c.Timestamp.Truncate(duration)

		// Only keep the first occurrence of each timestamp
		if _, exists := candleMap[c.Timestamp]; !exists {
			candleMap[c.Timestamp] = c
		}
	}

	// 3. Trim candles to requested time range (exclusive upper bound)
	var trimmedCandles []candle.Candle
	for ts, c := range candleMap {
		if (ts.Equal(start) || ts.After(start)) && ts.Before(to) {
			trimmedCandles = append(trimmedCandles, c)
		}
	}

	// Re-sort after trimming
	sort.Slice(trimmedCandles, func(i, j int) bool {
		return trimmedCandles[i].Timestamp.Before(trimmedCandles[j].Timestamp)
	})

	// 4. Generate missing candles
	if len(trimmedCandles) == 0 {
		return trimmedCandles
	}

	var completeCandles []candle.Candle
	duration := candle.GetTimeframeDuration(timeframe)

	// Start from the first candle's timestamp
	currentTime := trimmedCandles[0].Timestamp
	lastTime := trimmedCandles[len(trimmedCandles)-1].Timestamp

	// Use the first candle's close price as base for synthetic candles
	basePrice := trimmedCandles[0].Close

	i := 0 // Index for trimmedCandles
	for !currentTime.After(lastTime) && currentTime.Before(to) {
		if i < len(trimmedCandles) && trimmedCandles[i].Timestamp.Equal(currentTime) {
			// We have a real candle for this timestamp
			completeCandles = append(completeCandles, trimmedCandles[i])
			basePrice = trimmedCandles[i].Close // Update base price for future synthetic candles
			i++
		} else {
			// Create synthetic candle
			syntheticCandle := candle.Candle{
				Timestamp: currentTime,
				Open:      basePrice,
				High:      basePrice,
				Low:       basePrice,
				Close:     basePrice,
				Volume:    0, // Synthetic candles have zero volume
				Symbol:    symbol,
				Timeframe: timeframe,
				Source:    "synthetic",
			}
			completeCandles = append(completeCandles, syntheticCandle)
		}

		// Move to next time interval
		currentTime = currentTime.Add(duration)
	}

	return completeCandles
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

type CandleWithSignal struct {
	Candle candle.Candle    `json:"candle"`
	Signal *strategy.Signal `json:"signal,omitempty"`
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

	// For charting: store all candles and their signals
	var candlesWithSignals []CandleWithSignal

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
		sig, _ := strat.OnCandles(ctx, []candle.Candle{c})
		cancel()
		signals = append(signals, sig)

		var signalPtr *strategy.Signal
		if sig.Action == "buy" || sig.Action == "sell" {
			signalPtr = &sig
		}
		candlesWithSignals = append(candlesWithSignals, CandleWithSignal{Candle: c, Signal: signalPtr})

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

	// Output candlesWithSignals as JSON for charting
	chartDataFile := "backtest_chart_data.json"
	f, err := os.Create(chartDataFile)
	if err == nil {
		json.NewEncoder(f).Encode(candlesWithSignals)
		f.Close()
		log.Printf("Saved chart data to %s", chartDataFile)
	} else {
		log.Printf("Failed to save chart data: %v", err)
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

// Helper to adapt WallexTrade to position.Tick interface
func wallexTradeToTick(trade exchange.WallexTrade) position.Tick {
	price, _ := strconv.ParseFloat(trade.Price, 64)
	ts := trade.Timestamp
	return &simpleTick{price: price, timestamp: ts}
}

type simpleTick struct {
	price     float64
	timestamp time.Time
}

func (t *simpleTick) Price() float64       { return t.price }
func (t *simpleTick) Timestamp() time.Time { return t.timestamp }

// dataLogger writes trade data, market depth, and market cap to files
func dataLogger(ctx context.Context, symbols []string, tickChans map[string]<-chan exchange.WallexTrade, depthState exchange.MarketDepthStateManager, marketCapState exchange.MarketCapStateManager) {
	// Create data directory if it doesn't exist
	dataDir := "data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Printf("Failed to create data directory: %v", err)
		return
	}

	// Create files for each symbol
	files := make(map[string]*os.File)
	defer func() {
		for _, file := range files {
			if file != nil {
				file.Close()
			}
		}
	}()

	// Initialize files for each symbol
	for _, symbol := range symbols {
		filename := fmt.Sprintf("%s/%s_trades.csv", dataDir, strings.ToLower(symbol))

		// Check if file exists to determine if we need to write header
		fileExists := false
		if stat, err := os.Stat(filename); err == nil {
			fileExists = stat.Size() > 0
		}

		// Open file in append mode (O_APPEND ensures we append, not overwrite)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Printf("Failed to open file for %s: %v", symbol, err)
			continue
		}
		files[symbol] = file

		// Write CSV header only if file is new/empty
		if !fileExists {
			header := "timestamp,price,quantity,is_buy_order,symbol,market_depth_buy,market_depth_sell,market_cap_last_price,market_cap_24h_volume,market_cap_bid_price,market_cap_ask_price\n"
			if _, err := file.WriteString(header); err != nil {
				log.Printf("Failed to write header for %s: %v", symbol, err)
			}
			log.Printf("Created new trade log file: %s", filename)
		} else {
			log.Printf("Appending to existing trade log file: %s", filename)
		}
	}

	// Process trades from all symbols
	for {
		select {
		case <-ctx.Done():
			log.Println("Data logger shutting down...")
			return
		default:
			// Check each symbol's trade channel
			for symbol, tickCh := range tickChans {
				select {
				case trade, ok := <-tickCh:
					if !ok {
						log.Printf("Trade channel closed for %s", symbol)
						continue
					}

					// Get latest market depth and market cap data
					buyDepth, _ := depthState.Get(symbol, "buyDepth")
					sellDepth, _ := depthState.Get(symbol, "sellDepth")
					marketCap, _ := marketCapState.Get(symbol)

					// Format the data line
					line := formatTradeData(trade, symbol, buyDepth, sellDepth, marketCap)

					// Write to file
					if file, exists := files[symbol]; exists {
						if _, err := file.WriteString(line + "\n"); err != nil {
							log.Printf("Failed to write trade data for %s: %v", symbol, err)
							continue
						}
						file.Sync() // Ensure data is written immediately
					}

				default:
					// No trade available, continue to next symbol
					continue
				}
			}

			// Small delay to prevent busy waiting
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// formatTradeData formats trade data with market depth and market cap information
func formatTradeData(trade exchange.WallexTrade, symbol string, buyDepth *exchange.OrderBook, sellDepth *exchange.OrderBook, marketCap *exchange.MarketCapData) string {
	// Extract best prices from order books
	var bestBuyPrice, bestSellPrice string
	if buyDepth != nil && len(*buyDepth) > 0 {
		// Find highest buy price (best buy)
		var maxPrice float64
		for _, entry := range *buyDepth {
			if price, err := strconv.ParseFloat(entry.Price, 64); err == nil && price > maxPrice {
				maxPrice = price
				bestBuyPrice = entry.Price
			}
		}
	}

	if sellDepth != nil && len(*sellDepth) > 0 {
		// Find lowest sell price (best sell)
		var minPrice float64
		first := true
		for _, entry := range *sellDepth {
			if price, err := strconv.ParseFloat(entry.Price, 64); err == nil {
				if first || price < minPrice {
					minPrice = price
					bestSellPrice = entry.Price
					first = false
				}
			}
		}
	}

	// Extract market cap data
	var lastPrice, volume24h, bidPrice, askPrice string
	if marketCap != nil {
		lastPrice = marketCap.LastPrice
		volume24h = marketCap.Volume24h
		bidPrice = marketCap.BidPrice
		askPrice = marketCap.AskPrice
	}

	// Format CSV line
	return fmt.Sprintf("%s,%s,%s,%t,%s,%s,%s,%s,%s,%s,%s",
		trade.Timestamp.Format("2006-01-02 15:04:05.000"),
		trade.Price,
		trade.Quantity,
		trade.IsBuyOrder,
		symbol,
		bestBuyPrice,
		bestSellPrice,
		lastPrice,
		volume24h,
		bidPrice,
		askPrice,
	)
}
