// Package livetrading
package livetrading

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/exchange"
	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/position"
	"github.com/amirphl/simple-trader/internal/strategy"
	"github.com/amirphl/simple-trader/internal/tfutils"
)

// runLiveTrading handles the live trading mode
func RunLiveTrading(
	ctx context.Context,
	cfg config.Config,
	strats []strategy.Strategy,
	storage db.Storage,
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
func orderStatusChecker(ctx context.Context, storage db.Storage, ex exchange.Exchange, checkInterval time.Duration) {
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
					storage.LogEvent(ctx, db.Event{
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
					storage.LogEvent(ctx, db.Event{
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
	storage db.Storage,
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
		log.Printf("runTradingLoop | Failed to start trading loop with %s strategy: %v", strat.Name(), err)
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
					posDepthState[exchange.GetBuyDepthKey(symbol)] = *buyDepth
				}

				if sellOk && sellDepth != nil {
					posDepthState[exchange.GetSellDepthKey(symbol)] = *sellDepth
				}

				// Get latest market cap data if available
				marketCap, _ := marketCapState.Get(symbol)

				pos.OnTick(ctx, exchange.WallexTradeToTick(tick, symbol), posDepthState, marketCap)
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
		dur := tfutils.GetTimeframeDuration(strat.Timeframe())
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
					storage.LogEvent(ctx, db.Event{
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
						storage.LogEvent(ctx, db.Event{
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

				// Create market depth state for this signal
				posDepthState := make(map[string]exchange.OrderBook)
				// Get latest market depth data if available from exchange state
				buyDepth, buyOk := depthState.Get(symbol, "buyDepth")
				sellDepth, sellOk := depthState.Get(symbol, "sellDepth")
				if buyOk && buyDepth != nil {
					posDepthState[exchange.GetBuyDepthKey(symbol)] = *buyDepth
				}
				if sellOk && sellDepth != nil {
					posDepthState[exchange.GetSellDepthKey(symbol)] = *sellDepth
				}
				// Get latest market cap data if available
				marketCap, _ := marketCapState.Get(symbol)

				signal, err := strat.OnCandles(ctx, candle.DBCandlesToCandles(candles))
				if err != nil {
					log.Printf("runTradingLoop | [%s] Error processing candles: %v", strat.Name(), err)
					continue
				}
				pos.OnSignal(ctx, signal, posDepthState, marketCap)
				lastProcessed = candles[len(candles)-1].Timestamp
			}
		}
	}()

	wg.Wait()
	return nil
}

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
