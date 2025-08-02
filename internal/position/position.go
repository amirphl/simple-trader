// Package position
package position

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/exchange"
	"github.com/amirphl/simple-trader/internal/utils"

	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/strategy"
)

// executeWithTransaction executes a function with proper transaction management
// If a transaction exists in context, it uses that. Otherwise, it creates a new one.
func executeWithTransaction(ctx context.Context, storage db.Storage, fn func(context.Context) error) error {
	// Check if transaction exists in context
	if tx := db.GetTransaction(ctx); tx != nil {
		// Use existing transaction
		return fn(ctx)
	}

	// Create new transaction
	dbConn := storage.GetDB()
	tx, err := dbConn.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Create context with transaction
	txCtx := db.WithTransaction(ctx, tx)

	// Execute the function
	if fnErr := fn(txCtx); fnErr != nil {
		// Rollback on error
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("transaction rollback failed: %w (original error: %v)", rbErr, fnErr)
		}
		return fnErr
	}

	// Commit on success
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("transaction commit failed: %w", commitErr)
	}

	return nil
}

type Position interface {
	OnSignal(ctx context.Context, signal strategy.Signal, depthState map[string]exchange.OrderBook, marketCapState *exchange.MarketCap)
	OnTick(ctx context.Context, tick exchange.Tick, depthState map[string]exchange.OrderBook, marketCapState *exchange.MarketCap)
	Stats() (map[string]any, error)
	GetLastPNL() float64
	DailyPnL() (float64, error)
	IsActive() bool
}

// TODO: Transactional updates, atomicity, order status check, commission, slippage, exchange ctx, etc.

// New creates a new position manager
func New(cfg config.Config, strategyName, symbol string, storage db.Storage, exchange exchange.Exchange, notifier notifier.Notifier) Position {
	// Get risk parameters for this strategy
	riskParams := config.GetRiskParams(cfg, strategyName)

	pos := &position{
		StrategyName: strategyName,
		Symbol:       symbol,
		Time:         time.Now(),

		RiskParams: riskParams,

		Active:          false,
		TradingDisabled: false,

		Exchange: exchange,
		Storage:  storage,
		Notifier: notifier,

		Balance: riskParams.InitialBalance,

		LiveWinPnls:     make([]float64, 0),
		LiveLossPnls:    make([]float64, 0),
		LiveEquityCurve: make([]float64, 0),
		LiveTradeLog:    make([]Trade, 0),
	}

	// Set order configuration
	pos.OrderSpec.Type = cfg.OrderType
	pos.OrderSpec.MaxAttempts = 3
	pos.OrderSpec.Delay = time.Millisecond * 100

	return pos
}

// position represents a trading position with risk management and statistics
type position struct {
	// Database ID
	ID int64 `json:"id"`

	// Core position data
	StrategyName string    `json:"strategy_name"`
	Symbol       string    `json:"symbol"`
	Side         string    `json:"side"`
	Entry        float64   `json:"entry"`
	Size         float64   `json:"size"`
	OrderID      string    `json:"order_id"`
	Time         time.Time `json:"time"`

	// Risk management
	RiskParams config.RiskParams `json:"risk_params"`

	Active bool `json:"active"`

	TradingDisabled bool `json:"trading_disabled"`

	// Order configuration
	OrderSpec struct {
		Type        string        `json:"type"`
		MaxAttempts int           `json:"max_attempts"`
		Delay       time.Duration `json:"delay"`
	} `json:"order_spec"`

	// Dependencies (not serialized)
	Exchange exchange.Exchange `json:"-"`
	Storage  db.Storage        `json:"-"`
	Notifier notifier.Notifier `json:"-"`

	Balance float64 `json:"balance"`

	// Statistics
	LastPNL      float64 `json:"acc_pnl"`
	MeanPNL      float64 `json:"mean_pnl"`
	StdPNL       float64 `json:"std_pnl"`
	Sharpe       float64 `json:"sharpe"`
	Expectancy   float64 `json:"expectancy"`
	TrailingStop float64 `json:"trailing_stop"`

	LiveEquity      float64 `json:"live_equity"`
	LiveMaxEquity   float64 `json:"live_max_equity"`
	LiveMaxDrawdown float64 `json:"live_max_drawdown"`

	LiveWins   int64 `json:"live_wins"`
	LiveLosses int64 `json:"live_losses"`
	LiveTrades int64 `json:"live_trades"`

	LiveWinRate  float64 `json:"live_win_rate"`
	ProfitFactor float64 `json:"profit_factor"`

	LiveWinPnls     []float64 `json:"live_win_pnls"`
	LiveLossPnls    []float64 `json:"live_loss_pnls"`
	LiveEquityCurve []float64 `json:"live_equity_curve"`
	LiveTradeLog    []Trade   `json:"live_trade_log"`

	// Market data cache for optimal order calculations
	depthState         map[string]exchange.OrderBook `json:"-"`
	lastMarketCap      *exchange.MarketCap           `json:"-"`
	lastMarketDataTime time.Time                     `json:"-"`

	mu sync.RWMutex // Added mutex for thread safety
}

// backupState creates a backup of the current position state
func (p *position) backupState() []byte {
	// serialize p and return it
	json, err := json.Marshal(p)
	if err != nil {
		return nil
	}
	return json
}

// rollbackState restores the position state from backup
func (p *position) rollbackState(b []byte) {
	// deserialize backup
	var backup position
	err := json.Unmarshal(b, &backup)
	if err != nil {
		return
	}

	p.ID = backup.ID
	p.Side = backup.Side
	p.Entry = backup.Entry
	p.Size = backup.Size
	p.OrderID = backup.OrderID
	p.Time = backup.Time
	p.Active = backup.Active
	p.Balance = backup.Balance
	p.LastPNL = backup.LastPNL
	p.MeanPNL = backup.MeanPNL
	p.StdPNL = backup.StdPNL
	p.Sharpe = backup.Sharpe
	p.Expectancy = backup.Expectancy
	p.LiveEquity = backup.LiveEquity
	p.LiveMaxEquity = backup.LiveMaxEquity
	p.LiveMaxDrawdown = backup.LiveMaxDrawdown
	p.LiveWins = backup.LiveWins
	p.LiveLosses = backup.LiveLosses
	p.LiveTrades = backup.LiveTrades
	p.LiveWinRate = backup.LiveWinRate
	p.LiveTradeLog = backup.LiveTradeLog
	p.LiveEquityCurve = backup.LiveEquityCurve
	p.LiveWinPnls = backup.LiveWinPnls
	p.LiveLossPnls = backup.LiveLossPnls
	p.ProfitFactor = backup.ProfitFactor
	p.TrailingStop = backup.TrailingStop
	p.RiskParams = backup.RiskParams
	p.TradingDisabled = backup.TradingDisabled
}

// Trade represents a single trade record
type Trade struct {
	Entry     float64   `json:"entry"`
	Exit      float64   `json:"exit"`
	PnL       float64   `json:"pnl"`
	EntryTime time.Time `json:"entry_time"`
	ExitTime  time.Time `json:"exit_time"`
}

// Save persists the position to database with error handling
func (p *position) save(ctx context.Context) error {
	// Convert position to db.Position
	dbPos := p.toDBPosition()

	// Save to database
	if p.Storage != nil {
		// Reset ID for new position
		dbPos.ID = 0
		id, err := p.Storage.SavePosition(ctx, dbPos)
		if err != nil {
			return fmt.Errorf("failed to save position to database [%s %s]: %w", p.StrategyName, p.Symbol, err)
		}
		// Store the generated ID
		p.ID = id
	}

	return nil
}

func (p *position) update(ctx context.Context) error {
	// Convert position to db.Position
	dbPos := p.toDBPosition()

	// Update in database
	if p.Storage != nil {
		if err := p.Storage.UpdatePosition(ctx, dbPos); err != nil {
			return fmt.Errorf("failed to update position in database [%s %s]: %w", p.StrategyName, p.Symbol, err)
		}
	}

	return nil
}

// Load loads a position from database with proper error handling
// TODO: Provide orderbook and marketcap data to load function
func Load(ctx context.Context, stratName, symbol string, storage db.Storage, exchange exchange.Exchange, notifier notifier.Notifier) (Position, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage is required to load position")
	}

	dbPos, err := storage.GetPosition(ctx, stratName, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to load position from database [%s %s]: %w", stratName, symbol, err)
	}

	if dbPos == nil {
		return nil, fmt.Errorf("position not found in database [%s %s]", stratName, symbol)
	}

	// Convert db.Position to position
	pos := fromDBPosition(*dbPos, storage, exchange, notifier)

	return pos, nil
}

// toDBPosition converts position for database storage
func (p *position) toDBPosition() db.Position {
	// Convert Trade structs to TradeData
	tradeLog := make([]db.Trade, len(p.LiveTradeLog))
	for i, trade := range p.LiveTradeLog {
		tradeLog[i] = db.Trade{
			Entry:     trade.Entry,
			Exit:      trade.Exit,
			PnL:       trade.PnL,
			EntryTime: trade.EntryTime,
			ExitTime:  trade.ExitTime,
		}
	}

	// Convert RiskParams to map
	riskParams := map[string]any{
		"initial_balance":       p.RiskParams.InitialBalance,
		"risk_percent":          p.RiskParams.RiskPercent,
		"stop_loss_percent":     p.RiskParams.StopLossPercent,
		"take_profit_percent":   p.RiskParams.TakeProfitPercent,
		"trailing_stop_percent": p.RiskParams.TrailingStopPercent,
		"max_daily_loss":        p.RiskParams.MaxDailyLoss,
		"limit_spread":          p.RiskParams.LimitSpread,
	}

	// Convert OrderSpec to map
	orderSpec := map[string]any{
		"type":         p.OrderSpec.Type,
		"max_attempts": p.OrderSpec.MaxAttempts,
		"delay":        p.OrderSpec.Delay.String(),
	}

	return db.Position{
		ID:              p.ID,
		StrategyName:    p.StrategyName,
		Symbol:          p.Symbol,
		Side:            p.Side,
		Entry:           p.Entry,
		Size:            p.Size,
		OrderID:         p.OrderID,
		Time:            p.Time,
		Active:          p.Active,
		TradingDisabled: p.TradingDisabled,
		Balance:         p.Balance,
		LastPNL:         p.LastPNL,
		MeanPNL:         p.MeanPNL,
		StdPNL:          p.StdPNL,
		Sharpe:          p.Sharpe,
		Expectancy:      p.Expectancy,
		TrailingStop:    p.TrailingStop,
		LiveEquity:      p.LiveEquity,
		LiveMaxEquity:   p.LiveMaxEquity,
		LiveMaxDrawdown: p.LiveMaxDrawdown,
		LiveWins:        p.LiveWins,
		LiveLosses:      p.LiveLosses,
		LiveTrades:      p.LiveTrades,
		LiveWinRate:     p.LiveWinRate,
		ProfitFactor:    p.ProfitFactor,
		LiveWinPnls:     p.LiveWinPnls,
		LiveLossPnls:    p.LiveLossPnls,
		LiveEquityCurve: p.LiveEquityCurve,
		LiveTradeLog:    tradeLog,
		RiskParams:      riskParams,
		OrderSpec:       orderSpec,
		CreatedAt:       time.Now(),
		UpdatedAt:       time.Now(),
	}
}

// fromDBPosition converts db.Position to position
func fromDBPosition(dbPos db.Position, storage db.Storage, exchange exchange.Exchange, notifier notifier.Notifier) *position {
	// Convert TradeData back to Trade
	tradeLog := make([]Trade, len(dbPos.LiveTradeLog))
	for i, tradeData := range dbPos.LiveTradeLog {
		tradeLog[i] = Trade{
			Entry:     tradeData.Entry,
			Exit:      tradeData.Exit,
			PnL:       tradeData.PnL,
			EntryTime: tradeData.EntryTime,
			ExitTime:  tradeData.ExitTime,
		}
	}

	// Convert RiskParams from map
	riskParams := config.RiskParams{}
	if initialBalance, ok := dbPos.RiskParams["initial_balance"].(float64); ok {
		riskParams.InitialBalance = initialBalance
	}
	if riskPercent, ok := dbPos.RiskParams["risk_percent"].(float64); ok {
		riskParams.RiskPercent = riskPercent
	}
	if stopLossPercent, ok := dbPos.RiskParams["stop_loss_percent"].(float64); ok {
		riskParams.StopLossPercent = stopLossPercent
	}
	if takeProfitPercent, ok := dbPos.RiskParams["take_profit_percent"].(float64); ok {
		riskParams.TakeProfitPercent = takeProfitPercent
	}
	if trailingStopPercent, ok := dbPos.RiskParams["trailing_stop_percent"].(float64); ok {
		riskParams.TrailingStopPercent = trailingStopPercent
	}
	if maxDailyLoss, ok := dbPos.RiskParams["max_daily_loss"].(float64); ok {
		riskParams.MaxDailyLoss = maxDailyLoss
	}
	if limitSpread, ok := dbPos.RiskParams["limit_spread"].(float64); ok {
		riskParams.LimitSpread = limitSpread
	}

	// Convert OrderSpec from map
	var orderType string
	var maxAttempts int
	var delay time.Duration
	if typeStr, ok := dbPos.OrderSpec["type"].(string); ok {
		orderType = typeStr
	}
	if maxAttemptsVal, ok := dbPos.OrderSpec["max_attempts"].(float64); ok {
		maxAttempts = int(maxAttemptsVal)
	}
	if delayStr, ok := dbPos.OrderSpec["delay"].(string); ok {
		if parsedDelay, err := time.ParseDuration(delayStr); err == nil {
			delay = parsedDelay
		}
	}

	pos := &position{
		ID:              dbPos.ID,
		StrategyName:    dbPos.StrategyName,
		Symbol:          dbPos.Symbol,
		Side:            dbPos.Side,
		Entry:           dbPos.Entry,
		Size:            dbPos.Size,
		OrderID:         dbPos.OrderID,
		Time:            dbPos.Time,
		Active:          dbPos.Active,
		TradingDisabled: dbPos.TradingDisabled,
		Balance:         dbPos.Balance,
		LastPNL:         dbPos.LastPNL,
		MeanPNL:         dbPos.MeanPNL,
		StdPNL:          dbPos.StdPNL,
		Sharpe:          dbPos.Sharpe,
		Expectancy:      dbPos.Expectancy,
		TrailingStop:    dbPos.TrailingStop,
		LiveEquity:      dbPos.LiveEquity,
		LiveMaxEquity:   dbPos.LiveMaxEquity,
		LiveMaxDrawdown: dbPos.LiveMaxDrawdown,
		LiveWins:        dbPos.LiveWins,
		LiveLosses:      dbPos.LiveLosses,
		LiveTrades:      dbPos.LiveTrades,
		LiveWinRate:     dbPos.LiveWinRate,
		ProfitFactor:    dbPos.ProfitFactor,
		LiveWinPnls:     dbPos.LiveWinPnls,
		LiveLossPnls:    dbPos.LiveLossPnls,
		LiveEquityCurve: dbPos.LiveEquityCurve,
		LiveTradeLog:    tradeLog,
		RiskParams:      riskParams,
		Exchange:        exchange,
		Storage:         storage,
		Notifier:        notifier,
	}

	// Set OrderSpec
	pos.OrderSpec.Type = orderType
	pos.OrderSpec.MaxAttempts = maxAttempts
	pos.OrderSpec.Delay = delay

	return pos
}

// IsActive returns whether the position is active (thread-safe)
func (p *position) IsActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Active
}

// GetLastPNL returns the current PNL (thread-safe)
func (p *position) GetLastPNL() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.LastPNL
}

// DailyPnL returns the PnL for the current trading day
func (p *position) DailyPnL() (float64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Calculate daily PnL from trades that occurred today
	var dailyPnL float64
	today := time.Now().UTC().Truncate(24 * time.Hour)

	for _, trade := range p.LiveTradeLog {
		if trade.ExitTime.After(today) || trade.ExitTime.Equal(today) {
			dailyPnL += trade.PnL
		}
	}

	return dailyPnL, nil
}

// exit executes an exit order atomically with error handling
func (p *position) exit(ctx context.Context, price float64, reason string) error {
	orderReq := p.createExitOrder(price)
	orderResp, err := p.Exchange.SubmitOrderWithRetry(ctx, orderReq, p.OrderSpec.MaxAttempts, p.OrderSpec.Delay)

	// TODO: Handle order status properly

	if err != nil {
		p.handleOrderError(ctx, err, reason)
		return fmt.Errorf("failed to submit [%s] order: %w", reason, err)
	}

	// Update position state
	p.handleExitSuccess(ctx, orderResp, reason)
	p.onPositionClose(ctx, orderResp.AvgPrice)
	p.updateLiveStats()

	return nil
}

// enter executes an entry order atomically with error handling
func (p *position) enter(ctx context.Context, signal strategy.Signal) error {
	// Fetch current exchange balances
	balances, err := p.Exchange.FetchBalances(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch balances: %w", err)
	}

	// Extract quote currency from symbol (e.g., "ETH/DAI" -> "DAI")
	quoteCurrency := exchange.ExtractQuoteCurrency(p.Symbol)
	if quoteCurrency == "" {
		return fmt.Errorf("invalid symbol format: %s", p.Symbol)
	}

	// Get available quote currency balance
	quoteBalance, exists := balances[quoteCurrency]
	if !exists {
		return fmt.Errorf("quote currency %s not found in balances", quoteCurrency)
	}

	// Calculate balance to spend based on risk management
	availableBalance := quoteBalance.Available
	initialBalance := p.RiskParams.InitialBalance
	currentBalance := p.Balance

	// Calculate the minimum of available quote currency, initial balance, and current balance
	balanceToSpend := min(availableBalance, initialBalance, currentBalance)

	// Apply risk percentage
	riskAmount := balanceToSpend * p.RiskParams.RiskPercent / 100

	// Check if we have enough balance to enter the position
	if riskAmount <= 0 {
		return fmt.Errorf("insufficient balance to enter position: available=%.2f, initial=%.2f, current=%.2f, risk_amount=%.2f",
			availableBalance, initialBalance, currentBalance, riskAmount)
	}

	utils.GetLogger().Printf("Position | [%s %s] Entering position with balance: available=%.2f, initial=%.2f, current=%.2f, risk_amount=%.2f",
		p.Symbol, p.StrategyName, availableBalance, initialBalance, currentBalance, riskAmount)

	// Support both long and short positions based on signal
	side := "buy"
	if signal.Position == strategy.ShortBearish {
		side = "sell"
	}

	// Calculate optimal order price and quantity using stored market data
	optimalPrice, optimalQuantity, err := p.calculateOptimalOrderParams(signal, riskAmount, side)
	if err != nil {
		return fmt.Errorf("failed to calculate optimal order parameters: %w", err)
	}

	// Create entry order with calculated optimal parameters
	orderReq := p.createEntryOrder(optimalPrice, optimalQuantity, side)
	orderResp, err := p.Exchange.SubmitOrderWithRetry(ctx, orderReq, p.OrderSpec.MaxAttempts, p.OrderSpec.Delay)

	// TODO: Handle order status properly

	if err != nil {
		p.handleOrderError(ctx, err, "entry_order_submission")
		return fmt.Errorf("failed to submit entry order: %w", err)
	}

	p.handleEntrySuccess(ctx, orderResp, side)

	return nil
}

// updateMarketData updates the stored market data for optimal order calculations
func (p *position) updateMarketData(depthState map[string]exchange.OrderBook, marketCapState *exchange.MarketCap) {
	// Store order book data
	if depthState != nil {
		p.depthState = depthState
		p.lastMarketDataTime = time.Now()
	}

	// Store market cap data
	if marketCapState != nil {
		p.lastMarketCap = marketCapState
	}
}

// getMarketDataAge returns the age of the stored market data
func (p *position) getMarketDataAge() time.Duration {
	return time.Since(p.lastMarketDataTime)
}

// calculateOptimalOrderParams calculates optimal price and quantity for order entry
// using stored market cap data, order book information, and slippage calculations
func (p *position) calculateOptimalOrderParams(signal strategy.Signal, riskAmount float64, side string) (float64, float64, error) {
	var optimalPrice float64
	var optimalQuantity float64

	// Use stored order book data or fall back to signal price
	if p.depthState == nil {
		utils.GetLogger().Printf("Position | [%s %s] No stored order book data available, using signal price", p.Symbol, p.StrategyName)
		optimalPrice = signal.TriggerPrice
		optimalQuantity = riskAmount / signal.TriggerPrice
		return optimalPrice, optimalQuantity, nil
	}

	// Calculate slippage-adjusted price
	slippagePercent := p.RiskParams.LimitSpread // Use limit spread as slippage
	// if slippagePercent <= 0 {
	// 	slippagePercent = 0.1 // Default 0.1% slippage
	// }

	if side == "buy" {
		// For buy orders, use ask price (lowest sell price)
		sellDepth, sellExists := p.depthState[exchange.GetSellDepthKey(p.Symbol)]
		if !sellExists || len(sellDepth) == 0 {
			utils.GetLogger().Printf("Position | [%s %s] No asks in order book, using signal price", p.Symbol, p.StrategyName)
			optimalPrice = signal.TriggerPrice
			optimalQuantity = riskAmount / signal.TriggerPrice
			return optimalPrice, optimalQuantity, nil
		}

		// Find the lowest ask price (best ask)
		var bestAskPrice float64
		foundBestAsk := false

		for priceStr, _ := range sellDepth {
			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				continue
			}
			if !foundBestAsk || price < bestAskPrice {
				bestAskPrice = price
				foundBestAsk = true
			}
		}

		if !foundBestAsk {
			utils.GetLogger().Printf("Position | [%s %s] No valid ask prices found, using signal price", p.Symbol, p.StrategyName)
			optimalPrice = signal.TriggerPrice
			optimalQuantity = riskAmount / signal.TriggerPrice
			return optimalPrice, optimalQuantity, nil
		}

		// Calculate total available quantity within slippage range
		totalAvailable := 0.0
		for priceStr, entry := range sellDepth {
			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				continue
			}
			if price <= bestAskPrice*(1+slippagePercent/100) {
				totalAvailable += entry.Quantity
			} else {
				break
			}
		}

		// Calculate optimal price with slippage
		optimalPrice = bestAskPrice * (1 + slippagePercent/100)
		optimalQuantity = totalAvailable

		utils.GetLogger().Printf("Position | [%s %s] Buy order: ask_price=%.2f, slippage_price=%.2f, available_qty=%.6f",
			p.Symbol, p.StrategyName, bestAskPrice, optimalPrice, optimalQuantity)

	} else {
		// For sell orders, use bid price (highest buy price)
		buyDepth, buyExists := p.depthState[exchange.GetBuyDepthKey(p.Symbol)]
		if !buyExists || len(buyDepth) == 0 {
			utils.GetLogger().Printf("Position | [%s %s] No bids in order book, using signal price", p.Symbol, p.StrategyName)
			optimalPrice = signal.TriggerPrice
			optimalQuantity = riskAmount / signal.TriggerPrice
			return optimalPrice, optimalQuantity, nil
		}

		// Find the highest bid price (best bid)
		var bestBidPrice float64
		foundBestBid := false

		for priceStr, _ := range buyDepth {
			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				continue
			}
			if !foundBestBid || price > bestBidPrice {
				bestBidPrice = price
				foundBestBid = true
			}
		}

		if !foundBestBid {
			utils.GetLogger().Printf("Position | [%s %s] No valid bid prices found, using signal price", p.Symbol, p.StrategyName)
			optimalPrice = signal.TriggerPrice
			optimalQuantity = riskAmount / signal.TriggerPrice
			return optimalPrice, optimalQuantity, nil
		}

		// Calculate total available quantity within slippage range
		totalAvailable := 0.0
		for priceStr, entry := range buyDepth {
			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				continue
			}
			if price >= bestBidPrice*(1-slippagePercent/100) {
				totalAvailable += entry.Quantity
			} else {
				break
			}
		}

		// Calculate optimal price with slippage
		optimalPrice = bestBidPrice * (1 - slippagePercent/100)
		optimalQuantity = totalAvailable

		utils.GetLogger().Printf("Position | [%s %s] Sell order: bid_price=%.2f, slippage_price=%.2f, available_qty=%.6f",
			p.Symbol, p.StrategyName, bestBidPrice, optimalPrice, optimalQuantity)
	}

	// Calculate order quantity based on risk amount and optimal price
	orderQuantity := riskAmount / optimalPrice

	// Check if we have enough liquidity
	if orderQuantity > optimalQuantity {
		utils.GetLogger().Printf("Position | [%s %s] Warning: Order quantity (%.6f) exceeds available liquidity (%.6f), reducing quantity",
			p.Symbol, p.StrategyName, orderQuantity, optimalQuantity)
		orderQuantity = optimalQuantity * 0.95 // Use 95% of available liquidity for safety
	}

	// Validate minimum order size (if exchange has requirements)
	if orderQuantity <= 0 {
		return 0, 0, fmt.Errorf("calculated order quantity is zero or negative: %.6f", orderQuantity)
	}

	// Validate against market cap data if available
	if p.lastMarketCap != nil && p.lastMarketCap.LastPrice != "" {
		if marketCapPrice, err := strconv.ParseFloat(p.lastMarketCap.LastPrice, 64); err == nil {
			priceDiff := math.Abs(optimalPrice-marketCapPrice) / marketCapPrice
			if priceDiff > 0.01 { // 1% threshold
				utils.GetLogger().Printf("Position | [%s %s] Warning: Calculated price (%.2f) differs significantly from market cap price (%.2f), diff=%.2f%%",
					p.Symbol, p.StrategyName, optimalPrice, marketCapPrice, priceDiff*100)
			}
		}
	}

	// Log market data age for debugging
	if !p.lastMarketDataTime.IsZero() {
		age := p.getMarketDataAge()
		utils.GetLogger().Printf("Position | [%s %s] Using market data from %v ago", p.Symbol, p.StrategyName, age)
	}

	utils.GetLogger().Printf("Position | [%s %s] Final order params: price=%.2f, quantity=%.6f, risk_amount=%.2f",
		p.Symbol, p.StrategyName, optimalPrice, orderQuantity, riskAmount)

	return optimalPrice, orderQuantity, nil
}

// createExitOrder creates an exit order based on position side
func (p *position) createExitOrder(price float64) exchange.OrderRequest {
	// TODO: Better price management for ensuring full execution (using a function like calculateOptimalOrderParams)
	// TODO: Handle other order types
	// TODO: Mitigate risk of partial fill

	side := p.getExitSide()

	orderReq := exchange.OrderRequest{
		Symbol:   p.Symbol,
		Side:     side,
		Type:     "market",
		Price:    0,
		Quantity: p.Size,
	}

	// if p.OrderSpec.Type == "limit" {
	// 	orderReq.Price = price
	// 	// TODO: Apply spread for better execution outside of this function
	// 	if p.RiskParams.LimitSpread > 0 {
	// 		if side == "sell" {
	// 			orderReq.Price = price * (1 - p.RiskParams.LimitSpread/100)
	// 		} else {
	// 			orderReq.Price = price * (1 + p.RiskParams.LimitSpread/100)
	// 		}
	// 	}
	// }

	return orderReq
}

// createEntryOrder creates an entry order (buy or sell for futures)
func (p *position) createEntryOrder(price, orderSize float64, side string) exchange.OrderRequest {
	// TODO: Check caller passed optimal price and optimal quantity
	var orderReq exchange.OrderRequest

	switch p.OrderSpec.Type {
	case "limit":
		orderReq = exchange.OrderRequest{
			Symbol:   p.Symbol,
			Side:     side,
			Type:     "limit",
			Price:    price,
			Quantity: orderSize,
		}
	case "stop-limit":
		orderReq = exchange.OrderRequest{
			Symbol:    p.Symbol,
			Side:      side,
			Type:      "stop-limit",
			Price:     price,
			StopPrice: price, // TODO: Set proper value
			Quantity:  orderSize,
		}
		// TODO: Set proper value
		if side == "buy" {
			orderReq.StopPrice = price * (1 + p.RiskParams.LimitSpread/100)
		} else {
			orderReq.StopPrice = price * (1 - p.RiskParams.LimitSpread/100)
		}
	case "oco":
		// TODO: Set proper value
		limitOrder := exchange.OrderRequest{
			Symbol:   p.Symbol,
			Side:     side,
			Type:     "limit",
			Price:    price,
			Quantity: orderSize,
		}
		// TODO: Set proper value
		stopOrder := exchange.OrderRequest{
			Symbol:    p.Symbol,
			Side:      side,
			Type:      "stop-limit",
			Price:     price,
			StopPrice: price, // TODO: Set proper value
			Quantity:  orderSize,
		}
		// TODO: Set proper value
		if side == "buy" {
			stopOrder.StopPrice = price * (1 - p.RiskParams.LimitSpread/100)
		} else {
			stopOrder.StopPrice = price * (1 + p.RiskParams.LimitSpread/100)
		}
		// TODO: Set proper value
		orderReq = exchange.OrderRequest{
			Symbol:      p.Symbol,
			Side:        side,
			Type:        "limit",
			Price:       price,
			Quantity:    orderSize,
			AlgoType:    "OCO",
			ChildOrders: []exchange.OrderRequest{limitOrder, stopOrder},
		}
	default:
		orderReq = exchange.OrderRequest{
			Symbol:   p.Symbol,
			Side:     side,
			Type:     "market",
			Price:    0,
			Quantity: orderSize,
		}
	}

	return orderReq
}

// handleEntrySuccess handles successful entry order execution (buy or sell)
func (p *position) handleEntrySuccess(ctx context.Context, order exchange.Order, side string) {
	utils.GetLogger().Printf("Position | [%s %s] Entry order submitted: %+v", p.Symbol, p.StrategyName, order)

	if p.Storage != nil {
		err := p.Storage.SaveOrder(ctx, exchange.OrderToDBOrder(order))
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error saving order: %v", p.Symbol, p.StrategyName, err)
		}

		err = p.Storage.LogEvent(ctx, db.Event{
			Time:        time.Now(),
			Type:        "order",
			Description: "entry_order_submitted",
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "order": order},
		})
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error logging event: %v", p.Symbol, p.StrategyName, err)
		}
	}

	// Update position
	p.Side = side
	p.Entry = order.AvgPrice
	p.Size = order.Quantity
	p.OrderID = order.OrderID
	p.Active = true
	p.Time = time.Now()

	// NOTE: Don't update balance here. It's updated in onPositionClose.

	// Automatically update the position in database when it becomes active
	if p.Storage != nil {
		if err := p.save(ctx); err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Warning: Failed to update active position in database: %v", p.StrategyName, p.Symbol, err)
		}
	}

	if p.Notifier != nil {
		msg := fmt.Sprintf("[ORDER FILLED]\nSide: %s\nSymbol: %s\nQty: %.4f\nAvgPrice: %.2f\nType: %s\nOrderID: %s\nEvent: Manual\nTime: %s",
			side, p.Symbol, order.Quantity, order.AvgPrice, p.OrderSpec.Type, order.OrderID, time.Now().Format(time.RFC3339))
		err := p.Notifier.SendWithRetry(msg)
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
		}
	}
}

// handleOrderError handles order execution errors
func (p *position) handleOrderError(ctx context.Context, err error, desc string) {
	utils.GetLogger().Printf("Position | [%s %s] %s failed: %v", p.Symbol, p.StrategyName, desc, err)

	if p.Storage != nil {
		err := p.Storage.LogEvent(ctx, db.Event{
			Time:        time.Now(),
			Type:        "error",
			Description: desc,
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "error": err.Error()},
		})
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error logging event: %v", p.Symbol, p.StrategyName, err)
		}
	}

	if p.Notifier != nil {
		msg := fmt.Sprintf("ERROR: %s: %v", desc, err)
		err := p.Notifier.SendWithRetry(msg)
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
		}
	}
}

// handleExitSuccess handles successful exit order execution
func (p *position) handleExitSuccess(ctx context.Context, order exchange.Order, event string) {
	utils.GetLogger().Printf("Position | [%s %s] %s triggered, order submitted: %+v", p.Symbol, p.StrategyName, event, order)

	if p.Storage != nil {
		err := p.Storage.SaveOrder(ctx, exchange.OrderToDBOrder(order))
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error saving order: %v", p.Symbol, p.StrategyName, err)
		}
		err = p.Storage.LogEvent(ctx, db.Event{
			Time:        time.Now(),
			Type:        "order",
			Description: fmt.Sprintf("%s_triggered", event),
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "order": order},
		})
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error saving order: %v", p.Symbol, p.StrategyName, err)
		}
	}

	if p.Notifier != nil {
		msg := fmt.Sprintf("[ORDER FILLED]\nSide: %s\nSymbol: %s\nQty: %.4f\nAvgPrice: %.2f\nType: %s\nOrderID: %s\nEvent: %s\nPnL: %.2f\nTime: %s",
			p.getExitSide(), p.Symbol, p.Size, order.AvgPrice, p.OrderSpec.Type, order.OrderID, event, p.LastPNL, time.Now().Format(time.RFC3339))
		err := p.Notifier.SendWithRetry(msg)
		if err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
		}
	}
}

// calculatePNL calculates the current PNL
func (p *position) calculatePNL(exitPrice float64) float64 {
	if p.Side == "buy" {
		return (exitPrice - p.Entry) * p.Size
	}
	// For short positions
	return (p.Entry - exitPrice) * p.Size
}

// getExitSide returns the opposite side for exit orders
func (p *position) getExitSide() string {
	if p.Side == "buy" {
		return "sell"
	}
	return "buy"
}

// onPositionClose handles position closing logic and updates database
func (p *position) onPositionClose(ctx context.Context, exitPrice float64) {
	p.LastPNL = p.calculatePNL(exitPrice)

	p.Active = false
	// Update balance with PNL
	p.Balance += p.LastPNL

	// Add to trade log
	p.LiveTradeLog = append(p.LiveTradeLog, Trade{
		Entry:     p.Entry,
		Exit:      exitPrice,
		PnL:       p.LastPNL,
		EntryTime: p.Time,
		ExitTime:  time.Now(),
	})

	// Automatically update the position in database
	if p.Storage != nil {
		if err := p.update(ctx); err != nil {
			utils.GetLogger().Printf("Position | [%s %s] Warning: Failed to update closed position in database: %v", p.StrategyName, p.Symbol, err)
		}
	}

	// Reset ID for next position
	p.ID = 0
}

// updateLiveStats updates live trading statistics
func (p *position) updateLiveStats() {
	p.LiveEquity += p.LastPNL
	p.LiveEquityCurve = append(p.LiveEquityCurve, p.LiveEquity)

	if p.LastPNL > 0 {
		p.LiveWins++
		p.LiveWinPnls = append(p.LiveWinPnls, p.LastPNL)
	} else {
		p.LiveLosses++
		p.LiveLossPnls = append(p.LiveLossPnls, p.LastPNL)
	}

	p.LiveTrades++

	// Update max equity and drawdown
	if p.LiveEquity > p.LiveMaxEquity {
		p.LiveMaxEquity = p.LiveEquity
	}
	diff := p.LiveMaxEquity - p.LiveEquity
	if diff > p.LiveMaxDrawdown {
		p.LiveMaxDrawdown = diff
	}

	// Win rate
	if p.LiveTrades > 0 {
		p.LiveWinRate = float64(p.LiveWins) / float64(p.LiveTrades)
	}

	// Average win/loss
	avgWin, avgLoss := 0.0, 0.0
	if len(p.LiveWinPnls) > 0 {
		for _, w := range p.LiveWinPnls {
			avgWin += w
		}
		avgWin /= float64(len(p.LiveWinPnls))
	}
	if len(p.LiveLossPnls) > 0 {
		for _, l := range p.LiveLossPnls {
			avgLoss += l
		}
		avgLoss /= float64(len(p.LiveLossPnls))
	}

	// Profit factor
	p.ProfitFactor = 0.0
	if avgLoss != 0 {
		p.ProfitFactor = -avgWin / avgLoss
	}

	// Mean and standard deviation
	allPnls := append(p.LiveWinPnls, p.LiveLossPnls...)
	if len(allPnls) > 0 {
		p.MeanPNL = 0.0
		for _, v := range allPnls {
			p.MeanPNL += v
		}
		p.MeanPNL /= float64(len(allPnls))

		p.StdPNL = 0.0
		for _, v := range allPnls {
			p.StdPNL += (v - p.MeanPNL) * (v - p.MeanPNL)
		}
		p.StdPNL = math.Sqrt(p.StdPNL / float64(len(allPnls)))
	}

	// Sharpe ratio
	if p.StdPNL > 0 {
		p.Sharpe = p.MeanPNL / p.StdPNL
	}

	// Expectancy
	if p.LiveTrades > 0 {
		p.Expectancy = (p.LiveWinRate*avgWin + (1-p.LiveWinRate)*avgLoss)
	}

	// Check daily loss limit
	if p.LiveEquity < -p.RiskParams.MaxDailyLoss {
		p.TradingDisabled = true
		if p.Notifier != nil {
			msg := fmt.Sprintf("⚠️ DAILY LOSS LIMIT REACHED\nSymbol: %s\nStrategyName: %s\nDaily PnL: %.2f\nLimit: %.2f\nTrading disabled until next day",
				p.Symbol, p.StrategyName, p.LiveEquity, p.RiskParams.MaxDailyLoss)
			err := p.Notifier.SendWithRetry(msg)
			if err != nil {
				utils.GetLogger().Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
			}
		}
	}

	p.logStats()
}

// logStats logs trading statistics
func (p *position) logStats() {
	utils.GetLogger().Printf("Position | [%s %s] Live Stats: Trades=%d, WinRate=%.2f%%, PnL=%.2f, MaxDD=%.2f, ProfitFactor=%.2f",
		p.Symbol, p.StrategyName, p.LiveTrades, p.LiveWinRate*100, p.LiveEquity, p.LiveMaxDrawdown, p.ProfitFactor)
	utils.GetLogger().Printf("Position | [%s] REPORT:", p.StrategyName)
	utils.GetLogger().Printf("  Trades=%d, Wins=%d, Losses=%d, WinRate=%.2f%%", p.LiveTrades, p.LiveWins, p.LiveLosses, p.LiveWinRate*100)
	utils.GetLogger().Printf("  PnL=%.2f, MaxDrawdown=%.2f", p.LiveEquity, p.LiveMaxDrawdown)
	utils.GetLogger().Printf("  Sharpe=%.2f, Expectancy=%.2f", p.Sharpe, p.Expectancy)
	utils.GetLogger().Printf("  Risk: RiskPercent=%.2f, StopLossPercent=%.2f, TrailingStopPercent=%.2f",
		p.RiskParams.RiskPercent, p.RiskParams.StopLossPercent, p.RiskParams.TrailingStopPercent)

	// Log trade history
	utils.GetLogger().Printf("  Trade Log:")
	for i, t := range p.LiveTradeLog {
		utils.GetLogger().Printf("    Trade %d: Entry=%.2f at %s, Exit=%.2f at %s, PnL=%.2f",
			i+1, t.Entry, t.EntryTime.Format(time.RFC3339), t.Exit, t.ExitTime.Format(time.RFC3339), t.PnL)
	}
}

// sendSignalNotification sends a notification about the received signal
func (p *position) sendSignalNotification(ctx context.Context, signal strategy.Signal) {
	if p.Notifier == nil {
		return
	}

	// Check if signal has valid data
	if signal.Position == strategy.Hold {
		return
	}

	// Format timestamp correctly based on signal data
	timestamp := "unknown time"
	if !signal.Time.IsZero() {
		timestamp = signal.Time.Format(time.RFC3339)
	} else if signal.Candle.Timestamp.IsZero() {
		timestamp = signal.Candle.Timestamp.Format(time.RFC3339)
	}

	posStr := "unknown"
	switch signal.Position {
	case strategy.Hold:
		posStr = "hold"
	case strategy.LongBullish:
		posStr = "long bullish"
	case strategy.LongBearish:
		posStr = "long bearish"
	case strategy.ShortBullish:
		posStr = "short bullish"
	case strategy.ShortBearish:
		posStr = "short bearish"
	}

	msg := fmt.Sprintf("[%s %s] signal for %s at %s: %s (%s)",
		p.Symbol, signal.StrategyName, p.Symbol, timestamp, posStr, signal.Reason)

	if err := p.Notifier.SendWithRetry(msg); err != nil {
		utils.GetLogger().Printf("Position | [%s %s] Notification failed: %v", p.Symbol, signal.StrategyName, err)

		if p.Storage != nil {
			err = p.Storage.LogEvent(ctx, db.Event{
				Time:        time.Now(),
				Type:        "error",
				Description: "notification",
				Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "error": err.Error(), "msg": msg},
			})
			if err != nil {
				utils.GetLogger().Printf("Position | [%s %s] Error logging event: %v", p.Symbol, signal.StrategyName, err)
			}
		}
	}
}

// ResetDailyStats resets daily trading statistics
func (p *position) ResetDailyStats() {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Note: Keep cumulative stats like LiveWins, LiveLosses, etc.
	// Reset only daily-specific metrics

	p.TradingDisabled = false
	// TODO: Reset other daily stats?
	// p.LiveEquity = 0
	// p.LiveMaxEquity = 0
	// p.LiveMaxDrawdown = 0
}

// GetStats returns position statistics
func (p *position) Stats() (map[string]interface{}, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats := map[string]interface{}{
		"symbol":        p.Symbol,
		"strategy_name": p.StrategyName,
		"active":        p.Active,
		"equity":        p.LiveEquity,
		"max_equity":    p.LiveMaxEquity,
		"max_drawdown":  p.LiveMaxDrawdown,
		"wins":          p.LiveWins,
		"losses":        p.LiveLosses,
		"trades":        p.LiveTrades,
		"win_rate":      p.LiveWinRate,
		"profit_factor": p.ProfitFactor,
		"sharpe":        p.Sharpe,
		"expectancy":    p.Expectancy,
		"mean_pnl":      p.MeanPNL,
		"std_pnl":       p.StdPNL,
		"last_pnl":      p.LastPNL,
		"trailing_stop": p.TrailingStop,
		"win_pnls":      p.LiveWinPnls,
		"loss_pnls":     p.LiveLossPnls,
		"equity_curve":  p.LiveEquityCurve,
		"trade_log":     p.LiveTradeLog,
	}

	if p.Active {
		stats["side"] = p.Side
		stats["entry"] = p.Entry
		stats["size"] = p.Size
		stats["time"] = p.Time
	}

	return stats, nil
}

// OnTick processes a new tick with optional market depth and market cap information
func (p *position) OnTick(ctx context.Context, tick exchange.Tick, depthState map[string]exchange.OrderBook, marketCapState *exchange.MarketCap) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Update stored market data
	p.updateMarketData(depthState, marketCapState)

	if !p.Active || p.TradingDisabled {
		return
	}

	// Process market cap data if provided
	var marketCapPrice float64
	if marketCapState != nil {
		// Try to use market cap data for price validation
		if marketCapState.LastPrice != "" {
			if price, err := strconv.ParseFloat(marketCapState.LastPrice, 64); err == nil {
				marketCapPrice = price
			}
		}
	}

	// Use tick price or market cap price (if available and within reasonable bounds)
	if marketCapPrice > 0 {
		// If market cap price is within 1% of tick price, we can use it for validation
		priceDiff := math.Abs(tick.Price-marketCapPrice) / tick.Price
		if priceDiff > 0.01 {
			utils.GetLogger().Printf("Position | [%s %s] Warning: Tick price (%.2f) differs significantly from market cap price (%.2f)",
				p.Symbol, p.StrategyName, tick.Price, marketCapPrice)
		}
	}

	// Create backup of current state for rollback
	backup := p.backupState()

	// Execute with transaction management
	err := executeWithTransaction(ctx, p.Storage, func(txCtx context.Context) error {
		// Use tick.Price for all stop/take/trailing logic
		if p.shouldTriggerStopLoss(tick.Price) {
			return p.exit(txCtx, tick.Price, "stop_loss")
		}
		if p.shouldTriggerTakeProfit(tick.Price) {
			return p.exit(txCtx, tick.Price, "take_profit")
		}
		if p.RiskParams.TrailingStopPercent > 0 {
			p.updateTrailingStop(tick.Price)
			if p.shouldTriggerTrailingStop(tick.Price) {
				return p.exit(txCtx, tick.Price, "trailing_stop")
			}
		}
		return nil
	})

	// Handle errors with rollback
	if err != nil {
		utils.GetLogger().Printf("Position | [%s %s] Error processing tick, rolling back state: %v", p.Symbol, p.StrategyName, err)
		p.rollbackState(backup)

		// Log the error
		if p.Storage != nil {
			p.Storage.LogEvent(ctx, db.Event{
				Time:        time.Now(),
				Type:        "error",
				Description: "tick_processing_failed",
				Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "error": err.Error(), "price": tick.Price},
			})
		}
		return
	}
}

// OnSignal processes a new signal for open/close logic (lower frequency)
func (p *position) OnSignal(ctx context.Context, signal strategy.Signal, depthState map[string]exchange.OrderBook, marketCapState *exchange.MarketCap) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Update stored market data
	p.updateMarketData(depthState, marketCapState)

	// If its new trading day, reset daily stats
	if time.Now().Day() != p.Time.Day() {
		p.ResetDailyStats()
	}

	if p.TradingDisabled {
		utils.GetLogger().Printf("Position | [%s %s] Trading disabled, ignoring signal", p.Symbol, signal.StrategyName)
		return
	}

	// Validate signal
	if signal.Position == strategy.Hold {
		return
	}

	// Create backup of current state for rollback
	backup := p.backupState()

	// Execute with transaction management
	err := executeWithTransaction(ctx, p.Storage, func(txCtx context.Context) error {
		// Execute the signal logic
		if p.Active {
			// Manual close (sell signal)
			if (signal.Position == strategy.LongBearish && p.Side == "buy") || (signal.Position == strategy.ShortBullish && p.Side == "sell") {
				return p.exit(txCtx, signal.TriggerPrice, "manual_exit")
			}
		} else {
			// Open position (buy signal)
			if signal.Position == strategy.LongBullish || signal.Position == strategy.ShortBearish {
				return p.enter(txCtx, signal)
			}
		}
		return nil
	})

	// Handle errors with rollback
	if err != nil {
		utils.GetLogger().Printf("Position | [%s %s] Error processing signal, rolling back state: %v", p.Symbol, signal.StrategyName, err)
		p.rollbackState(backup)

		// Log the error
		if p.Storage != nil {
			p.Storage.LogEvent(ctx, db.Event{
				Time:        time.Now(),
				Type:        "error",
				Description: "signal_processing_failed",
				Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "error": err.Error(), "signal": signal},
			})
		}
		return
	}

	// Send signal notification
	// p.sendSignalNotification(ctx, signal)
}

// --- Tick-based stop/take/trailing logic ---
func (p *position) shouldTriggerStopLoss(price float64) bool {
	if p.RiskParams.StopLossPercent <= 0 {
		return false
	}
	if p.Side == "buy" {
		return price <= p.Entry*(1-p.RiskParams.StopLossPercent/100)
	}
	return price >= p.Entry*(1+p.RiskParams.StopLossPercent/100)
}

func (p *position) shouldTriggerTakeProfit(price float64) bool {
	if p.RiskParams.TakeProfitPercent <= 0 {
		return false
	}
	if p.Side == "buy" {
		return price >= p.Entry*(1+p.RiskParams.TakeProfitPercent/100)
	}
	return price <= p.Entry*(1-p.RiskParams.TakeProfitPercent/100)
}

func (p *position) updateTrailingStop(price float64) {
	if p.Side == "buy" {
		profit := price - p.Entry
		if profit > p.TrailingStop {
			p.TrailingStop = profit
		}
	} else {
		profit := p.Entry - price
		if profit > p.TrailingStop {
			p.TrailingStop = profit
		}
	}
}

func (p *position) shouldTriggerTrailingStop(price float64) bool {
	if p.TrailingStop <= 0 {
		return false
	}

	if p.Side == "buy" {
		// For long positions, trigger when price falls below entry + trailing stop - trailing stop percentage
		trailingStopPrice := p.Entry + p.TrailingStop - (p.Entry * p.RiskParams.TrailingStopPercent / 100)
		return price <= trailingStopPrice
	} else {
		// For short positions, trigger when price rises above entry - trailing stop + trailing stop percentage
		trailingStopPrice := p.Entry - p.TrailingStop + (p.Entry * p.RiskParams.TrailingStopPercent / 100)
		return price >= trailingStopPrice
	}
}

// min returns the minimum of three float64 values
func min(a, b, c float64) float64 {
	min := a
	if b < min {
		min = b
	}
	if c < min {
		min = c
	}
	return min
}
