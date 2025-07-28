// Package position
package position

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/exchange"
	"github.com/amirphl/simple-trader/internal/journal"
	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/order"
	"github.com/amirphl/simple-trader/internal/strategy"
)

type Storage interface {
	GetDB() *sql.DB
	SaveOrder(ctx context.Context, order order.OrderResponse) error
	LogEvent(ctx context.Context, event journal.Event) error
}

// Tick represents a market tick (price update)
type Tick interface {
	Price() float64
	Timestamp() time.Time
}

// executeWithTransaction executes a function with proper transaction management
// If a transaction exists in context, it uses that. Otherwise, it creates a new one.
func executeWithTransaction(ctx context.Context, storage Storage, fn func(context.Context) error) error {
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
	OnSignal(ctx context.Context, signal strategy.Signal)
	OnTick(ctx context.Context, tick Tick, depthState map[string]exchange.OrderBook, marketCapState *exchange.MarketCapData)
	Stats() (map[string]interface{}, error)
	DailyPnL() (float64, error)
	IsActive() bool
}

// TODO: Transactional updates, atomicity, order status check, commission, slippage, exchange ctx, etc.

// New creates a new position manager
func New(cfg config.Config, strategyName, symbol string, storage Storage, exchange exchange.Exchange, notifier notifier.Notifier) Position {
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

		LiveWinPnls:     make([]float64, 0),
		LiveLossPnls:    make([]float64, 0),
		LiveEquityCurve: make([]float64, 0),
		LiveTradeLog:    make([]Trade, 0),
	}

	// Set order configuration
	pos.OrderSpec.Type = cfg.OrderType
	pos.OrderSpec.MaxAttempts = 2
	pos.OrderSpec.Delay = time.Second * 2

	return pos
}

// positionState represents a backup of position state for rollback
type positionState struct {
	Side            string
	Entry           float64
	Size            float64
	OrderID         string
	Time            time.Time
	Active          bool
	LastPNL         float64
	MeanPNL         float64
	StdPNL          float64
	Sharpe          float64
	Expectancy      float64
	LiveEquity      float64
	LiveMaxEquity   float64
	LiveMaxDrawdown float64
	LiveWins        int64
	LiveLosses      int64
	LiveTrades      int64
	LiveWinRate     float64
	LiveTradeLog    []Trade
	LiveEquityCurve []float64
	LiveWinPnls     []float64
	LiveLossPnls    []float64
	ProfitFactor    float64
	TrailingStop    float64
	RiskParams      config.RiskParams
	TradingDisabled bool
}

// position represents a trading position with risk management and statistics
type position struct {
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
	Storage  Storage           `json:"-"`
	Notifier notifier.Notifier `json:"-"`

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

	mu sync.RWMutex // Added mutex for thread safety
}

// backupState creates a backup of the current position state
func (p *position) backupState() positionState {
	return positionState{
		Side:            p.Side,
		Entry:           p.Entry,
		Size:            p.Size,
		OrderID:         p.OrderID,
		Time:            p.Time,
		Active:          p.Active,
		LastPNL:         p.LastPNL,
		MeanPNL:         p.MeanPNL,
		StdPNL:          p.StdPNL,
		Sharpe:          p.Sharpe,
		Expectancy:      p.Expectancy,
		LiveEquity:      p.LiveEquity,
		LiveMaxEquity:   p.LiveMaxEquity,
		LiveMaxDrawdown: p.LiveMaxDrawdown,
		LiveWins:        p.LiveWins,
		LiveLosses:      p.LiveLosses,
		LiveTrades:      p.LiveTrades,
		LiveWinRate:     p.LiveWinRate,
		LiveTradeLog:    p.LiveTradeLog,
		LiveEquityCurve: p.LiveEquityCurve,
		LiveWinPnls:     p.LiveWinPnls,
		LiveLossPnls:    p.LiveLossPnls,
		ProfitFactor:    p.ProfitFactor,
		TrailingStop:    p.TrailingStop,
		RiskParams:      p.RiskParams,
		TradingDisabled: p.TradingDisabled,
	}
}

// rollbackState restores the position state from backup
func (p *position) rollbackState(backup positionState) {
	p.Side = backup.Side
	p.Entry = backup.Entry
	p.Size = backup.Size
	p.OrderID = backup.OrderID
	p.Time = backup.Time
	p.Active = backup.Active
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

// Save persists the position to file with error handling
func (p *position) Save() error {
	// TODO: Save in database
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal position [%s %s]: %w", p.StrategyName, p.Symbol, err)
	}

	if err := os.WriteFile(fmt.Sprintf("%s-%s.json", p.StrategyName, p.Symbol), data, 0o644); err != nil {
		return fmt.Errorf("failed to write position file [%s %s]: %w", p.StrategyName, p.Symbol, err)
	}

	return nil
}

// Load loads a position from file with proper error handling
func Load(stratName, symbol string, storage Storage, exchange exchange.Exchange, notifier notifier.Notifier) (Position, error) {
	// TODO: Load from database
	var pos position

	data, err := os.ReadFile(fmt.Sprintf("%s-%s.json", stratName, symbol))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("position file not found [%s %s]", stratName, symbol)
		}
		return nil, fmt.Errorf("failed to read position file [%s %s]: %w", stratName, symbol, err)
	}

	if err := json.Unmarshal(data, &pos); err != nil {
		return nil, fmt.Errorf("failed to unmarshal position [%s %s]: %w", stratName, symbol, err)
	}

	pos.Exchange = exchange
	pos.Storage = storage
	pos.Notifier = notifier

	return &pos, nil
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
	today := time.Now().Truncate(24 * time.Hour)

	for _, trade := range p.LiveTradeLog {
		if trade.ExitTime.After(today) || trade.ExitTime.Equal(today) {
			dailyPnL += trade.PnL
		}
	}

	return dailyPnL, nil
}

// executeStopLoss executes a stop loss order atomically with error handling
func (p *position) executeStopLoss(ctx context.Context, price float64) error {
	orderReq := p.createExitOrder(price, "stop_loss")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(ctx, orderReq, p.OrderSpec.MaxAttempts, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "stop_loss_order")
		return fmt.Errorf("failed to submit stop loss order: %w", err)
	}

	// Update position state
	p.handleExitSuccess(ctx, orderResp, "Stop Loss")
	p.onPositionClose(orderResp.AvgPrice)
	p.updateLiveStats()

	return nil
}

// executeTakeProfit executes a take profit order atomically with error handling
func (p *position) executeTakeProfit(ctx context.Context, price float64) error {
	orderReq := p.createExitOrder(price, "take_profit")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(ctx, orderReq, p.OrderSpec.MaxAttempts, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "take_profit_order")
		return fmt.Errorf("failed to submit take profit order: %w", err)
	}
	// Update position state
	p.handleExitSuccess(ctx, orderResp, "Take Profit")
	p.onPositionClose(orderResp.AvgPrice)
	p.updateLiveStats()

	return nil
}

// executeTrailingStop executes a trailing stop order atomically with error handling
func (p *position) executeTrailingStop(ctx context.Context, price float64) error {
	orderReq := p.createExitOrder(price, "trailing_stop")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(ctx, orderReq, p.OrderSpec.MaxAttempts, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "trailing_stop_order")
		return fmt.Errorf("failed to submit trailing stop order: %w", err)
	}

	// Update position state
	p.handleExitSuccess(ctx, orderResp, "Trailing Stop")
	p.onPositionClose(orderResp.AvgPrice)
	p.updateLiveStats()

	return nil
}

// executeManualExit executes a manual exit order atomically with error handling
func (p *position) executeManualExit(ctx context.Context, price float64) error {
	orderReq := p.createExitOrder(price, "manual_exit")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(ctx, orderReq, p.OrderSpec.MaxAttempts, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "manual_exit_order")
		return fmt.Errorf("failed to submit manual exit order: %w", err)
	}

	// Update position state
	p.handleExitSuccess(ctx, orderResp, "Manual Exit")
	p.onPositionClose(orderResp.AvgPrice)
	p.updateLiveStats()

	return nil
}

// executeEntry executes an entry order atomically with error handling
func (p *position) executeEntry(ctx context.Context, signal strategy.Signal) error {
	// Check if we have enough balance
	if p.RiskParams.Balance <= 0 {
		return fmt.Errorf("insufficient balance: %.2f", p.RiskParams.Balance)
	}

	orderSize := p.RiskParams.Balance * p.RiskParams.RiskPercent / 100
	// TODO: check with max loss

	// Support both long and short positions based on signal
	side := "buy"
	if signal.Position == strategy.ShortBearish {
		side = "sell"
	}

	// TODO: Better price management for guaranteed full execution
	// TODO : Decide using market cap and orderbook
	orderReq := p.createEntryOrder(signal.TriggerPrice, orderSize, side)
	orderResp, err := p.Exchange.SubmitOrderWithRetry(ctx, orderReq, p.OrderSpec.MaxAttempts, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "entry_order_submission")
		return fmt.Errorf("failed to submit entry order: %w", err)
	}

	p.handleEntrySuccess(ctx, orderResp, orderReq.Quantity, side)

	return nil
}

// createExitOrder creates an exit order based on position side
func (p *position) createExitOrder(price float64, orderType string) order.OrderRequest {
	// TODO: Better price management for ensuring full execution
	// TODO: Handle other order types

	side := p.getExitSide()

	orderReq := order.OrderRequest{
		Symbol:   p.Symbol,
		Side:     side,
		Type:     p.OrderSpec.Type,
		Price:    0,
		Quantity: p.Size,
	}

	if p.OrderSpec.Type == "limit" {
		orderReq.Price = price
		// Apply spread for better execution
		if p.RiskParams.LimitSpread > 0 {
			if side == "sell" {
				orderReq.Price = price * (1 - p.RiskParams.LimitSpread/100)
			} else {
				orderReq.Price = price * (1 + p.RiskParams.LimitSpread/100)
			}
		}
	}

	return orderReq
}

// createEntryOrder creates an entry order (buy or sell for futures)
func (p *position) createEntryOrder(price, orderSize float64, side string) order.OrderRequest {
	var orderReq order.OrderRequest

	switch p.OrderSpec.Type {
	case "limit":
		orderReq = order.OrderRequest{
			Symbol:   p.Symbol,
			Side:     side,
			Type:     "limit",
			Price:    price,
			Quantity: orderSize,
		}
		if p.RiskParams.LimitSpread > 0 {
			if side == "buy" {
				orderReq.Price = price * (1 - p.RiskParams.LimitSpread/100)
			} else {
				orderReq.Price = price * (1 + p.RiskParams.LimitSpread/100)
			}
		}
	case "stop-limit":
		orderReq = order.OrderRequest{
			Symbol:    p.Symbol,
			Side:      side,
			Type:      "stop-limit",
			Price:     price,
			StopPrice: price * 1.001, // TODO: Better stop price management
			Quantity:  orderSize,
		}
	case "oco":
		limitOrder := order.OrderRequest{
			Symbol:   p.Symbol,
			Side:     side,
			Type:     "limit",
			Price:    price,
			Quantity: orderSize,
		}
		stopOrder := order.OrderRequest{
			Symbol:    p.Symbol,
			Side:      side,
			Type:      "stop-limit",
			Price:     price,
			StopPrice: price * 1.001,
			Quantity:  orderSize,
		}
		orderReq = order.OrderRequest{
			Symbol:      p.Symbol,
			Side:        side,
			Type:        "limit",
			Price:       price,
			Quantity:    orderSize,
			AlgoType:    "OCO",
			ChildOrders: []order.OrderRequest{limitOrder, stopOrder},
		}
	default:
		orderReq = order.OrderRequest{
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
func (p *position) handleEntrySuccess(ctx context.Context, orderResp order.OrderResponse, orderSize float64, side string) {
	log.Printf("Position | [%s %s] Entry order submitted: %+v", p.Symbol, p.StrategyName, orderResp)

	if p.Storage != nil {
		err := p.Storage.SaveOrder(ctx, orderResp)
		if err != nil {
			log.Printf("Position | [%s %s] Error saving order: %v", p.Symbol, p.StrategyName, err)
		}

		err = p.Storage.LogEvent(ctx, journal.Event{
			Time:        time.Now(),
			Type:        "order",
			Description: "entry_order_submitted",
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "order": orderResp},
		})
		if err != nil {
			log.Printf("Position | [%s %s] Error logging event: %v", p.Symbol, p.StrategyName, err)
		}
	}

	// Update position
	p.Side = side
	p.Entry = orderResp.AvgPrice
	p.Size = orderSize
	p.OrderID = orderResp.OrderID
	p.Active = true
	p.Time = time.Now()

	// NOTE: Don't update balance here. It's updated in onPositionClose.

	if p.Notifier != nil {
		msg := fmt.Sprintf("[ORDER FILLED]\nSide: %s\nSymbol: %s\nQty: %.4f\nAvgPrice: %.2f\nType: %s\nOrderID: %s\nEvent: Manual\nTime: %s",
			side, p.Symbol, orderSize, orderResp.AvgPrice, p.OrderSpec.Type, orderResp.OrderID, time.Now().Format(time.RFC3339))
		err := p.Notifier.SendWithRetry(msg)
		if err != nil {
			log.Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
		}
	}
}

// handleOrderError handles order execution errors
func (p *position) handleOrderError(ctx context.Context, err error, description string) {
	log.Printf("Position | [%s %s] %s failed: %v", p.Symbol, p.StrategyName, description, err)

	if p.Storage != nil {
		err := p.Storage.LogEvent(ctx, journal.Event{
			Time:        time.Now(),
			Type:        "error",
			Description: description,
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "error": err.Error()},
		})
		if err != nil {
			log.Printf("Position | [%s %s] Error logging event: %v", p.Symbol, p.StrategyName, err)
		}
	}

	if p.Notifier != nil {
		msg := fmt.Sprintf("ERROR: %s: %v", description, err)
		err := p.Notifier.SendWithRetry(msg)
		if err != nil {
			log.Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
		}
	}
}

// handleExitSuccess handles successful exit order execution
func (p *position) handleExitSuccess(ctx context.Context, orderResp order.OrderResponse, event string) {
	log.Printf("Position | [%s %s] %s triggered, order submitted: %+v", p.Symbol, p.StrategyName, event, orderResp)

	if p.Storage != nil {
		err := p.Storage.SaveOrder(ctx, orderResp)
		if err != nil {
			log.Printf("Position | [%s %s] Error saving order: %v", p.Symbol, p.StrategyName, err)
		}
		err = p.Storage.LogEvent(ctx, journal.Event{
			Time:        time.Now(),
			Type:        "order",
			Description: fmt.Sprintf("%s_triggered", event),
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "order": orderResp},
		})
		if err != nil {
			log.Printf("Position | [%s %s] Error saving order: %v", p.Symbol, p.StrategyName, err)
		}
	}

	if p.Notifier != nil {
		msg := fmt.Sprintf("[ORDER FILLED]\nSide: %s\nSymbol: %s\nQty: %.4f\nAvgPrice: %.2f\nType: %s\nOrderID: %s\nEvent: %s\nPnL: %.2f\nTime: %s",
			p.getExitSide(), p.Symbol, p.Size, orderResp.AvgPrice, p.OrderSpec.Type, orderResp.OrderID, event, p.LastPNL, time.Now().Format(time.RFC3339))
		err := p.Notifier.SendWithRetry(msg)
		if err != nil {
			log.Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
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

// onPositionClose handles position closing logic
func (p *position) onPositionClose(exitPrice float64) {
	p.LastPNL = p.calculatePNL(exitPrice)

	p.Active = false
	// Update balance with PNL
	p.RiskParams.Balance += p.LastPNL

	// Add to trade log
	p.LiveTradeLog = append(p.LiveTradeLog, Trade{
		Entry:     p.Entry,
		Exit:      exitPrice,
		PnL:       p.LastPNL,
		EntryTime: p.Time,
		ExitTime:  time.Now(),
	})
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
				log.Printf("Position | [%s %s] Error sending notification: %v", p.Symbol, p.StrategyName, err)
			}
		}
	}

	p.logStats()
}

// logStats logs trading statistics
func (p *position) logStats() {
	log.Printf("Position | [%s %s] Live Stats: Trades=%d, WinRate=%.2f%%, PnL=%.2f, MaxDD=%.2f, ProfitFactor=%.2f",
		p.Symbol, p.StrategyName, p.LiveTrades, p.LiveWinRate*100, p.LiveEquity, p.LiveMaxDrawdown, p.ProfitFactor)
	log.Printf("Position | [%s] REPORT:", p.StrategyName)
	log.Printf("  Trades=%d, Wins=%d, Losses=%d, WinRate=%.2f%%", p.LiveTrades, p.LiveWins, p.LiveLosses, p.LiveWinRate*100)
	log.Printf("  PnL=%.2f, MaxDrawdown=%.2f", p.LiveEquity, p.LiveMaxDrawdown)
	log.Printf("  Sharpe=%.2f, Expectancy=%.2f", p.Sharpe, p.Expectancy)
	log.Printf("  Risk: RiskPercent=%.2f, StopLossPercent=%.2f, TrailingStopPercent=%.2f",
		p.RiskParams.RiskPercent, p.RiskParams.StopLossPercent, p.RiskParams.TrailingStopPercent)

	// Log trade history
	log.Printf("  Trade Log:")
	for i, t := range p.LiveTradeLog {
		log.Printf("    Trade %d: Entry=%.2f at %s, Exit=%.2f at %s, PnL=%.2f",
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
	if signal.Position == strategy.Hold {
		posStr = "hold"
	} else if signal.Position == strategy.LongBullish {
		posStr = "long bullish"
	} else if signal.Position == strategy.LongBearish {
		posStr = "long bearish"
	} else if signal.Position == strategy.ShortBullish {
		posStr = "short bullish"
	} else if signal.Position == strategy.ShortBearish {
		posStr = "short bearish"
	}

	msg := fmt.Sprintf("[%s %s] signal for %s at %s: %s (%s)",
		p.Symbol, signal.StrategyName, p.Symbol, timestamp, posStr, signal.Reason)

	if err := p.Notifier.SendWithRetry(msg); err != nil {
		log.Printf("Position | [%s %s] Notification failed: %v", p.Symbol, signal.StrategyName, err)

		if p.Storage != nil {
			err = p.Storage.LogEvent(ctx, journal.Event{
				Time:        time.Now(),
				Type:        "error",
				Description: "notification",
				Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "error": err.Error(), "msg": msg},
			})
			if err != nil {
				log.Printf("Position | [%s %s] Error logging event: %v", p.Symbol, signal.StrategyName, err)
			}
		}
	}
}

// flush saves trade logs and equity curves to persistent storage
func (p *position) flush() error {
	if p.Storage == nil {
		return nil
	}

	// Save trade log
	// p.DB.SaveTradeLog(p.Symbol, "timeframe", p.LiveTradeLog)

	// Save equity curve
	// p.DB.SaveEquityCurve(p.Symbol, "timeframe", p.LiveEquityCurve)

	// Save position state
	if err := p.Save(); err != nil {
		return fmt.Errorf("failed to save position: %w", err)
	}

	return nil
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
func (p *position) OnTick(ctx context.Context, tick Tick, depthState map[string]exchange.OrderBook, marketCapState *exchange.MarketCapData) {
	if tick == nil {
		log.Printf("Position | [%s %s] Received nil tick, ignoring", p.Symbol, p.StrategyName)
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.Active || p.TradingDisabled {
		return
	}

	// Store market depth for later use if provided
	var bestBuyPrice, bestSellPrice float64
	if depthState != nil {
		// Process market depth data
		buyBook, ok := depthState[exchange.GetBuyDepthKey(p.Symbol)]
		if ok {
			// Get best buy price (highest price in buy book)
			for _, entry := range buyBook {
				if price, err := strconv.ParseFloat(entry.Price, 64); err == nil {
					if price > bestBuyPrice {
						bestBuyPrice = price
					}
				}
			}
			log.Printf("Position | [%s %s] Best buy price: %.8f", p.Symbol, p.StrategyName, bestBuyPrice)
		}

		sellBook, ok := depthState[exchange.GetSellDepthKey(p.Symbol)]
		if ok {
			// Get best sell price (lowest price in sell book)
			first := true
			for _, entry := range sellBook {
				if price, err := strconv.ParseFloat(entry.Price, 64); err == nil {
					if first || price < bestSellPrice {
						bestSellPrice = price
						first = false
					}
				}
			}
			log.Printf("Position | [%s %s] Best sell price: %.8f", p.Symbol, p.StrategyName, bestSellPrice)
		}
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
	price := tick.Price()
	if marketCapPrice > 0 {
		// If market cap price is within 1% of tick price, we can use it for validation
		priceDiff := math.Abs(price-marketCapPrice) / price
		if priceDiff > 0.01 {
			log.Printf("Position | [%s %s] Warning: Tick price (%.2f) differs significantly from market cap price (%.2f)",
				p.Symbol, p.StrategyName, price, marketCapPrice)
		}
	}

	// Create backup of current state for rollback
	backup := p.backupState()

	// Execute with transaction management
	err := executeWithTransaction(ctx, p.Storage, func(txCtx context.Context) error {
		// Use tick.Price for all stop/take/trailing logic
		if p.shouldTriggerStopLoss(price) {
			return p.executeStopLoss(txCtx, price)
		}
		if p.shouldTriggerTakeProfit(price) {
			return p.executeTakeProfit(txCtx, price)
		}
		if p.RiskParams.TrailingStopPercent > 0 {
			p.updateTrailingStop(price)
			if p.shouldTriggerTrailingStop(price) {
				return p.executeTrailingStop(txCtx, price)
			}
		}
		return nil
	})

	// Handle errors with rollback
	if err != nil {
		log.Printf("Position | [%s %s] Error processing tick, rolling back state: %v", p.Symbol, p.StrategyName, err)
		p.rollbackState(backup)

		// Log the error
		if p.Storage != nil {
			p.Storage.LogEvent(ctx, journal.Event{
				Time:        time.Now(),
				Type:        "error",
				Description: "tick_processing_failed",
				Data:        map[string]any{"symbol": p.Symbol, "strategy_name": p.StrategyName, "error": err.Error(), "price": price},
			})
		}
		return
	}
}

// OnSignal processes a new signal for open/close logic (lower frequency)
func (p *position) OnSignal(ctx context.Context, signal strategy.Signal) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// If its new trading day, reset daily stats
	if time.Now().Day() != p.Time.Day() {
		p.ResetDailyStats()
	}

	if p.TradingDisabled {
		log.Printf("Position | [%s %s] Trading disabled, ignoring signal", p.Symbol, signal.StrategyName)
		return
	}

	// Validate signal
	if signal.Position == strategy.Hold {
		log.Printf("Position | [%s %s] Received empty signal action, ignoring", p.Symbol, p.StrategyName)
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
				return p.executeManualExit(txCtx, signal.TriggerPrice)
			}
		} else {
			// Open position (buy signal)
			if signal.Position == strategy.LongBullish || signal.Position == strategy.ShortBearish {
				return p.executeEntry(txCtx, signal)
			}
		}
		return nil
	})

	// Handle errors with rollback
	if err != nil {
		log.Printf("Position | [%s %s] Error processing signal, rolling back state: %v", p.Symbol, signal.StrategyName, err)
		p.rollbackState(backup)

		// Log the error
		if p.Storage != nil {
			p.Storage.LogEvent(ctx, journal.Event{
				Time:        time.Now(),
				Type:        "error",
				Description: "signal_processing_failed",
				Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "error": err.Error(), "signal": signal},
			})
		}
		return
	}
	// Send signal notification
	p.sendSignalNotification(ctx, signal)
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
