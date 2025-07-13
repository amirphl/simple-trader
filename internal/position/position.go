// Package position
package position

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
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

// Manager handles position management for a strategy
type Manager interface {
	// ProcessSignal processes a trading signal and manages positions accordingly
	ProcessSignal(ctx context.Context, signal strategy.Signal, riskParams config.RiskParams) error

	// GetDailyPnL returns the current day's profit and loss
	GetDailyPnL(ctx context.Context) (float64, error)

	// GetStats returns position statistics
	GetStats(ctx context.Context) (map[string]interface{}, error)

	// ResetDailyStats resets daily statistics
	ResetDailyStats(ctx context.Context) error
}

// DefaultManager implements the Manager interface
type DefaultManager struct {
	strategyName string
	symbol       string
	db           db.DB
	position     *Position
	mu           sync.RWMutex
}

// logEvent is a helper function to handle the mismatch between interfaces
// The db.DB interface expects a context parameter for LogEvent, but the journal.Journaler interface doesn't
func (m *DefaultManager) logEvent(ctx context.Context, event journal.Event) {
	// Use type assertion to access the context-aware LogEvent method
	if dbWithCtx, ok := m.db.(interface {
		LogEvent(ctx context.Context, event journal.Event) error
	}); ok {
		if err := dbWithCtx.LogEvent(ctx, event); err != nil {
			log.Printf("Failed to log event: %v", err)
		}
	} else {
		// Fall back to the standard interface method
		if err := m.db.LogEvent(ctx, event); err != nil {
			log.Printf("Failed to log event: %v", err)
		}
	}
}

// NewManager creates a new position manager
func NewManager(ctx context.Context, strategyName, symbol string, database db.DB, cfg config.Config) (Manager, error) {
	// Try to load existing position
	pos, err := LoadFromDB(ctx, database, strategyName)
	if err != nil {
		// If no position exists, create a new one
		log.Printf("No existing position found for %s, creating new position", strategyName)

		// Get risk parameters for this strategy
		riskParams := config.GetRiskParams(cfg, strategyName)

		pos = &Position{
			Symbol:            symbol,
			Active:            false,
			Time:              time.Now(),
			LiveWinPnls:       make([]float64, 0),
			LiveLossPnls:      make([]float64, 0),
			LiveTradeLog:      make([]Trade, 0),
			RiskParams:        riskParams,
			TakeProfitPercent: cfg.TakeProfitPercent,
			LimitSpread:       cfg.LimitSpread,
			MaxDailyLoss:      cfg.MaxDailyLoss,
			Balance:           0, // This should be set by the caller
		}

		// Set order configuration
		pos.OrderSpec.Type = cfg.OrderType
		pos.OrderSpec.MaxAttemps = 3                // Default value
		pos.OrderSpec.Delay = cfg.NotificationDelay // Reuse notification delay for order retries
	}

	return &DefaultManager{
		strategyName: strategyName,
		symbol:       symbol,
		db:           database,
		position:     pos,
	}, nil
}

// ProcessSignal processes a trading signal
func (m *DefaultManager) ProcessSignal(ctx context.Context, signal strategy.Signal, riskParams config.RiskParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Set risk parameters
	m.position.RiskParams = riskParams

	// Process signal based on position state
	if m.position.Active {
		return m.handleActivePosition(ctx, signal)
	} else {
		return m.handleInactivePosition(ctx, signal)
	}
}

// handleActivePosition manages active positions
func (m *DefaultManager) handleActivePosition(ctx context.Context, signal strategy.Signal) error {
	p := m.position

	// Check stop loss
	if p.shouldTriggerStopLoss(signal) {
		return m.executeExit(ctx, signal, "stop_loss")
	}

	// Handle trailing stop
	if p.RiskParams.TrailingStopPercent > 0 {
		p.updateTrailingStop(signal)
		if p.shouldTriggerTrailingStop(signal) {
			return m.executeExit(ctx, signal, "trailing_stop")
		}
	}

	// Handle take profit
	if p.TakeProfitPercent > 0 && p.shouldTriggerTakeProfit(signal) {
		return m.executeExit(ctx, signal, "take_profit")
	}

	// Handle manual exit signals
	if (signal.Action == "sell" && p.Side == "buy") || (signal.Action == "buy" && p.Side == "sell") {
		return m.executeExit(ctx, signal, "signal")
	}

	return nil
}

// handleInactivePosition manages inactive positions
func (m *DefaultManager) handleInactivePosition(ctx context.Context, signal strategy.Signal) error {
	// Support both buy and sell signals for futures trading
	if signal.Action == "buy" || signal.Action == "sell" {
		return m.executeEntry(ctx, signal)
	}

	return nil
}

// executeEntry executes an entry order
func (m *DefaultManager) executeEntry(ctx context.Context, signal strategy.Signal) error {
	p := m.position

	// Log entry attempt
	m.logEvent(ctx, journal.Event{
		Time:        time.Now(),
		Type:        "position",
		Description: "entry_attempt",
		Data: map[string]any{
			"strategy": m.strategyName,
			"symbol":   m.symbol,
			"signal":   signal,
		},
	})

	// In a real implementation, this would create and submit an order
	// For now, simulate a successful entry
	p.Side = signal.Action
	p.Entry = signal.TriggerPrice
	p.Size = 1.0 // This would be calculated based on risk
	p.Active = true
	p.Time = time.Now()

	// Log entry success
	m.logEvent(ctx, journal.Event{
		Time:        time.Now(),
		Type:        "position",
		Description: "entry_success",
		Data: map[string]any{
			"strategy": m.strategyName,
			"symbol":   m.symbol,
			"price":    p.Entry,
			"size":     p.Size,
			"side":     p.Side,
		},
	})

	// Save position to database
	return m.savePosition(ctx)
}

// executeExit executes an exit order
func (m *DefaultManager) executeExit(ctx context.Context, signal strategy.Signal, reason string) error {
	p := m.position

	// Log exit attempt
	m.logEvent(ctx, journal.Event{
		Time:        time.Now(),
		Type:        "position",
		Description: "exit_attempt",
		Data: map[string]any{
			"strategy": m.strategyName,
			"symbol":   m.symbol,
			"reason":   reason,
			"signal":   signal,
		},
	})

	// Calculate PnL
	exitPrice := signal.TriggerPrice
	var pnl float64

	if p.Side == "buy" {
		pnl = (exitPrice - p.Entry) * p.Size
	} else {
		pnl = (p.Entry - exitPrice) * p.Size
	}

	// Update position statistics
	p.LastPNL = pnl
	p.LiveTrades++

	if pnl > 0 {
		p.LiveWins++
		p.LiveWinPnls = append(p.LiveWinPnls, pnl)
	} else {
		p.LiveLosses++
		p.LiveLossPnls = append(p.LiveLossPnls, pnl)
	}

	p.LiveEquity += pnl
	p.LiveEquityCurve = append(p.LiveEquityCurve, p.LiveEquity)

	if p.LiveEquity > p.LiveMaxEquity {
		p.LiveMaxEquity = p.LiveEquity
	}

	dd := p.LiveMaxEquity - p.LiveEquity
	if dd > p.LiveMaxDrawdown {
		p.LiveMaxDrawdown = dd
	}

	// Add to trade log
	p.LiveTradeLog = append(p.LiveTradeLog, Trade{
		Entry:     p.Entry,
		Exit:      exitPrice,
		PnL:       pnl,
		EntryTime: p.Time,
		ExitTime:  time.Now(),
	})

	// Update statistics
	p.calculateStats()

	// Reset position
	p.Active = false
	p.TrailingStop = 0

	// Log exit success
	m.logEvent(ctx, journal.Event{
		Time:        time.Now(),
		Type:        "position",
		Description: "exit_success",
		Data: map[string]any{
			"strategy":   m.strategyName,
			"symbol":     m.symbol,
			"entry":      p.Entry,
			"exit":       exitPrice,
			"pnl":        pnl,
			"reason":     reason,
			"cumulative": p.LiveEquity,
		},
	})

	// Save position to database
	return m.savePosition(ctx)
}

// GetDailyPnL returns the current day's profit and loss
func (m *DefaultManager) GetDailyPnL(ctx context.Context) (float64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Calculate daily PnL from trades that occurred today
	var dailyPnL float64
	today := time.Now().Truncate(24 * time.Hour)

	for _, trade := range m.position.LiveTradeLog {
		if trade.ExitTime.After(today) {
			dailyPnL += trade.PnL
		}
	}

	return dailyPnL, nil
}

// GetStats returns position statistics
func (m *DefaultManager) GetStats(ctx context.Context) (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	p := m.position

	stats := map[string]interface{}{
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
	}

	if p.Active {
		stats["side"] = p.Side
		stats["entry"] = p.Entry
		stats["size"] = p.Size
		stats["time"] = p.Time
	}

	return stats, nil
}

// ResetDailyStats resets daily statistics
func (m *DefaultManager) ResetDailyStats(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Keep only trades from previous days
	today := time.Now().Truncate(24 * time.Hour)
	var oldTrades []Trade

	for _, trade := range m.position.LiveTradeLog {
		if trade.ExitTime.Before(today) {
			oldTrades = append(oldTrades, trade)
		}
	}

	m.position.LiveTradeLog = oldTrades

	// Recalculate statistics
	m.position.calculateStats()

	// Save position to database
	return m.savePosition(ctx)
}

// savePosition saves the position to the database
func (m *DefaultManager) savePosition(ctx context.Context) error {
	// In a real implementation, this would save to the database
	// For now, just log
	log.Printf("[%s] Position saved: active=%v", m.strategyName, m.position.Active)
	return nil
}

// LoadFromDB loads a position from the database
func LoadFromDB(ctx context.Context, database db.DB, strategyName string) (*Position, error) {
	// In a real implementation, this would load from the database
	// For now, try to load from file as a fallback
	return Load(strategyName)
}

// Position represents a trading position with risk management and statistics
type Position struct {
	// Core position data
	Symbol  string    `json:"symbol"`
	Side    string    `json:"side"`
	Entry   float64   `json:"entry"`
	Size    float64   `json:"size"`
	OrderID string    `json:"order_id"`
	Active  bool      `json:"active"`
	Time    time.Time `json:"time"`

	// Risk management
	RiskParams        config.RiskParams `json:"risk_params"`
	TakeProfitPercent float64           `json:"take_profit_percent"`
	LimitSpread       float64           `json:"limit_spread"`
	MaxDailyLoss      float64           `json:"max_daily_loss"`
	Balance           float64           `json:"balance"`

	// Order configuration
	OrderSpec struct {
		Type       string        `json:"type"`
		MaxAttemps int           `json:"max_attempts"` // Fixed typo
		Delay      time.Duration `json:"delay"`
	} `json:"order_spec"`

	// Dependencies (not serialized)
	Exchange exchange.Exchange `json:"-"`
	DB       db.DB             `json:"-"`
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

	TradingDisabled bool    `json:"trading_disabled"`
	LiveTradeLog    []Trade `json:"live_trade_log"`

	mu sync.RWMutex // Added mutex for thread safety
}

// Trade represents a single trade record
type Trade struct {
	Entry     float64   `json:"entry"`
	Exit      float64   `json:"exit"`
	PnL       float64   `json:"pnl"`
	EntryTime time.Time `json:"entry_time"`
	ExitTime  time.Time `json:"exit_time"`
}

const defaultFilePath = "open_position.json"

// Save persists the position to file with error handling
func (p *Position) Save() error {
	// TODO:
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal position: %w", err)
	}

	if err := os.WriteFile(defaultFilePath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write position file: %w", err)
	}

	return nil
}

// Load loads a position from file with proper error handling
func Load(stratName string) (*Position, error) {
	// TODO:
	var pos Position

	data, err := os.ReadFile(defaultFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("position file not found")
		}
		return nil, fmt.Errorf("failed to read position file: %w", err)
	}

	if err := json.Unmarshal(data, &pos); err != nil {
		return nil, fmt.Errorf("failed to unmarshal position: %w", err)
	}

	// Initialize dependencies would need to be done by caller
	// since they can't be serialized
	return &pos, nil
}

// Clear clears the position file
func Clear() error {
	if err := os.WriteFile(defaultFilePath, []byte("{}"), 0o644); err != nil {
		return fmt.Errorf("failed to clear position file: %w", err)
	}
	return nil
}

// SetDependencies sets the external dependencies that can't be serialized
func (p *Position) SetDependencies(exchange exchange.Exchange, db db.DB, notifier notifier.Notifier) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.Exchange = exchange
	p.DB = db
	p.Notifier = notifier
}

// IsActive returns whether the position is active (thread-safe)
func (p *Position) IsActive() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.Active
}

// GetLastPNL returns the current PNL (thread-safe)
func (p *Position) GetLastPNL() float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.LastPNL
}

// OnSignal handles incoming trading signals
func (p *Position) OnSignal(ctx context.Context, signal strategy.Signal) {
	p.mu.Lock()
	defer p.mu.Unlock()

	defer func() {
		// Flush logs and data to persistent storage
		if err := p.flush(); err != nil {
			log.Printf("Failed to flush data to storage: %v", err)
		}
	}()

	// Check if trading is disabled
	if p.TradingDisabled {
		log.Printf("[%s %s] Trading disabled, ignoring signal", p.Symbol, signal.StrategyName)
		return
	}

	if p.Active {
		p.handleActivePosition(ctx, signal)
	} else {
		p.handleInactivePosition(ctx, signal)
	}

	// Send signal notification
	p.sendSignalNotification(ctx, signal)
}

// handleActivePosition manages active positions
func (p *Position) handleActivePosition(ctx context.Context, signal strategy.Signal) {
	// Check stop loss
	if p.shouldTriggerStopLoss(signal) {
		p.executeStopLoss(ctx, signal)
		return
	}

	// Handle trailing stop
	if p.RiskParams.TrailingStopPercent > 0 {
		p.updateTrailingStop(signal)
		if p.shouldTriggerTrailingStop(signal) {
			p.executeTrailingStop(ctx, signal)
			return
		}
	}

	// Handle take profit
	if p.TakeProfitPercent > 0 && p.shouldTriggerTakeProfit(signal) {
		p.executeTakeProfit(ctx, signal)
		return
	}

	// Handle manual exit signals
	if (signal.Action == "sell" && p.Side == "buy") || (signal.Action == "buy" && p.Side == "sell") {
		p.executeManualExit(ctx, signal)
	}
}

// handleInactivePosition manages inactive positions
func (p *Position) handleInactivePosition(ctx context.Context, signal strategy.Signal) {
	// Support both buy and sell signals for futures trading
	if signal.Action == "buy" || signal.Action == "sell" {
		// TODO: What if this is the second buy?
		p.executeBuy(ctx, signal)
	}
}

// shouldTriggerStopLoss checks if stop loss should be triggered
func (p *Position) shouldTriggerStopLoss(signal strategy.Signal) bool {
	if p.Side == "buy" {
		return signal.TriggerPrice <= p.Entry*(1-p.RiskParams.StopLossPercent/100)
	}
	// For sell positions (short)
	return signal.TriggerPrice >= p.Entry*(1+p.RiskParams.StopLossPercent/100)
}

// shouldTriggerTakeProfit checks if take profit should be triggered
func (p *Position) shouldTriggerTakeProfit(signal strategy.Signal) bool {
	if p.Side == "buy" {
		return signal.TriggerPrice >= p.Entry*(1+p.TakeProfitPercent/100)
	}
	// For sell positions (short)
	return signal.TriggerPrice <= p.Entry*(1-p.TakeProfitPercent/100)
}

// updateTrailingStop updates the trailing stop level
func (p *Position) updateTrailingStop(signal strategy.Signal) {
	if p.Side == "buy" {
		profit := signal.TriggerPrice - p.Entry
		if profit > p.TrailingStop {
			p.TrailingStop = profit
		}
	} else {
		// For short positions
		profit := p.Entry - signal.TriggerPrice
		if profit > p.TrailingStop {
			p.TrailingStop = profit
		}
	}
}

// shouldTriggerTrailingStop checks if trailing stop should be triggered
func (p *Position) shouldTriggerTrailingStop(signal strategy.Signal) bool {
	if p.Side == "buy" {
		return signal.TriggerPrice <= p.Entry+p.TrailingStop-(p.Entry*p.RiskParams.TrailingStopPercent/100)
	}
	// For short positions
	return signal.TriggerPrice >= p.Entry-p.TrailingStop+(p.Entry*p.RiskParams.TrailingStopPercent/100)
}

// executeStopLoss executes a stop loss order
func (p *Position) executeStopLoss(ctx context.Context, signal strategy.Signal) {
	orderReq := p.createExitOrder(signal, "stop_loss")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(orderReq, p.OrderSpec.MaxAttemps, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "stop_loss_order", signal.StrategyName)
		return
	}

	p.handleOrderSuccess(ctx, orderResp, signal, "Stop Loss")
	p.onPositionClose(signal)
	// p.updateLiveStats(signal) // TODO:
}

// executeTrailingStop executes a trailing stop order
func (p *Position) executeTrailingStop(ctx context.Context, signal strategy.Signal) {
	orderReq := p.createExitOrder(signal, "trailing_stop")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(orderReq, p.OrderSpec.MaxAttemps, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "trailing_stop_order", signal.StrategyName)
		return
	}

	p.handleOrderSuccess(ctx, orderResp, signal, "Trailing Stop")
	p.onPositionClose(signal)
	p.updateLiveStats(signal)
}

// executeTakeProfit executes a take profit order
func (p *Position) executeTakeProfit(ctx context.Context, signal strategy.Signal) {
	orderReq := p.createExitOrder(signal, "take_profit")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(orderReq, p.OrderSpec.MaxAttemps, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "take_profit_order", signal.StrategyName)
		return
	}

	p.handleOrderSuccess(ctx, orderResp, signal, "Take Profit")
	p.onPositionClose(signal)
	p.updateLiveStats(signal)
}

// executeManualExit executes a manual exit order for both long and short positions
func (p *Position) executeManualExit(ctx context.Context, signal strategy.Signal) {
	orderReq := p.createExitOrder(signal, "manual_exit")
	orderResp, err := p.Exchange.SubmitOrderWithRetry(orderReq, p.OrderSpec.MaxAttemps, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "order_submission", signal.StrategyName)
		return
	}

	p.handleOrderSuccess(ctx, orderResp, signal, "Manual")
	p.onPositionClose(signal)
	p.updateLiveStats(signal)
}

// executeBuy executes a buy order
func (p *Position) executeBuy(ctx context.Context, signal strategy.Signal) {
	// Check if we have enough balance
	if p.Balance <= 0 {
		log.Printf("[%s %s] Insufficient balance: %.2f", p.Symbol, signal.StrategyName, p.Balance)
		return
	}

	// Support both long and short positions based on signal
	side := signal.Action
	orderReq := p.createEntryOrder(signal, side)
	orderResp, err := p.Exchange.SubmitOrderWithRetry(orderReq, p.OrderSpec.MaxAttemps, p.OrderSpec.Delay)
	if err != nil {
		p.handleOrderError(ctx, err, "order_submission", signal.StrategyName)
		return
	}

	p.handleEntrySuccess(ctx, orderResp, signal, orderReq.Quantity, side)
}

// createExitOrder creates an exit order based on position side
func (p *Position) createExitOrder(signal strategy.Signal, orderType string) order.OrderRequest {
	side := "sell"
	if p.Side == "sell" {
		side = "buy"
	}

	orderReq := order.OrderRequest{
		Symbol:   p.Symbol,
		Side:     side,
		Type:     p.OrderSpec.Type,
		Price:    0,
		Quantity: p.Size,
	}

	if p.OrderSpec.Type == "limit" {
		orderReq.Price = signal.TriggerPrice
		// Apply spread for better execution
		if p.LimitSpread > 0 {
			if side == "sell" {
				orderReq.Price = signal.TriggerPrice * (1 - p.LimitSpread/100)
			} else {
				orderReq.Price = signal.TriggerPrice * (1 + p.LimitSpread/100)
			}
		}
	}

	return orderReq
}

// createEntryOrder creates an entry order (buy or sell for futures)
func (p *Position) createEntryOrder(signal strategy.Signal, side string) order.OrderRequest {
	// Calculate position size based on risk
	riskAmount := p.Balance * p.RiskParams.RiskPercent / 100
	stopLossAmount := signal.TriggerPrice * p.RiskParams.StopLossPercent / 100
	orderSize := riskAmount / stopLossAmount

	// Ensure we don't exceed balance
	maxSize := p.Balance / signal.TriggerPrice
	if orderSize > maxSize {
		orderSize = maxSize
	}

	var orderReq order.OrderRequest

	switch p.OrderSpec.Type {
	case "limit":
		orderReq = order.OrderRequest{
			Symbol:   p.Symbol,
			Side:     side,
			Type:     "limit",
			Price:    signal.TriggerPrice,
			Quantity: orderSize,
		}
		if p.LimitSpread > 0 {
			if side == "buy" {
				orderReq.Price = signal.TriggerPrice * (1 - p.LimitSpread/100)
			} else {
				orderReq.Price = signal.TriggerPrice * (1 + p.LimitSpread/100)
			}
		}
	case "stop-limit":
		orderReq = order.OrderRequest{
			Symbol:    p.Symbol,
			Side:      side,
			Type:      "stop-limit",
			Price:     signal.TriggerPrice,
			StopPrice: signal.TriggerPrice * 1.001,
			Quantity:  orderSize,
		}
	case "oco":
		limitOrder := order.OrderRequest{
			Symbol:   p.Symbol,
			Side:     side,
			Type:     "limit",
			Price:    signal.TriggerPrice,
			Quantity: orderSize,
		}
		stopOrder := order.OrderRequest{
			Symbol:    p.Symbol,
			Side:      side,
			Type:      "stop-limit",
			Price:     signal.TriggerPrice,
			StopPrice: signal.TriggerPrice * 1.001,
			Quantity:  orderSize,
		}
		orderReq = order.OrderRequest{
			Symbol:      p.Symbol,
			Side:        side,
			Type:        "limit",
			Price:       signal.TriggerPrice,
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
func (p *Position) handleEntrySuccess(ctx context.Context, orderResp order.OrderResponse, signal strategy.Signal, orderSize float64, side string) {
	log.Printf("[%s %s] Order submitted: %+v", p.Symbol, signal.StrategyName, orderResp)

	if p.DB != nil {
		p.DB.SaveOrder(ctx, orderResp)
		p.DB.LogEvent(ctx, journal.Event{
			Time:        time.Now(),
			Type:        "order",
			Description: "order_submitted",
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "order": orderResp},
		})
	}

	// Update position
	p.Side = side
	p.Entry = signal.TriggerPrice
	p.Size = orderSize
	p.OrderID = orderResp.OrderID
	p.Active = true
	p.Time = time.Now()

	// Reduce balance
	p.Balance -= signal.TriggerPrice * orderSize

	if p.Notifier != nil {
		msg := fmt.Sprintf("[ORDER FILLED]\nSide: %s\nSymbol: %s\nQty: %.4f\nPrice: %.2f\nType: %s\nOrderID: %s\nEvent: Manual\nTime: %s",
			side, p.Symbol, orderSize, signal.TriggerPrice, p.OrderSpec.Type, orderResp.OrderID, time.Now().Format(time.RFC3339))
		p.Notifier.SendWithRetry(msg)
	}
}

// handleOrderError handles order execution errors
func (p *Position) handleOrderError(ctx context.Context, err error, description, strategyName string) {
	log.Printf("[%s %s] %s failed: %v", p.Symbol, strategyName, description, err)

	if p.DB != nil {
		p.DB.LogEvent(ctx, journal.Event{
			Time:        time.Now(),
			Type:        "error",
			Description: description,
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": strategyName, "error": err.Error()},
		})
	}

	if p.Notifier != nil {
		msg := fmt.Sprintf("ERROR: %s: %v", description, err)
		p.Notifier.SendWithRetry(msg)
	}
}

// handleOrderSuccess handles successful order execution
func (p *Position) handleOrderSuccess(ctx context.Context, orderResp order.OrderResponse, signal strategy.Signal, event string) {
	log.Printf("[%s %s] %s triggered, order submitted: %+v", p.Symbol, signal.StrategyName, event, orderResp)

	if p.DB != nil {
		p.DB.SaveOrder(ctx, orderResp)
		p.DB.LogEvent(ctx, journal.Event{
			Time:        time.Now(),
			Type:        "order",
			Description: fmt.Sprintf("%s_triggered", event),
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "order": orderResp},
		})
	}

	p.LastPNL = p.calculatePNL(signal.TriggerPrice)

	if p.Notifier != nil {
		msg := fmt.Sprintf("[ORDER FILLED]\nSide: %s\nSymbol: %s\nQty: %.4f\nPrice: %.2f\nType: %s\nOrderID: %s\nEvent: %s\nPnL: %.2f\nTime: %s",
			p.getExitSide(), p.Symbol, p.Size, signal.TriggerPrice, p.OrderSpec.Type, orderResp.OrderID, event, p.LastPNL, time.Now().Format(time.RFC3339))
		p.Notifier.SendWithRetry(msg)
	}
}

// handleBuySuccess handles successful buy order execution
func (p *Position) handleBuySuccess(ctx context.Context, orderResp order.OrderResponse, signal strategy.Signal, orderSize float64) {
	log.Printf("[%s %s] Order submitted: %+v", p.Symbol, signal.StrategyName, orderResp)

	if p.DB != nil {
		p.DB.SaveOrder(ctx, orderResp)
		p.DB.LogEvent(ctx, journal.Event{
			Time:        time.Now(),
			Type:        "order",
			Description: "order_submitted",
			Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "order": orderResp},
		})
	}

	// Update position
	p.Side = "buy"
	p.Entry = signal.TriggerPrice
	p.Size = orderSize
	p.OrderID = orderResp.OrderID
	p.Active = true
	p.Time = time.Now()

	// Reduce balance
	p.Balance -= signal.TriggerPrice * orderSize

	if p.Notifier != nil {
		msg := fmt.Sprintf("[ORDER FILLED]\nSide: buy\nSymbol: %s\nQty: %.4f\nPrice: %.2f\nType: %s\nOrderID: %s\nEvent: Manual\nTime: %s",
			p.Symbol, orderSize, signal.TriggerPrice, p.OrderSpec.Type, orderResp.OrderID, time.Now().Format(time.RFC3339))
		p.Notifier.SendWithRetry(msg)
	}
}

// calculatePNL calculates the current PNL
func (p *Position) calculatePNL(currentPrice float64) float64 {
	if p.Side == "buy" {
		return (currentPrice - p.Entry) * p.Size
	}
	// For short positions
	return (p.Entry - currentPrice) * p.Size
}

// getExitSide returns the opposite side for exit orders
func (p *Position) getExitSide() string {
	if p.Side == "buy" {
		return "sell"
	}
	return "buy"
}

// onPositionClose handles position closing logic
func (p *Position) onPositionClose(signal strategy.Signal) {
	p.Active = false
	// Update balance with PNL
	p.Balance += p.LastPNL

	// Add to trade log
	p.LiveTradeLog = append(p.LiveTradeLog, Trade{
		Entry:     p.Entry,
		Exit:      signal.TriggerPrice,
		PnL:       p.LastPNL,
		EntryTime: p.Time,
		ExitTime:  time.Now(),
	})
}

// updateLiveStats updates live trading statistics
func (p *Position) updateLiveStats(signal strategy.Signal) {
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

	// Check daily loss limit
	if p.LiveEquity < -p.MaxDailyLoss {
		p.TradingDisabled = true
		if p.Notifier != nil {
			msg := fmt.Sprintf("⚠️ DAILY LOSS LIMIT REACHED\nSymbol: %s\nStrategyName: %s\nDaily PnL: %.2f\nLimit: %.2f\nTrading disabled until next day",
				p.Symbol, signal.StrategyName, p.LiveEquity, p.MaxDailyLoss)
			p.Notifier.SendWithRetry(msg)
		}
	}

	// Calculate statistics
	p.calculateStats()
	p.logStats(signal.StrategyName)
}

// calculateStats calculates trading statistics
func (p *Position) calculateStats() {
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
}

// logStats logs trading statistics
func (p *Position) logStats(strategyName string) {
	log.Printf("[%s %s] Live Stats: Trades=%d, WinRate=%.2f%%, PnL=%.2f, MaxDD=%.2f, ProfitFactor=%.2f",
		p.Symbol, strategyName, p.LiveTrades, p.LiveWinRate*100, p.LiveEquity, p.LiveMaxDrawdown, p.ProfitFactor)
	log.Printf("[%s] REPORT:", strategyName)
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
func (p *Position) sendSignalNotification(ctx context.Context, signal strategy.Signal) {
	if p.Notifier == nil {
		return
	}

	msg := fmt.Sprintf("[%s %s] signal for %s at %s: %s (%s)",
		p.Symbol, signal.StrategyName, p.Symbol, signal.Candle.Timestamp.Format(time.RFC3339), signal.Action, signal.Reason)

	if err := p.Notifier.SendWithRetry(msg); err != nil {
		log.Printf("[%s %s] Notification failed: %v", p.Symbol, signal.StrategyName, err)
		if p.DB != nil {
			p.DB.LogEvent(ctx, journal.Event{
				Time:        time.Now(),
				Type:        "error",
				Description: "notification",
				Data:        map[string]any{"symbol": p.Symbol, "strategy_name": signal.StrategyName, "error": err.Error(), "msg": msg},
			})
		}
	}
}

// flush saves trade logs and equity curves to persistent storage
func (p *Position) flush() error {
	if p.DB == nil {
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

// TODO:
// ResetDailyStats resets daily trading statistics
func (p *Position) ResetDailyStats() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.TradingDisabled = false
	p.LiveEquity = 0
	p.LiveMaxEquity = 0
	p.LiveMaxDrawdown = 0
	// Note: Keep cumulative stats like LiveWins, LiveLosses, etc.
	// Reset only daily-specific metrics
}

// GetStats returns current trading statistics (thread-safe)
func (p *Position) GetStats() (wins, losses, trades int64, winRate, pnl, maxDD float64) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.LiveWins, p.LiveLosses, p.LiveTrades, p.LiveWinRate, p.LiveEquity, p.LiveMaxDrawdown
}
