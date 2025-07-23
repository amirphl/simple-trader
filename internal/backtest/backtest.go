// Package backtest
package backtest

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"math/rand"
	"strconv"

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/indicator"
	"github.com/amirphl/simple-trader/internal/strategy"
)

// runBacktest handles the backtest mode
func RunBacktest(
	ctx context.Context,
	cfg config.Config,
	strats []strategy.Strategy,
	storage db.DB,
) {
	for _, strat := range strats {
		// Load candles for backtesting
		candles, err := loadBacktestCandles(ctx, storage, strat.Symbol(), strat.Timeframe(), cfg.BacktestFrom.Time, cfg.BacktestTo.Time, cfg)
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

// getSideString converts a Position enum to its string representation
func getSideString(pos strategy.Position) string {
	switch pos {
	case strategy.LongBullish:
		return "long"
	case strategy.ShortBearish:
		return "short"
	default:
		return "hold"
	}
}

// loadBacktestCandles loads candles for backtesting, downloading from public API if necessary
func loadBacktestCandles(
	ctx context.Context,
	storage db.DB,
	symbol, timeframe string,
	from, to time.Time,
	cfg config.Config,
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
			downloadedCandles, err := downloadCandlesFromPublicAPIWithRetry(
				downloadCtx,
				symbol,
				timeframe,
				currTime,
				next,
				cfg.ProxyURL,
				cfg.APIRetryMaxAttempts,
				cfg.APIRetryBaseDelay,
				cfg.APIRetryMaxDelay,
			)
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

// downloadCandlesFromPublicAPIWithRetry downloads candles with configurable retry parameters
func downloadCandlesFromPublicAPIWithRetry(ctx context.Context, symbol, timeframe string, start, end time.Time, proxyURL string, maxRetries int, baseDelay, maxDelay time.Duration) ([]candle.Candle, error) {
	// Retry configuration
	const (
		backoffFactor = 2.0
		jitterRange   = 0.1 // ±10% jitter
	)

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
		log.Printf("downloadCandlesFromPublicAPI | Using proxy: %s", proxyURL)
	}

	client := &http.Client{
		Timeout:   30 * time.Second, // Increased timeout for proxy environments
		Transport: transport,
	}

	// Retry loop with exponential backoff
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		// Create request with context
		req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
		if err != nil {
			return nil, fmt.Errorf("error creating request: %w", err)
		}

		// Set headers
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "application/json")

		// Attempt request
		log.Printf("downloadCandlesFromPublicAPI | Attempt %d/%d for %s", attempt+1, maxRetries, symbol)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("network error on attempt %d: %w", attempt+1, err)
			log.Printf("downloadCandlesFromPublicAPI | %v", lastErr)

			// Check if we should retry
			if attempt < maxRetries-1 {
				delay := calculateRetryDelay(attempt, baseDelay, maxDelay, backoffFactor, jitterRange)
				log.Printf("downloadCandlesFromPublicAPI | Retrying in %v...", delay)

				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			// Last attempt failed, return error
			continue
		}

		// Check response status
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			lastErr = fmt.Errorf("API error (status %d) on attempt %d: %s", resp.StatusCode, attempt+1, string(body))
			log.Printf("downloadCandlesFromPublicAPI | %v", lastErr)

			// Check if this is a retryable error
			if isRetryableHTTPStatus(resp.StatusCode) && attempt < maxRetries-1 {
				delay := calculateRetryDelay(attempt, baseDelay, maxDelay, backoffFactor, jitterRange)
				log.Printf("downloadCandlesFromPublicAPI | Retrying in %v...", delay)

				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			// Non-retryable error or last attempt, return error
			continue
		}

		// Success! Process the response
		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()

		if err != nil {
			lastErr = fmt.Errorf("error reading response body on attempt %d: %w", attempt+1, err)
			log.Printf("downloadCandlesFromPublicAPI | %v", lastErr)

			if attempt < maxRetries-1 {
				delay := calculateRetryDelay(attempt, baseDelay, maxDelay, backoffFactor, jitterRange)
				log.Printf("downloadCandlesFromPublicAPI | Retrying in %v...", delay)

				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			continue
		}

		// Parse response body
		var rawCandles [][]any
		if err := json.Unmarshal(bodyBytes, &rawCandles); err != nil {
			lastErr = fmt.Errorf("JSON decode error on attempt %d: %w", attempt+1, err)
			log.Printf("downloadCandlesFromPublicAPI | %v", lastErr)

			if attempt < maxRetries-1 {
				delay := calculateRetryDelay(attempt, baseDelay, maxDelay, backoffFactor, jitterRange)
				log.Printf("downloadCandlesFromPublicAPI | Retrying in %v...", delay)

				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			continue
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

		log.Printf("downloadCandlesFromPublicAPI | Successfully downloaded %d candles for %s on attempt %d", len(candles), symbol, attempt+1)
		return candles, nil
	}

	// All retries exhausted
	return nil, fmt.Errorf("failed to download candles after %d attempts, last error: %w", maxRetries, lastErr)
}

// calculateRetryDelay calculates the delay for the next retry attempt with exponential backoff and jitter
func calculateRetryDelay(attempt int, baseDelay, maxDelay time.Duration, backoffFactor, jitterRange float64) time.Duration {
	// Calculate exponential backoff: baseDelay * backoffFactor^attempt
	delay := float64(baseDelay) * math.Pow(backoffFactor, float64(attempt))

	// Cap at maximum delay
	if delay > float64(maxDelay) {
		delay = float64(maxDelay)
	}

	// Add jitter to avoid thundering herd problem
	// Jitter range: ±jitterRange% of the delay
	jitter := delay * jitterRange * (2*rand.Float64() - 1) // Random value between -jitterRange and +jitterRange
	delay += jitter

	// Ensure delay is not negative
	if delay < 0 {
		delay = float64(baseDelay)
	}

	return time.Duration(delay)
}

// isRetryableHTTPStatus determines if an HTTP status code indicates a retryable error
func isRetryableHTTPStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusTooManyRequests, // 429 - Rate limit
		http.StatusInternalServerError, // 500 - Server error
		http.StatusBadGateway,          // 502 - Bad gateway
		http.StatusServiceUnavailable,  // 503 - Service unavailable
		http.StatusGatewayTimeout:      // 504 - Gateway timeout
		return true
	default:
		return false
	}
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
	Equity          float64            `json:"equity"`
	MaxEquity       float64            `json:"max_equity"`
	MaxDrawdown     float64            `json:"max_drawdown"`
	Wins            int                `json:"wins"`
	Losses          int                `json:"losses"`
	Trades          int                `json:"trades"`
	LongTrades      int                `json:"long_trades"`
	ShortTrades     int                `json:"short_trades"`
	WinPnls         []float64          `json:"win_pnls"`
	LossPnls        []float64          `json:"loss_pnls"`
	EquityCurve     []float64          `json:"equity_curve"`
	TradeLog        []TradeLogEntry    `json:"trade_log"`
	Metrics         map[string]float64 `json:"metrics"`
	MaxConsecLosses int                `json:"max_consec_losses"`
	MaxConsecWins   int                `json:"max_consec_wins"`
	StartingBalance float64            `json:"starting_balance"`
}

// TradeLogEntry represents a single trade in the backtest
type TradeLogEntry struct {
	Entry     float64   `json:"entry"`
	Exit      float64   `json:"exit"`
	PnL       float64   `json:"pnl"`
	EntryTime time.Time `json:"entry_time"`
	ExitTime  time.Time `json:"exit_time"`
	Side      string    `json:"side"`     // "long" or "short"
	Reason    string    `json:"reason"`   // Reason for exit (signal, stop-loss, trailing-stop)
	MAE       float64   `json:"mae"`      // Maximum Adverse Excursion
	MFE       float64   `json:"mfe"`      // Maximum Favorable Excursion
	Quantity  float64   `json:"quantity"` // Number of units/contracts traded
	Balance   float64   `json:"balance"`  // Account balance at entry
	Cost      float64   `json:"cost"`     // Total cost of the trade (entry price * quantity)
}

type Float64 float64

func (f Float64) MarshalJSON() ([]byte, error) {
	if math.IsNaN(float64(f)) {
		return []byte(`0`), nil
	}
	return json.Marshal(float64(f))
}

// Usage:
type Data struct {
	Value Float64 `json:"value"`
}

type Stochastic struct {
	K Float64 `json:"k"`
	D Float64 `json:"d"`
}

type CandleWithSignal struct {
	Candle       candle.Candle   `json:"candle"`
	HeikinAshi   candle.Candle   `json:"heikin_ashi,omitempty"`
	Stochastic   Stochastic      `json:"stochastic,omitempty"`
	Signal       strategy.Signal `json:"signal,omitempty"`
	TradeInfo    *TradeLogEntry  `json:"trade_info,omitempty"`
	Balance      float64         `json:"balance"`
	Equity       float64         `json:"equity"`
	ActiveTrade  bool            `json:"active_trade"`
	TradeDetails string          `json:"trade_details,omitempty"`
}

// runStrategyBacktest runs a backtest for a specific strategy
func runStrategyBacktest(strat strategy.Strategy, candles []candle.Candle, cfg config.Config) BacktestResults {

	var results BacktestResults
	results.Metrics = make(map[string]float64)
	results.StartingBalance = 10000.0 // Default starting balance
	results.Equity = results.StartingBalance
	currentBalance := results.StartingBalance

	// Initialize backtest variables
	var activeEntryPrice float64
	var active bool
	var activeSide strategy.Position
	var dailyPnL float64
	var lastDay int = -1
	var trailingStop float64
	var currentTrade *TradeLogEntry
	var consecutiveWins, consecutiveLosses int

	// For charting: store all candles and their signals
	var candlesWithSignals []CandleWithSignal

	// Generate Heikin Ashi candles for the entire dataset
	heikinAshiCandles := candle.GenerateHeikenAshiCandles(candles)

	var stochastics *indicator.StochasticResult
	// Generate Stochastic candles for the entire dataset
	if stochasticHeikinAshiStrat, ok := strat.(*strategy.StochasticHeikinAshi); ok {
		var err error
		stochastics, err = indicator.CalculateStochastic(candles,
			stochasticHeikinAshiStrat.PeriodK(),
			stochasticHeikinAshiStrat.SmoothK(),
			stochasticHeikinAshiStrat.PeriodD(),
		)
		if err != nil {
			log.Printf("runStrategyBacktest | Error generating Stochastic candles: %v", err)
		}
	} else {
		var err error
		stochastics, err = indicator.CalculateStochastic(candles, 14, 10, 3)
		if err != nil {
			log.Printf("runStrategyBacktest | Error generating Stochastic candles: %v", err)
		}
	}

	// Risk parameters
	stopLossPercent := cfg.StopLossPercent
	trailingStopPercent := cfg.TrailingStopPercent
	takeProfitPercent := cfg.TakeProfitPercent
	maxDailyLoss := cfg.MaxDailyLoss
	slippage := cfg.SlippagePercent / 100.0
	commission := cfg.CommissionPercent / 100.0
	riskPerTrade := cfg.RiskPercent / 100.0

	// Warmup period - ensure strategy has enough data before generating signals
	warmupPeriod := 0
	if s, ok := strat.(interface{ WarmupPeriod() int }); ok {
		warmupPeriod = s.WarmupPeriod()
	}
	if warmupPeriod <= 0 {
		warmupPeriod = 20 // Default warmup period if not specified
	}

	// Process each candle
	for i, c := range candles {
		// Reset daily PnL on new day
		if lastDay != c.Timestamp.Day() {
			dailyPnL = 0
			lastDay = c.Timestamp.Day()
		}

		// Skip warmup period
		isWarmup := i < warmupPeriod

		// Get signal from strategy
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		sig, err := strat.OnCandles(ctx, []candle.Candle{c})
		cancel()

		if err != nil {
			log.Printf("runStrategyBacktest | Error getting signal for candle %s: %v", c.Timestamp.Format(time.RFC3339), err)
			// Create a default hold signal if strategy fails
			sig = strategy.Signal{
				Time:         c.Timestamp,
				Position:     strategy.Hold,
				Reason:       "strategy error",
				StrategyName: strat.Name(),
				TriggerPrice: c.Close,
				Candle:       &c,
			}
		}

		// Get corresponding Heikin Ashi candle
		haCandle := heikinAshiCandles[i]

		// Get corresponding Stochastic candle
		stochasticK := stochastics.K[i]
		stochasticD := stochastics.D[i]

		// Create candle with signal data for charting
		cwsEntry := CandleWithSignal{
			Candle:     c,
			HeikinAshi: haCandle,
			Stochastic: Stochastic{
				K: Float64(stochasticK),
				D: Float64(stochasticD),
			},
			// Signal:      sig,
			Balance:     currentBalance,
			Equity:      results.Equity,
			ActiveTrade: active,
		}

		// TODO: Recheck
		// Update MAE/MFE for active trade
		if active {
			if activeSide == strategy.LongBullish {
				// Update Maximum Adverse Excursion (worst drawdown during trade)
				if c.Low-activeEntryPrice < currentTrade.MAE {
					currentTrade.MAE = c.Low - activeEntryPrice
				}

				// Update Maximum Favorable Excursion (highest profit during trade)
				if c.High-activeEntryPrice > currentTrade.MFE {
					currentTrade.MFE = c.High - activeEntryPrice
				}
			} else if activeSide == strategy.ShortBearish {
				// For shorts, MAE is when price goes up (against position)
				if activeEntryPrice-c.High < currentTrade.MAE {
					currentTrade.MAE = activeEntryPrice - c.High
				}

				// For shorts, MFE is when price goes down (favorable to position)
				if activeEntryPrice-c.Low > currentTrade.MFE {
					currentTrade.MFE = activeEntryPrice - c.Low
				}
			}
		}

		// Process entry signals immediately
		if !active && !isWarmup && (sig.Position == strategy.LongBullish || (sig.Position == strategy.ShortBearish)) &&
			(maxDailyLoss == 0 || dailyPnL > -maxDailyLoss) {

			// Calculate position size based on account balance and risk
			stopLossAmount := currentBalance * riskPerTrade

			// Entry price with slippage
			var entryPrice float64
			if sig.Position == strategy.LongBullish {
				entryPrice = c.Close * (1 + slippage) // Buy at close with slippage
			} else if sig.Position == strategy.ShortBearish {
				entryPrice = c.Close * (1 - slippage) // Short at close with slippage
			}

			// Calculate max loss per unit
			var maxLossPerUnit float64
			if sig.Position == strategy.LongBullish {
				maxLossPerUnit = entryPrice - (entryPrice * (1 - stopLossPercent/100))
			} else if sig.Position == strategy.ShortBearish {
				maxLossPerUnit = (entryPrice * (1 + stopLossPercent/100)) - entryPrice
			}

			// Calculate quantity to trade
			quantity := stopLossAmount / maxLossPerUnit
			if quantity*entryPrice > currentBalance*0.95 {
				// Limit to 95% of account balance
				quantity = (currentBalance * 0.95) / entryPrice
			}

			// Apply commission
			var totalCost float64
			if sig.Position == strategy.LongBullish {
				totalCost = entryPrice * quantity * (1 + commission)
			} else if sig.Position == strategy.ShortBearish {
				totalCost = entryPrice * quantity * (1 - commission)
			}

			// Update state
			activeEntryPrice = entryPrice
			active = true
			activeSide = sig.Position
			results.Trades++

			if sig.Position == strategy.LongBullish {
				results.LongTrades++
			} else if sig.Position == strategy.ShortBearish {
				results.ShortTrades++
			}

			// Record trade entry
			currentTrade = &TradeLogEntry{
				Entry:     entryPrice,
				EntryTime: c.Timestamp,
				Side:      getSideString(sig.Position),
				MAE:       0,
				MFE:       0,
				Quantity:  quantity,
				Balance:   currentBalance,
				Cost:      totalCost,
			}

			// Update candle with signal info
			cwsEntry.Signal = sig
			cwsEntry.TradeInfo = currentTrade
			cwsEntry.TradeDetails = fmt.Sprintf("Entry: %s at %.2f, Qty: %.4f, Cost: %.2f",
				currentTrade.Side, currentTrade.Entry, currentTrade.Quantity, currentTrade.Cost)

			trailingStop = 0

		} else if active {
			// Process exit signals and stops for active positions

			price := c.Close
			var stopLossPrice, trailingStopPrice, takeProfitPrice float64
			var exitPrice float64
			var pnl float64
			var exitReason string
			var shouldExit bool

			// Calculate stop prices based on position side
			if activeSide == strategy.LongBullish {
				stopLossPrice = activeEntryPrice * (1 - stopLossPercent/100)
				takeProfitPrice = activeEntryPrice * (1 + takeProfitPercent/100)

				// Trailing stop for long positions
				if trailingStopPercent > 0 {
					// Update trailing stop - only move it up for long positions
					profit := price - activeEntryPrice
					if profit > trailingStop {
						trailingStop = profit
					}

					// Calculate trailing stop price
					trailingStopPrice = activeEntryPrice + trailingStop - (activeEntryPrice * trailingStopPercent / 100)
				}

				// Check for exit conditions for long positions
				if takeProfitPercent > 0 && c.High >= takeProfitPrice {
					// Take profit triggered - assume we exit at take profit price
					exitPrice = takeProfitPrice * (1 - slippage)
					exitPrice -= exitPrice * commission
					pnl = (exitPrice - activeEntryPrice) * currentTrade.Quantity
					exitReason = "take-profit"
					shouldExit = true
				} else if stopLossPercent > 0 && c.Low <= stopLossPrice {
					// Stop loss triggered - assume we exit at stop price
					exitPrice = stopLossPrice * (1 - slippage)
					exitPrice -= exitPrice * commission
					pnl = (exitPrice - activeEntryPrice) * currentTrade.Quantity
					exitReason = "stop-loss"
					shouldExit = true
				} else if trailingStopPercent > 0 && c.Low <= trailingStopPrice {
					// Trailing stop triggered
					exitPrice = trailingStopPrice * (1 - slippage)
					exitPrice -= exitPrice * commission
					pnl = (exitPrice - activeEntryPrice) * currentTrade.Quantity
					exitReason = "trailing-stop"
					shouldExit = true
				} else if sig.Position == strategy.LongBearish {
					// Exit long position on short signal
					exitPrice = price * (1 - slippage)
					exitPrice -= exitPrice * commission
					pnl = (exitPrice - activeEntryPrice) * currentTrade.Quantity
					exitReason = "signal"
					shouldExit = true
				}
			} else if activeSide == strategy.ShortBearish {
				stopLossPrice = activeEntryPrice * (1 + stopLossPercent/100)
				takeProfitPrice = activeEntryPrice * (1 - takeProfitPercent/100)

				// Trailing stop for short positions
				if trailingStopPercent > 0 {
					// Update trailing stop - only move it up for short positions
					profit := activeEntryPrice - price
					if profit > trailingStop {
						trailingStop = profit
					}

					// Calculate trailing stop price
					trailingStopPrice = activeEntryPrice - trailingStop + (activeEntryPrice * trailingStopPercent / 100)
				}

				// Check for exit conditions for short positions
				if takeProfitPercent > 0 && c.Low <= takeProfitPrice {
					// Take profit triggered - assume we exit at take profit price
					exitPrice = takeProfitPrice * (1 + slippage)
					exitPrice += exitPrice * commission
					pnl = (activeEntryPrice - exitPrice) * currentTrade.Quantity
					exitReason = "take-profit"
					shouldExit = true
				} else if stopLossPercent > 0 && c.High >= stopLossPrice {
					// Stop loss triggered
					exitPrice = stopLossPrice * (1 + slippage)
					exitPrice += exitPrice * commission
					pnl = (activeEntryPrice - exitPrice) * currentTrade.Quantity
					exitReason = "stop-loss"
					shouldExit = true
				} else if trailingStopPercent > 0 && c.High >= trailingStopPrice {
					// Trailing stop triggered
					exitPrice = trailingStopPrice * (1 + slippage)
					exitPrice += exitPrice * commission
					pnl = (activeEntryPrice - exitPrice) * currentTrade.Quantity
					exitReason = "trailing-stop"
					shouldExit = true
				} else if sig.Position == strategy.ShortBullish {
					// Exit short position on long signal
					exitPrice = price * (1 + slippage)
					exitPrice += exitPrice * commission
					pnl = (activeEntryPrice - exitPrice) * currentTrade.Quantity
					exitReason = "signal"
					shouldExit = true
				}
			}

			// Process exit if needed
			if shouldExit {
				results.Equity += pnl
				currentBalance += pnl
				dailyPnL += pnl

				// Update trade log
				currentTrade.Exit = exitPrice
				currentTrade.ExitTime = c.Timestamp
				currentTrade.PnL = pnl
				currentTrade.Reason = exitReason
				results.TradeLog = append(results.TradeLog, *currentTrade)

				// Update candle with trade exit info
				cwsEntry.Signal = sig
				cwsEntry.TradeInfo = currentTrade
				cwsEntry.TradeDetails = fmt.Sprintf("Exit: %s at %.2f, PnL: %.2f, Reason: %s",
					currentTrade.Side, exitPrice, pnl, exitReason)

				// Update win/loss stats
				if pnl > 0 {
					results.Wins++
					results.WinPnls = append(results.WinPnls, pnl)
					consecutiveWins++
					consecutiveLosses = 0
				} else {
					results.Losses++
					results.LossPnls = append(results.LossPnls, pnl)
					consecutiveLosses++
					consecutiveWins = 0
				}

				// Update max consecutive wins/losses
				if consecutiveWins > results.MaxConsecWins {
					results.MaxConsecWins = consecutiveWins
				}
				if consecutiveLosses > results.MaxConsecLosses {
					results.MaxConsecLosses = consecutiveLosses
				}

				active = false
				activeSide = strategy.Hold
				trailingStop = 0
				currentTrade = nil
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

		// Add candle to chart data
		candlesWithSignals = append(candlesWithSignals, cwsEntry)
	}

	cwsEntry := candlesWithSignals[len(candlesWithSignals)-1]

	// Handle open position at the end of backtest
	if active && (cwsEntry.Signal.Position != strategy.LongBullish && cwsEntry.Signal.Position != strategy.ShortBearish) {
		// Close position at last price
		lastCandle := candles[len(candles)-1]
		exitPrice := lastCandle.Close
		var pnl float64

		if activeSide == strategy.LongBullish {
			exitPrice = exitPrice * (1 - slippage)
			exitPrice -= exitPrice * commission
			pnl = (exitPrice - activeEntryPrice) * currentTrade.Quantity
		} else if activeSide == strategy.ShortBearish { // short
			exitPrice = exitPrice * (1 + slippage)
			exitPrice += exitPrice * commission
			pnl = (activeEntryPrice - exitPrice) * currentTrade.Quantity
		}

		results.Equity += pnl
		currentBalance += pnl
		dailyPnL += pnl

		// Update trade log
		currentTrade.Exit = exitPrice
		currentTrade.ExitTime = lastCandle.Timestamp
		currentTrade.PnL = pnl
		currentTrade.Reason = "end-of-backtest"
		results.TradeLog = append(results.TradeLog, *currentTrade)

		// Update candle with trade exit info
		// cwsEntry.Signal = sig
		cwsEntry.TradeInfo = currentTrade
		cwsEntry.TradeDetails = fmt.Sprintf("Exit: %s at %.2f, PnL: %.2f, Reason: %s",
			currentTrade.Side, exitPrice, pnl, "end-of-backtest")

		// Update win/loss stats
		if pnl > 0 {
			results.Wins++
			results.WinPnls = append(results.WinPnls, pnl)
			consecutiveWins++
			consecutiveLosses = 0
		} else {
			results.Losses++
			results.LossPnls = append(results.LossPnls, pnl)
			consecutiveLosses++
			consecutiveWins = 0
		}

		// Update max consecutive wins/losses
		if consecutiveWins > results.MaxConsecWins {
			results.MaxConsecWins = consecutiveWins
		}
		if consecutiveLosses > results.MaxConsecLosses {
			results.MaxConsecLosses = consecutiveLosses
		}

		active = false
		activeSide = strategy.Hold
		trailingStop = 0
		currentTrade = nil

		// Update final equity curve
		results.EquityCurve = append(results.EquityCurve, results.Equity)

		candlesWithSignals[len(candlesWithSignals)-1] = cwsEntry
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
		err = json.NewEncoder(f).Encode(candlesWithSignals)
		if err != nil {
			log.Printf("Error encoding chart data for %s: %v", chartDataFile, err)
		}
		f.Close()
		log.Printf("Saved chart data to %s", chartDataFile)
	} else {
		log.Printf("Failed to save chart data: %v", err)
	}

	// Chart data is saved as JSON for external HTML chart

	return results
}

// calculatePerformanceMetrics calculates performance metrics for backtest results
func calculatePerformanceMetrics(results *BacktestResults) {
	// Calculate win rate
	if results.Trades > 0 {
		results.Metrics["win_rate"] = float64(results.Wins) / float64(results.Trades)
		results.Metrics["long_ratio"] = float64(results.LongTrades) / float64(results.Trades)
		results.Metrics["short_ratio"] = float64(results.ShortTrades) / float64(results.Trades)
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

	// Calculate return metrics
	if results.StartingBalance > 0 {
		totalReturn := results.Equity - results.StartingBalance
		percentReturn := (totalReturn / results.StartingBalance) * 100
		results.Metrics["total_return"] = totalReturn
		results.Metrics["percent_return"] = percentReturn
	}

	// Calculate max consecutive metrics
	results.Metrics["max_consecutive_wins"] = float64(results.MaxConsecWins)
	results.Metrics["max_consecutive_losses"] = float64(results.MaxConsecLosses)

	// Calculate average MAE and MFE
	totalMAE, totalMFE := 0.0, 0.0
	for _, trade := range results.TradeLog {
		totalMAE += trade.MAE
		totalMFE += trade.MFE
	}

	if len(results.TradeLog) > 0 {
		results.Metrics["avg_mae"] = totalMAE / float64(len(results.TradeLog))
		results.Metrics["avg_mfe"] = totalMFE / float64(len(results.TradeLog))
	}

	// Calculate exit reason statistics
	signalExits, stopLossExits, trailingStopExits := 0, 0, 0
	for _, trade := range results.TradeLog {
		switch trade.Reason {
		case "signal":
			signalExits++
		case "stop-loss":
			stopLossExits++
		case "trailing-stop":
			trailingStopExits++
		}
	}

	if results.Trades > 0 {
		results.Metrics["signal_exit_ratio"] = float64(signalExits) / float64(results.Trades)
		results.Metrics["stop_loss_exit_ratio"] = float64(stopLossExits) / float64(results.Trades)
		results.Metrics["trailing_stop_exit_ratio"] = float64(trailingStopExits) / float64(results.Trades)
	}
}

// printBacktestResults prints the results of a backtest
func printBacktestResults(strat strategy.Strategy, results BacktestResults) {
	log.Printf("Backtest Results (%s):\n", strat.Name())
	log.Printf("  Trades=%d (Long=%d, Short=%d), Wins=%d, Losses=%d, WinRate=%.2f%%\n",
		results.Trades, results.LongTrades, results.ShortTrades, results.Wins, results.Losses, results.Metrics["win_rate"]*100)
	log.Printf("  Starting Balance=%.2f, Final Equity=%.2f, Return=%.2f%%\n",
		results.StartingBalance, results.Equity, results.Metrics["percent_return"])
	log.Printf("  MaxDrawdown=%.2f, MaxConsecWins=%d, MaxConsecLosses=%d\n",
		results.MaxDrawdown, results.MaxConsecWins, results.MaxConsecLosses)
	log.Printf("  AvgWin=%.2f, AvgLoss=%.2f, ProfitFactor=%.2f\n",
		results.Metrics["avg_win"], results.Metrics["avg_loss"], results.Metrics["profit_factor"])
	log.Printf("  Sharpe=%.2f, Expectancy=%.2f\n",
		results.Metrics["sharpe"], results.Metrics["expectancy"])
	log.Printf("  Exit Types: Signal=%.1f%%, StopLoss=%.1f%%, TrailingStop=%.1f%%\n",
		results.Metrics["signal_exit_ratio"]*100,
		results.Metrics["stop_loss_exit_ratio"]*100,
		results.Metrics["trailing_stop_exit_ratio"]*100)
	log.Printf("  Avg MAE=%.2f, Avg MFE=%.2f\n",
		results.Metrics["avg_mae"], results.Metrics["avg_mfe"])

	// Print strategy-specific metrics
	if len(results.Metrics) > 0 {
		log.Println("  Strategy Metrics:")
		for k, v := range results.Metrics {
			// Skip metrics we've already printed
			if k != "win_rate" && k != "avg_win" && k != "avg_loss" &&
				k != "profit_factor" && k != "sharpe" && k != "expectancy" &&
				k != "long_ratio" && k != "short_ratio" && k != "total_return" &&
				k != "percent_return" && k != "max_consecutive_wins" &&
				k != "max_consecutive_losses" && k != "avg_mae" && k != "avg_mfe" &&
				k != "signal_exit_ratio" && k != "stop_loss_exit_ratio" &&
				k != "trailing_stop_exit_ratio" {
				log.Printf("    %s: %.4f\n", k, v)
			}
		}
	}

	// Print trade log summary
	log.Println("Trade Log Summary (Last 10 trades):")
	maxTrades := 10 // Limit the number of trades to print
	for i, t := range results.TradeLog {
		if i >= maxTrades {
			log.Printf("  ... and %d more trades\n", len(results.TradeLog)-maxTrades)
			break
		}
		log.Printf("  Trade %d: %s Entry=%.2f at %s, Exit=%.2f at %s, PnL=%.2f, Reason=%s\n",
			i+1, t.Side, t.Entry, t.EntryTime.Format(time.RFC3339),
			t.Exit, t.ExitTime.Format(time.RFC3339), t.PnL, t.Reason)
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
