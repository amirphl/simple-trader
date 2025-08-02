// Package candle
package candle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/tfutils"
	"github.com/amirphl/simple-trader/internal/utils"
)

type Candle struct {
	Timestamp time.Time `json:"timestamp"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
	Symbol    string    `json:"symbol"`
	Timeframe string    `json:"timeframe"`
	Source    string    `json:"source"`
}

// IsComplete checks if a candle is complete (not the current minute)
func (c *Candle) IsComplete() bool {
	now := time.Now().UTC()
	candleEnd := c.Timestamp.Add(tfutils.GetTimeframeDuration(c.Timeframe))
	return now.After(candleEnd)
}

// Validate checks if a candle has valid data
func (c *Candle) Validate() error {
	if c.Timestamp.IsZero() {
		return errors.New("candle timestamp is zero")
	}
	if c.Open <= 0 || c.High <= 0 || c.Low <= 0 || c.Close <= 0 {
		return errors.New("candle prices must be positive")
	}
	if c.High < c.Low {
		return errors.New("candle high cannot be less than low")
	}
	if c.Open < c.Low || c.Open > c.High {
		return errors.New("candle open price must be between high and low")
	}
	if c.Close < c.Low || c.Close > c.High {
		return errors.New("candle close price must be between high and low")
	}
	if c.Volume < 0 {
		return errors.New("candle volume cannot be negative")
	}
	if c.Symbol == "" {
		return errors.New("candle symbol cannot be empty")
	}
	if c.Timeframe == "" {
		return errors.New("candle timeframe cannot be empty")
	}
	return nil
}

// GetAggregationPath returns the path of timeframes needed to aggregate from source to target
func GetAggregationPath(sourceTf, targetTf string) ([]string, error) {
	if !tfutils.IsValidTimeframe(sourceTf) || !tfutils.IsValidTimeframe(targetTf) {
		return nil, fmt.Errorf("invalid timeframe: source=%s, target=%s", sourceTf, targetTf)
	}

	sourceDur := tfutils.GetTimeframeDuration(sourceTf)
	targetDur := tfutils.GetTimeframeDuration(targetTf)

	if sourceDur >= targetDur {
		return nil, fmt.Errorf("source timeframe must be smaller than target timeframe")
	}

	// Pre-sort timeframes by duration for more efficient lookup
	timeframes := tfutils.GetSupportedTimeframes()
	sort.Slice(timeframes, func(i, j int) bool {
		return tfutils.GetTimeframeDuration(timeframes[i]) < tfutils.GetTimeframeDuration(timeframes[j])
	})

	// Find the path of timeframes needed
	var path []string
	current := sourceTf
	for tfutils.GetTimeframeDuration(current) < targetDur {
		// Find the next larger timeframe
		found := false
		for _, timeframe := range timeframes {
			tfDuration := tfutils.GetTimeframeDuration(timeframe)
			if tfDuration > tfutils.GetTimeframeDuration(current) && tfDuration <= targetDur {
				path = append(path, timeframe)
				current = timeframe
				found = true
				break
			}
		}
		if !found {
			break
		}
	}

	return path, nil
}

// Aggregator interface for candle aggregation
type Aggregator interface {
	Aggregate(candles []Candle, timeframe string) ([]Candle, error)
	AggregateFrom1m(oneMCandles []Candle, targetTimeframe string) ([]Candle, error)
	Aggregate1mTimeRange(ctx context.Context, symbol string, start, end time.Time, targetTimeframe string) ([]Candle, error)
}

type Ingester interface {
	IngestCandle(ctx context.Context, c Candle) error
	IngestRaw1mCandles(ctx context.Context, candles []Candle) error
	AggregateSymbolToHigherTimeframes(ctx context.Context, symbol string) error
	BulkAggregateFrom1m(ctx context.Context, symbol string, start, end time.Time) error
	BulkAggregateAllSymbolsFrom1m(ctx context.Context, start, end time.Time) error
	GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
	CleanupOldData(ctx context.Context, symbol, timeframe string, retentionDays int) error

	// Proxy
	GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error)
	GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error)

	Subscribe() <-chan []Candle
	Unsubscribe(ch <-chan []Candle)
}

type DefaultAggregator struct {
	storage db.Storage
}

// NewAggregator creates a new aggregator
func NewAggregator(storage db.Storage) Aggregator {
	return &DefaultAggregator{
		storage: storage,
	}
}

// Aggregate aggregates candles to a higher timeframe
func (a *DefaultAggregator) Aggregate(candles []Candle, timeframe string) ([]Candle, error) {
	if len(candles) == 0 {
		return nil, nil
	}

	dur, err := tfutils.ParseTimeframe(timeframe)
	if err != nil {
		return nil, fmt.Errorf("invalid timeframe %s: %w", timeframe, err)
	}

	// Sort candles by timestamp to ensure proper aggregation
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp.Before(candles[j].Timestamp)
	})

	firstCandleSymbol := candles[0].Symbol
	firstCandleTimeframe := candles[0].Timeframe
	firstCandleDur, err := tfutils.ParseTimeframe(firstCandleTimeframe)
	if err != nil {
		return nil, fmt.Errorf("invalid timeframe %s: %w", firstCandleTimeframe, err)
	}
	firstCandleTimestamp := candles[0].Timestamp.Truncate(firstCandleDur)

	buckets := make(map[time.Time][]Candle)
	for i, c := range candles {
		if err := c.Validate(); err != nil {
			return nil, fmt.Errorf("invalid candle at index %d: %w", i, err)
		}
		if c.Symbol != firstCandleSymbol {
			return nil, fmt.Errorf("candle at index %d has different symbol: %s, expected: %s", i, c.Symbol, firstCandleSymbol)
		}
		if c.Timeframe != firstCandleTimeframe {
			return nil, fmt.Errorf("candle at index %d has different timeframe: %s, expected: %s", i, c.Timeframe, firstCandleTimeframe)
		}
		// check for missing candles
		if c.Timestamp.Truncate(firstCandleDur).Sub(firstCandleTimestamp) != time.Duration(i)*firstCandleDur {
			return nil, fmt.Errorf("candle at index %d has different timestamp: %s, expected: %s", i, c.Timestamp, firstCandleTimestamp.Add(time.Duration(i)*firstCandleDur))
		}

		bucket := c.Timestamp.Truncate(dur).Add(dur)
		buckets[bucket] = append(buckets[bucket], c)
	}

	var result []Candle
	for bucket, group := range buckets {
		if len(group) == 0 {
			continue
		}

		agg := Candle{
			Timestamp: bucket,
			Open:      group[0].Open,
			High:      group[0].High,
			Low:       group[0].Low,
			Close:     group[len(group)-1].Close,
			Volume:    0,
			Symbol:    group[0].Symbol,
			Timeframe: timeframe,
			Source:    "constructed",
		}

		for _, c := range group {
			if c.High > agg.High {
				agg.High = c.High
			}
			if c.Low < agg.Low {
				agg.Low = c.Low
			}
			agg.Volume += c.Volume
		}

		// Validate aggregated candle
		if err := agg.Validate(); err != nil {
			return nil, fmt.Errorf("invalid aggregated candle for bucket %v: %w", bucket, err)
		}

		result = append(result, agg)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})

	return result, nil
}

// AggregateFrom1m efficiently aggregates 1m candles to higher timeframes
func (a *DefaultAggregator) AggregateFrom1m(oneMCandles []Candle, targetTimeframe string) ([]Candle, error) {
	// Validate that all candles are 1m
	for i, c := range oneMCandles {
		if c.Timeframe != "1m" {
			return nil, fmt.Errorf("candle at index %d is not 1m: %s", i, c.Timeframe)
		}
	}

	return a.Aggregate(oneMCandles, targetTimeframe)
}

// Aggregate1mTimeRange aggregates 1m candles from storage for a specific time range
func (a *DefaultAggregator) Aggregate1mTimeRange(ctx context.Context, symbol string, start, end time.Time, targetTimeframe string) ([]Candle, error) {
	// Check if storage is nil
	if a.storage == nil {
		return nil, fmt.Errorf("storage is nil, aggregator not properly initialized")
	}

	// Get 1m candles for the time range
	oneMCandles, err := a.storage.GetCandles(ctx, symbol, "1m", "", start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(oneMCandles) == 0 {
		return nil, nil
	}

	// Aggregate to target timeframe
	return a.AggregateFrom1m(DBCandlesToCandles(oneMCandles), targetTimeframe)
}

// DefaultIngester handles real-time candle ingestion and aggregation
type DefaultIngester struct {
	storage     db.Storage
	aggregator  Aggregator
	cache       map[string]map[string]*Candle // symbol -> timeframe -> latest candle
	subscribers []chan []Candle               // TODO: Lifetime management
	mu          sync.RWMutex
}

// NewCandleIngester creates a new ingester with configuration
func NewCandleIngester(storage db.Storage) Ingester {
	return &DefaultIngester{
		storage:    storage,
		aggregator: NewAggregator(storage),
		cache:      make(map[string]map[string]*Candle),
	}
}

// IngestCandle processes a new candle and triggers aggregation for higher timeframes
func (ci *DefaultIngester) IngestCandle(ctx context.Context, c Candle) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid candle: %w", err)
	}

	dur := tfutils.GetTimeframeDuration(c.Timeframe)
	if dur == 0 {
		return nil
	}
	c.Timestamp = c.Timestamp.Truncate(dur)

	ci.mu.Lock()
	defer ci.mu.Unlock()

	// Save the original candle
	if err := ci.storage.SaveCandles(ctx, CandlesToDbCandles([]Candle{c})); err != nil {
		return fmt.Errorf("failed to save candle: %w", err)
	}

	// If this is a base timeframe (1m), aggregate to higher timeframes
	if c.Timeframe == "1m" {
		return ci.AggregateSymbolToHigherTimeframes(ctx, c.Symbol)
	}

	// ISSUE: This is not Transactional.
	if ci.cache[c.Symbol] == nil {
		ci.cache[c.Symbol] = make(map[string]*Candle)
	}
	ci.cache[c.Symbol][c.Timeframe] = &c

	if c.Timeframe == "1m" {
		// ISSUE: This is not Transactional.
		for _, ch := range ci.subscribers {
			// Non-blocking send to avoid blocking on slow receivers
			select {
			case ch <- []Candle{c}:
			default:
				utils.GetLogger().Println("Warning: subscriber channel is full, skipping")
			}
		}
	}

	return nil
}

// IngestRaw1mCandles efficiently ingests raw 1m candles and triggers aggregation
func (ci *DefaultIngester) IngestRaw1mCandles(ctx context.Context, candles []Candle) error {
	if len(candles) == 0 {
		return nil
	}

	ci.mu.Lock()
	defer ci.mu.Unlock()

	// Filter to only valid 1m candles in one pass
	var oneMCandles []Candle
	symbols := make(map[string]bool)
	latestBySymbol := make(map[string]*Candle)

	for i := range candles {
		c := &candles[i]
		if c.Timeframe != "1m" {
			continue
		}

		// Validate candle once here instead of multiple times later
		if err := c.Validate(); err != nil {
			utils.GetLogger().Printf("Ingester | Skipping invalid candle: %v\n", err)
			continue
		}

		c.Timestamp = c.Timestamp.Truncate(time.Minute)

		oneMCandles = append(oneMCandles, *c)
		symbols[c.Symbol] = true

		// Track the latest candle for each symbol
		if latest, exists := latestBySymbol[c.Symbol]; !exists || c.Timestamp.After(latest.Timestamp) {
			latestBySymbol[c.Symbol] = c
		}
	}

	if len(oneMCandles) == 0 {
		return nil
	}

	// Save raw 1m candles in one batch operation
	if err := ci.storage.SaveCandles(ctx, CandlesToDbCandles(oneMCandles)); err != nil {
		return fmt.Errorf("failed to save raw 1m candles: %w", err)
	}

	for symbol := range symbols {
		if err := ci.AggregateSymbolToHigherTimeframes(ctx, symbol); err != nil {
			return fmt.Errorf("failed to aggregate symbol %s: %w", symbol, err)
		}
	}

	// ISSUE: This is not Transactional.
	for symbol, latestCandle := range latestBySymbol {
		if ci.cache[symbol] == nil {
			ci.cache[symbol] = make(map[string]*Candle)
		}
		ci.cache[symbol]["1m"] = latestCandle
	}

	// ISSUE: This is not Transactional.
	for _, ch := range ci.subscribers {
		// Non-blocking send to avoid blocking on slow receivers
		select {
		case ch <- oneMCandles:
			utils.GetLogger().Printf("Ingester | Sent %d 1m candles to subscriber", len(oneMCandles))
		default:
			utils.GetLogger().Println("Ingester | Warning: subscriber channel is full, skipping")
		}
	}

	return nil
}

// AggregateSymbolToHigherTimeframes aggregates all 1m candles for a symbol to higher timeframes
func (ci *DefaultIngester) AggregateSymbolToHigherTimeframes(ctx context.Context, symbol string) error {
	latest5m, err := ci.storage.GetLatestCandle(ctx, symbol, "5m")
	if err != nil {
		return fmt.Errorf("failed to get latest 5m candle: %w", err)
	}

	now := time.Now().UTC()
	var startTime time.Time

	if latest5m != nil {
		startTime = latest5m.Timestamp.Truncate(24 * time.Hour)
	} else {
		// 7 years ago
		startTime = now.AddDate(-7, 0, 0).Truncate(24 * time.Hour)
	}

	// Get all 1m candles for the time range
	allOneMins, err := ci.storage.GetCandles(ctx, symbol, "1m", "", startTime, now)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(allOneMins) == 0 {
		return nil
	}

	// Aggregate to all higher timeframes
	higherTimeframes := tfutils.GetAggregationTimeframes()
	// Prepare batch updates
	var newCandles []Candle

	for _, timeframe := range higherTimeframes {
		dur := tfutils.GetTimeframeDuration(timeframe)
		if dur == 0 {
			continue
		}

		// Aggregate candles for this bucket
		aggregated, err := ci.aggregator.AggregateFrom1m(DBCandlesToCandles(allOneMins), timeframe)
		if err != nil {
			return fmt.Errorf("failed to aggregate to %s for bucket %v: %w", timeframe, timeframe, err)
		}

		if len(aggregated) == 0 {
			continue
		}

		newCandles = append(newCandles, aggregated...)

		if ci.cache[symbol] == nil {
			ci.cache[symbol] = make(map[string]*Candle)
		}
		latestAggCandle := aggregated[len(aggregated)-1]
		ci.cache[symbol][timeframe] = &latestAggCandle
	}

	// NOTE: Very important: In case of conflict, it updates the candle.
	if len(newCandles) > 0 {
		if err := ci.storage.SaveConstructedCandles(ctx, CandlesToDbCandles(newCandles)); err != nil {
			return fmt.Errorf("failed to save constructed candles: %w", err)
		}
	}

	return nil
}

// BulkAggregateFrom1m performs bulk aggregation of 1m candles to all higher timeframes
// NOTE: It Truncates start to 00:00
// NOTE: It Truncates end   to 00:00
func (ci *DefaultIngester) BulkAggregateFrom1m(ctx context.Context, symbol string, start, end time.Time) error {
	start = start.Truncate(24 * time.Hour)
	end = end.Truncate(24 * time.Hour)

	ci.mu.Lock()
	defer ci.mu.Unlock()

	// Get all 1m candles for the time range in one database query
	oneMCandles, err := ci.storage.GetCandles(ctx, symbol, "1m", "", start, end)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(oneMCandles) == 0 {
		return nil
	}

	// Aggregate to all higher timeframes
	higherTimeframes := tfutils.GetAggregationTimeframes()

	// Use a map to collect all constructed candles for batch saving
	allConstructedCandles := make([]Candle, 0)

	for _, timeframe := range higherTimeframes {
		// Skip invalid timeframes
		if tfutils.GetTimeframeDuration(timeframe) == 0 {
			continue
		}

		// Aggregate candles for this timeframe
		aggregated, err := ci.aggregator.AggregateFrom1m(DBCandlesToCandles(oneMCandles), timeframe)
		if err != nil {
			return fmt.Errorf("failed to aggregate to %s: %w", timeframe, err)
		}

		if len(aggregated) == 0 {
			continue
		}

		// Add to batch for saving later
		allConstructedCandles = append(allConstructedCandles, aggregated...)

		// Update cache with latest candle
		if ci.cache[symbol] == nil {
			ci.cache[symbol] = make(map[string]*Candle)
		}
		latestCandle := aggregated[len(aggregated)-1]
		ci.cache[symbol][timeframe] = &latestCandle

		// Log the aggregation result
		utils.GetLogger().Printf("Aggregated %d %s constructed candles for %s\n", len(aggregated), timeframe, symbol)
	}

	// Save all constructed candles in one batch operation
	if len(allConstructedCandles) > 0 {
		if err := ci.storage.SaveConstructedCandles(ctx, CandlesToDbCandles(allConstructedCandles)); err != nil {
			return fmt.Errorf("failed to save constructed candles: %w", err)
		}
	}

	return nil
}

// BulkAggregateAllSymbolsFrom1m performs bulk aggregation for all symbols in a given time range
// using the optimized GetCandlesV2 function which doesn't filter by symbol
// NOTE: It Truncates start to 00:00
// NOTE: It Truncates end   to 00:00
func (ci *DefaultIngester) BulkAggregateAllSymbolsFrom1m(ctx context.Context, start, end time.Time) error {
	start = start.Truncate(24 * time.Hour)
	end = end.Truncate(24 * time.Hour)

	// NOTE: Don't lock here.
	// ci.mu.Lock()
	// defer ci.mu.Unlock()

	// Get all 1m candles across all symbols in the time range
	allCandles, err := ci.storage.GetCandlesV2(ctx, "1m", start, end)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(allCandles) == 0 {
		return nil
	}

	// Group candles by symbol
	symbolCandles := make(map[string][]Candle)
	for _, c := range allCandles {
		symbolCandles[c.Symbol] = append(symbolCandles[c.Symbol], DBCandleToCandle(c))
	}

	// Process each symbol's candles in parallel using goroutines
	var wg sync.WaitGroup
	errorCh := make(chan error, len(symbolCandles))

	// Higher timeframes to aggregate to
	higherTimeframes := tfutils.GetAggregationTimeframes()

	// Process each symbol
	for symbol, candles := range symbolCandles {
		wg.Add(1)

		go func(symbol string, candles []Candle) {
			defer wg.Done()

			// Aggregate to all higher timeframes
			allConstructedCandles := make([]Candle, 0, len(candles)) // Pre-allocate with estimated capacity

			for _, timeframe := range higherTimeframes {
				// Skip invalid timeframes
				if tfutils.GetTimeframeDuration(timeframe) == 0 {
					continue
				}

				// Aggregate candles for this timeframe
				aggregated, err := ci.aggregator.AggregateFrom1m(candles, timeframe)
				if err != nil {
					errorCh <- fmt.Errorf("failed to aggregate %s to %s: %w", symbol, timeframe, err)
					return
				}

				if len(aggregated) == 0 {
					continue
				}

				// Add to batch for saving later
				allConstructedCandles = append(allConstructedCandles, aggregated...)

				// Update cache with latest candle
				ci.mu.Lock()
				if ci.cache[symbol] == nil {
					ci.cache[symbol] = make(map[string]*Candle)
				}
				latestCandle := aggregated[len(aggregated)-1]
				ci.cache[symbol][timeframe] = &latestCandle
				ci.mu.Unlock()

				// Log the aggregation result
				utils.GetLogger().Printf("Ingester | Aggregated %d %s constructed candles for %s\n", len(aggregated), timeframe, symbol)
			}

			// Save all constructed candles for this symbol in one batch operation
			if len(allConstructedCandles) > 0 {
				if err := ci.storage.SaveConstructedCandles(ctx, CandlesToDbCandles(allConstructedCandles)); err != nil {
					errorCh <- fmt.Errorf("failed to save constructed candles for %s: %w", symbol, err)
					return
				}
			}
		}(symbol, candles)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorCh)

	// Collect all errors
	var processingErrors []error
	for err := range errorCh {
		processingErrors = append(processingErrors, err)
	}

	// Report any errors that occurred during processing
	if len(processingErrors) > 0 {
		errMsg := fmt.Sprintf("failed to aggregate some symbols (%d/%d failed):", len(processingErrors), len(symbolCandles))
		for _, err := range processingErrors {
			errMsg += "\n" + err.Error()
		}
		return errors.New(errMsg)
	}

	return nil
}

// GetLatestCandle returns the latest candle for a symbol and timeframe
func (ci *DefaultIngester) GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error) {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	// Check cache first
	if ci.cache[symbol] != nil {
		if cached, exists := ci.cache[symbol][timeframe]; exists {
			return cached, nil
		}
	}

	// Fall back to storage
	c, err := ci.storage.GetLatestCandle(ctx, symbol, timeframe)
	if err != nil {
		return nil, err
	}

	if c != nil {
		cc := DBCandleToCandle(*c)
		if ci.cache[symbol] == nil {
			ci.cache[symbol] = make(map[string]*Candle)
		}

		ci.cache[symbol][timeframe] = &cc
		return &cc, nil
	}

	return nil, nil
}

// CleanupOldData removes old candles to prevent database bloat
func (ci *DefaultIngester) CleanupOldData(ctx context.Context, symbol, timeframe string, retentionDays int) error {
	cutoff := time.Now().UTC().AddDate(0, 0, -retentionDays)
	return ci.storage.DeleteCandles(ctx, symbol, timeframe, cutoff)
}

func (ci *DefaultIngester) Subscribe() <-chan []Candle {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	ch := make(chan []Candle, 10) // buffered channel
	ci.subscribers = append(ci.subscribers, ch)
	return ch
}

func (ci *DefaultIngester) Unsubscribe(ch <-chan []Candle) {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	for i, sub := range ci.subscribers {
		if sub == ch {
			close(sub)
			ci.subscribers = append(ci.subscribers[:i], ci.subscribers[i+1:]...)
			break
		}
	}
}

// GetCandleCount returns the number of candles for a symbol and timeframe in a given time range
func (ci *DefaultIngester) GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	return ci.storage.GetCandleCount(ctx, symbol, timeframe, start, end)
}

// GetAggregationStats returns the aggregation stats for a symbol in a given time range
func (ci *DefaultIngester) GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error) {
	return ci.storage.GetAggregationStats(ctx, symbol)
}

// Helper functions
func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
