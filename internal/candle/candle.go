// Package candle
package candle

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

type Candle struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	Symbol    string
	Timeframe string
	Source    string
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

// IsComplete checks if a candle is complete (not the current minute)
func (c *Candle) IsComplete() bool {
	now := time.Now()
	candleEnd := c.Timestamp.Add(GetTimeframeDuration(c.Timeframe))
	return now.After(candleEnd)
}

// GetAggregationPath returns the path of timeframes needed to aggregate from source to target
func GetAggregationPath(sourceTf, targetTf string) ([]string, error) {
	if !IsValidTimeframe(sourceTf) || !IsValidTimeframe(targetTf) {
		return nil, fmt.Errorf("invalid timeframe: source=%s, target=%s", sourceTf, targetTf)
	}

	sourceDur := GetTimeframeDuration(sourceTf)
	targetDur := GetTimeframeDuration(targetTf)

	if sourceDur >= targetDur {
		return nil, fmt.Errorf("source timeframe must be smaller than target timeframe")
	}

	// Pre-sort timeframes by duration for more efficient lookup
	timeframes := GetSupportedTimeframes()
	sort.Slice(timeframes, func(i, j int) bool {
		return GetTimeframeDuration(timeframes[i]) < GetTimeframeDuration(timeframes[j])
	})

	// Find the path of timeframes needed
	var path []string
	current := sourceTf
	for GetTimeframeDuration(current) < targetDur {
		// Find the next larger timeframe
		found := false
		for _, timeframe := range timeframes {
			tfDuration := GetTimeframeDuration(timeframe)
			if tfDuration > GetTimeframeDuration(current) && tfDuration <= targetDur {
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

// Storage interface for saving and retrieving candles.
type Storage interface {
	SaveCandle(ctx context.Context, candle *Candle) error
	SaveCandles(ctx context.Context, candles []Candle) error
	SaveConstructedCandles(ctx context.Context, candles []Candle) error
	GetCandle(ctx context.Context, symbol, timeframe string, timestamp time.Time, source string) (*Candle, error)
	GetCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error)
	GetCandlesV2(ctx context.Context, timeframe string, start, end time.Time) ([]Candle, error)
	GetCandlesInRange(ctx context.Context, symbol, timeframe string, start, end time.Time, source string) ([]Candle, error)
	GetConstructedCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error)
	GetRawCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error)
	GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
	GetLatestCandleInRange(ctx context.Context, symbol, timeframe string, start, end time.Time) (*Candle, error)
	GetLatestConstructedCandle(ctx context.Context, symbol, timeframe string) (*Candle, error)
	GetLatest1mCandle(ctx context.Context, symbol string) (*Candle, error)
	DeleteCandles(ctx context.Context, symbol, timeframe string, before time.Time) error
	DeleteCandlesInRange(ctx context.Context, symbol, timeframe string, start, end time.Time, source string) error
	DeleteConstructedCandles(ctx context.Context, symbol, timeframe string, before time.Time) error
	GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error)
	GetConstructedCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error)
	UpdateCandle(ctx context.Context, candle Candle) error
	UpdateCandles(ctx context.Context, candle []Candle) error
	GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error)
	GetMissingCandleRanges(ctx context.Context, symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error)
	GetCandleSourceStats(ctx context.Context, symbol string, start, end time.Time) (map[string]any, error)
}

// Aggregator interface for candle aggregation
type Aggregator interface {
	Aggregate(candles []Candle, timeframe string) ([]Candle, error)
	AggregateIncremental(newCandle Candle, existingCandles []Candle, timeframe string) ([]Candle, error)
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

	Subscribe() <-chan []Candle
	Unsubscribe(ch chan []Candle)
}

type DefaultAggregator struct {
	mu      sync.RWMutex
	storage Storage
}

// NewAggregator creates a new aggregator
func NewAggregator(storage Storage) Aggregator {
	return &DefaultAggregator{
		storage: storage,
	}
}

// Aggregate aggregates candles to a higher timeframe
func (a *DefaultAggregator) Aggregate(candles []Candle, timeframe string) ([]Candle, error) {
	if len(candles) == 0 {
		return nil, nil
	}

	dur, err := ParseTimeframe(timeframe)
	if err != nil {
		return nil, fmt.Errorf("invalid timeframe %s: %w", timeframe, err)
	}

	// Sort candles by timestamp to ensure proper aggregation
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp.Before(candles[j].Timestamp)
	})

	buckets := make(map[time.Time][]Candle)
	for i, c := range candles {
		if err := c.Validate(); err != nil {
			return nil, fmt.Errorf("invalid candle at index %d: %w", i, err)
		}

		bucket := c.Timestamp.Truncate(dur)
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
	// Get 1m candles for the time range
	oneMCandles, err := a.storage.GetCandles(ctx, symbol, "1m", start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(oneMCandles) == 0 {
		return nil, nil
	}

	// Aggregate to target timeframe
	return a.AggregateFrom1m(oneMCandles, targetTimeframe)
}

// AggregateIncremental efficiently aggregates a new candle with existing candles
// ISSUE: Review this code before use.
func (a *DefaultAggregator) AggregateIncremental(newCandle Candle, existingCandles []Candle, timeframe string) ([]Candle, error) {
	if err := newCandle.Validate(); err != nil {
		return nil, fmt.Errorf("invalid new candle: %w", err)
	}

	dur, err := ParseTimeframe(timeframe)
	if err != nil {
		return nil, fmt.Errorf("invalid timeframe %s: %w", timeframe, err)
	}

	newBucket := newCandle.Timestamp.Truncate(dur)

	// Find if we already have a candle for this bucket
	bucketFound := false

	// More efficient to modify in place when possible
	for i := range existingCandles {
		existingBucket := existingCandles[i].Timestamp.Truncate(dur)

		if existingBucket.Equal(newBucket) {
			// Update existing candle in place
			existingCandles[i].High = max(existingCandles[i].High, newCandle.High)
			existingCandles[i].Low = min(existingCandles[i].Low, newCandle.Low)
			existingCandles[i].Close = newCandle.Close
			existingCandles[i].Volume += newCandle.Volume
			existingCandles[i].Source = "constructed"

			if err := existingCandles[i].Validate(); err != nil {
				return nil, fmt.Errorf("invalid updated candle: %w", err)
			}

			bucketFound = true
			break
		}
	}

	if !bucketFound {
		// Create new aggregated candle
		agg := Candle{
			Timestamp: newBucket,
			Open:      newCandle.Open,
			High:      newCandle.High,
			Low:       newCandle.Low,
			Close:     newCandle.Close,
			Volume:    newCandle.Volume,
			Symbol:    newCandle.Symbol,
			Timeframe: timeframe,
			Source:    "constructed",
		}
		existingCandles = append(existingCandles, agg)

		// Sort by timestamp only when adding a new candle
		sort.Slice(existingCandles, func(i, j int) bool {
			return existingCandles[i].Timestamp.Before(existingCandles[j].Timestamp)
		})
	}

	return existingCandles, nil
}

// DefaultIngester handles real-time candle ingestion and aggregation
type DefaultIngester struct {
	storage     Storage
	aggregator  Aggregator
	cache       map[string]map[string]*Candle // symbol -> timeframe -> latest candle
	subscribers []chan []Candle               // TODO: Lifetime management
	mu          sync.RWMutex
}

// NewCandleIngester creates a new ingester with configuration
func NewCandleIngester(storage Storage) Ingester {
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

	dur := GetTimeframeDuration(c.Timeframe)
	if dur == 0 {
		return nil
	}
	c.Timestamp = c.Timestamp.Truncate(dur)

	ci.mu.Lock()
	defer ci.mu.Unlock()

	// Save the original candle
	if err := ci.storage.SaveCandles(ctx, []Candle{c}); err != nil {
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
				fmt.Println("Warning: subscriber channel is full, skipping")
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
			// Log error but continue with valid candles
			fmt.Printf("Skipping invalid candle: %v\n", err)
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
	if err := ci.storage.SaveCandles(ctx, oneMCandles); err != nil {
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
		default:
			fmt.Println("Warning: subscriber channel is full, skipping")
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
	allOneMins, err := ci.storage.GetCandles(ctx, symbol, "1m", startTime, now)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(allOneMins) == 0 {
		return nil
	}

	// Aggregate to all higher timeframes
	higherTimeframes := GetAggregationTimeframes()
	// Prepare batch updates
	var newCandles []Candle

	for _, timeframe := range higherTimeframes {
		dur := GetTimeframeDuration(timeframe)
		if dur == 0 {
			continue
		}

		// Aggregate candles for this bucket
		aggregated, err := ci.aggregator.AggregateFrom1m(allOneMins, timeframe)
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
		if err := ci.storage.SaveConstructedCandles(ctx, newCandles); err != nil {
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
	oneMCandles, err := ci.storage.GetCandles(ctx, symbol, "1m", start, end)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(oneMCandles) == 0 {
		return nil
	}

	// Aggregate to all higher timeframes
	higherTimeframes := GetAggregationTimeframes()

	// Use a map to collect all constructed candles for batch saving
	allConstructedCandles := make([]Candle, 0)

	for _, timeframe := range higherTimeframes {
		// Skip invalid timeframes
		if GetTimeframeDuration(timeframe) == 0 {
			continue
		}

		// Aggregate candles for this timeframe
		aggregated, err := ci.aggregator.AggregateFrom1m(oneMCandles, timeframe)
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
		log.Printf("Aggregated %d %s constructed candles for %s\n", len(aggregated), timeframe, symbol)
	}

	// Save all constructed candles in one batch operation
	if len(allConstructedCandles) > 0 {
		if err := ci.storage.SaveConstructedCandles(ctx, allConstructedCandles); err != nil {
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
		symbolCandles[c.Symbol] = append(symbolCandles[c.Symbol], c)
	}

	// Process each symbol's candles in parallel using goroutines
	var wg sync.WaitGroup
	errorCh := make(chan error, len(symbolCandles))

	// Higher timeframes to aggregate to
	higherTimeframes := GetAggregationTimeframes()

	// Process each symbol
	for symbol, candles := range symbolCandles {
		wg.Add(1)

		go func(symbol string, candles []Candle) {
			defer wg.Done()

			// Aggregate to all higher timeframes
			allConstructedCandles := make([]Candle, 0, len(candles)) // Pre-allocate with estimated capacity

			for _, timeframe := range higherTimeframes {
				// Skip invalid timeframes
				if GetTimeframeDuration(timeframe) == 0 {
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
				log.Printf("Aggregated %d %s constructed candles for %s\n", len(aggregated), timeframe, symbol)
			}

			// Save all constructed candles for this symbol in one batch operation
			if len(allConstructedCandles) > 0 {
				if err := ci.storage.SaveConstructedCandles(ctx, allConstructedCandles); err != nil {
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
		ci.cache[symbol][timeframe] = c
	}

	return c, nil
}

// CleanupOldData removes old candles to prevent database bloat
func (ci *DefaultIngester) CleanupOldData(ctx context.Context, symbol, timeframe string, retentionDays int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	return ci.storage.DeleteCandles(ctx, symbol, timeframe, cutoff)
}

func (ci *DefaultIngester) Subscribe() <-chan []Candle {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	ch := make(chan []Candle, 10) // buffered channel
	ci.subscribers = append(ci.subscribers, ch)
	return ch
}

func (ci *DefaultIngester) Unsubscribe(ch chan []Candle) {
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

// ParseTimeframe parses timeframe string (e.g., "5m", "1h") to time.Duration
func ParseTimeframe(timeframe string) (time.Duration, error) {
	switch timeframe {
	case "1m":
		return time.Minute, nil
	case "5m":
		return 5 * time.Minute, nil
	case "15m":
		return 15 * time.Minute, nil
	case "30m":
		return 30 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "4h":
		return 4 * time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		return 0, errors.New("unsupported timeframe")
	}
}

// GetTimeframeDuration returns the duration for a given timeframe
func GetTimeframeDuration(timeframe string) time.Duration {
	switch timeframe {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "4h":
		return 4 * time.Hour
	case "1d":
		return 24 * time.Hour
	default:
		return 0
	}
}

func TimeframeMinutes(timeframe string) int {
	switch timeframe {
	case "1m":
		return 1
	case "5m":
		return 5
	case "15m":
		return 15
	case "30m":
		return 30
	case "1h":
		return 60
	case "4h":
		return 4 * 60
	case "1d":
		return 24 * 4 * 60
	default:
		return 0
	}
}

// GetSupportedTimeframes returns all supported timeframes
func GetSupportedTimeframes() []string {
	return []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
}

func GetAggregationTimeframes() []string {
	return []string{"5m", "15m", "30m", "1h", "4h", "1d"}
}

// IsValidTimeframe checks if a timeframe is supported
func IsValidTimeframe(timeframe string) bool {
	return GetTimeframeDuration(timeframe) > 0
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

// IsConstructed returns true if the candle was constructed (aggregated)
func (c *Candle) IsConstructed() bool {
	return c.Source == "constructed"
}

// IsSynthesized returns true if the candle was synthesized
func (c *Candle) IsSynthesized() bool {
	return c.Source == "synthetic"
}

// IsRaw returns true if the candle is raw (from exchange)
func (c *Candle) IsRaw() bool {
	return c.Source != "constructed"
}

// GetSourceType returns the type of candle source
func (c *Candle) GetSourceType() string {
	if c.IsConstructed() {
		return "constructed"
	}
	if c.IsSynthesized() {
		return "synthetic"
	}
	return "raw"
}

// SetConstructed marks the candle as constructed
func (c *Candle) SetConstructed() {
	c.Source = "constructed"
}

// SetRaw marks the candle as raw with the given source
func (c *Candle) SetRaw(source string) {
	if source == "" {
		source = "unknown"
	}
	c.Source = source
}
