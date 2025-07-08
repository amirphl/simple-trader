// Package candle
package candle

import (
	"errors"
	"fmt"
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

// Aggregator interface for candle aggregation
type Aggregator interface {
	Aggregate(candles []Candle, timeframe string) ([]Candle, error)
	AggregateIncremental(newCandle Candle, existingCandles []Candle, timeframe string) ([]Candle, error)
	AggregateFrom1m(oneMCandles []Candle, targetTimeframe string) ([]Candle, error)
	Aggregate1mTimeRange(symbol string, start, end time.Time, targetTimeframe string, storage Storage) ([]Candle, error)
}

// Storage interface for saving and retrieving candles.
type Storage interface {
	SaveCandle(candle *Candle) error
	SaveCandles(candles []Candle) error
	SaveConstructedCandles(candles []Candle) error
	GetCandle(symbol, timeframe string, timestamp time.Time, source string) (*Candle, error)
	GetCandles(symbol, timeframe string, start, end time.Time) ([]Candle, error)
	GetCandlesV2(timeframe string, start, end time.Time) ([]Candle, error)
	GetCandlesInRange(symbol, timeframe string, start, end time.Time, source string) ([]Candle, error)
	GetConstructedCandles(symbol, timeframe string, start, end time.Time) ([]Candle, error)
	GetRawCandles(symbol, timeframe string, start, end time.Time) ([]Candle, error)
	GetLatestCandle(symbol, timeframe string) (*Candle, error)
	GetLatestCandleInRange(symbol, timeframe string, start, end time.Time) (*Candle, error)
	GetLatestConstructedCandle(symbol, timeframe string) (*Candle, error)
	GetLatest1mCandle(symbol string) (*Candle, error)
	DeleteCandles(symbol, timeframe string, before time.Time) error
	DeleteCandlesInRange(symbol, timeframe string, start, end time.Time, source string) error
	DeleteConstructedCandles(symbol, timeframe string, before time.Time) error
	GetCandleCount(symbol, timeframe string, start, end time.Time) (int, error)
	GetConstructedCandleCount(symbol, timeframe string, start, end time.Time) (int, error)
	UpdateCandle(candle Candle) error
	UpdateCandles(candle []Candle) error
	GetAggregationStats(symbol string) (map[string]any, error)
	GetMissingCandleRanges(symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error)
	GetCandleSourceStats(symbol string, start, end time.Time) (map[string]any, error)
}

type Ingester interface {
	IngestCandle(c Candle) error
	IngestRaw1mCandles(candles []Candle) error
	BulkAggregateFrom1m(symbol string, start, end time.Time) error
	BulkAggregateAllSymbolsFrom1m(start, end time.Time) error
	GetLatestCandleFromCache(symbol, timeframe string) (*Candle, error)
	GetCandlesFromDB(symbol, timeframe string, start, end time.Time) ([]Candle, error)
	CleanupOldData(symbol, timeframe string, retentionDays int) error

	// NOTE: Don't use.
	AggregateToHigherTimeframes(c Candle) error
}

type DefaultAggregator struct {
	mu sync.RWMutex
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
func (a *DefaultAggregator) Aggregate1mTimeRange(symbol string, start, end time.Time, targetTimeframe string, storage Storage) ([]Candle, error) {
	// Get 1m candles for the time range
	oneMCandles, err := storage.GetCandles(symbol, "1m", start, end)
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
// WARN: Take care.
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
	storage    Storage
	aggregator Aggregator
	mu         sync.RWMutex
	cache      map[string]map[string]*Candle // symbol -> timeframe -> latest candle
}

// NewCandleIngester creates a new ingester with configuration
func NewCandleIngester(storage Storage, aggregator Aggregator) Ingester {
	return &DefaultIngester{
		storage:    storage,
		aggregator: aggregator,
		cache:      make(map[string]map[string]*Candle),
	}
}

// TODO: Transactional

// IngestCandle processes a new candle and triggers aggregation for higher timeframes
func (ci *DefaultIngester) IngestCandle(c Candle) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid candle: %w", err)
	}

	ci.mu.Lock()
	defer ci.mu.Unlock()

	// Save the original candle
	if err := ci.storage.SaveCandles([]Candle{c}); err != nil {
		return fmt.Errorf("failed to save candle: %w", err)
	}

	// Update cache
	if ci.cache[c.Symbol] == nil {
		ci.cache[c.Symbol] = make(map[string]*Candle)
	}
	ci.cache[c.Symbol][c.Timeframe] = &c

	// If this is a base timeframe (1m), aggregate to higher timeframes
	if c.Timeframe == "1m" {
		return ci.AggregateToHigherTimeframes(c)
	}

	return nil
}

// IngestRaw1mCandles efficiently ingests raw 1m candles and triggers aggregation
func (ci *DefaultIngester) IngestRaw1mCandles(candles []Candle) error {
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
	if err := ci.storage.SaveCandles(oneMCandles); err != nil {
		return fmt.Errorf("failed to save raw 1m candles: %w", err)
	}

	// Update cache with only the latest candle per symbol
	for symbol, latestCandle := range latestBySymbol {
		if ci.cache[symbol] == nil {
			ci.cache[symbol] = make(map[string]*Candle)
		}
		ci.cache[symbol]["1m"] = latestCandle
	}

	// Trigger aggregation for each symbol
	var aggregationErrors []error
	for symbol := range symbols {
		if err := ci.aggregateSymbolToHigherTimeframesV2(symbol); err != nil {
			// Collect errors but continue processing other symbols
			aggregationErrors = append(aggregationErrors, fmt.Errorf("failed to aggregate symbol %s: %w", symbol, err))
		}
	}

	// Report aggregation errors if any occurred
	if len(aggregationErrors) > 0 {
		errMsg := "aggregation errors occurred:"
		for _, err := range aggregationErrors {
			errMsg += "\n" + err.Error()
		}
		return errors.New(errMsg)
	}

	return nil
}

func (ci *DefaultIngester) AggregateToHigherTimeframes(c Candle) error {
	// Get all supported timeframes that are larger than 1m
	higherTimeframes := GetAggregationTimeframes()

	// Pre-calculate all bucket start times
	bucketStarts := make(map[string]time.Time)
	bucketEnds := make(map[string]time.Time)
	for _, timeframe := range higherTimeframes {
		dur := GetTimeframeDuration(timeframe)
		if dur == 0 {
			continue
		}
		bucketStarts[timeframe] = c.Timestamp.Truncate(dur)
		bucketEnds[timeframe] = bucketStarts[timeframe].Add(dur)
	}

	// Get all 1m candles for the largest timeframe (which contains all smaller ones)
	// This reduces multiple database queries to just one
	largestTimeframe := "1d" // Assuming this is the largest
	start := bucketStarts[largestTimeframe]
	end := bucketEnds[largestTimeframe]
	allOneMins, err := ci.storage.GetCandles(c.Symbol, "1m", start, end)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(allOneMins) == 0 {
		return nil // No candles to aggregate
	}

	// Get all existing latest candles in one batch query if possible
	// ISSUE: Batch opt
	latestCandles := make(map[string]*Candle)
	for _, timeframe := range higherTimeframes {
		latest, err := ci.storage.GetLatestCandle(c.Symbol, timeframe)
		if err != nil {
			return fmt.Errorf("failed to get latest %s candle: %w", timeframe, err)
		}
		latestCandles[timeframe] = latest
	}

	// Prepare batch updates
	var updatedCandles []Candle
	var newCandles []Candle

	for _, timeframe := range higherTimeframes {
		dur := GetTimeframeDuration(timeframe)
		if dur == 0 {
			continue
		}

		bucketStart := bucketStarts[timeframe]
		bucketEnd := bucketEnds[timeframe]

		// Filter 1m candles for this bucket
		var oneMins []Candle

		// ISSUE: Optimize
		for _, candle := range allOneMins {
			if !candle.Timestamp.Before(bucketStart) && candle.Timestamp.Before(bucketEnd) {
				oneMins = append(oneMins, candle)
			}
		}

		if len(oneMins) == 0 {
			// ISSUE:
			continue
		}

		latestAgg := latestCandles[timeframe]

		// If we have a latest aggregated candle and it's for the same bucket, update it
		if latestAgg != nil && latestAgg.Timestamp.Equal(bucketStart) {
			// Recalculate the entire candle from 1m data for accuracy
			// ISSUE: What about applying to latestAgg itself?
			aggregated, err := ci.aggregator.AggregateFrom1m(oneMins, timeframe)
			if err != nil {
				return fmt.Errorf("failed to aggregate to %s: %w", timeframe, err)
			}

			if len(aggregated) > 0 {
				// Update existing candle with recalculated values
				updatedCandle := aggregated[0]
				updatedCandle.Timestamp = latestAgg.Timestamp // Ensure timestamp is preserved

				// Add to batch update
				updatedCandles = append(updatedCandles, updatedCandle)

				// Update cache
				if ci.cache[c.Symbol] == nil {
					ci.cache[c.Symbol] = make(map[string]*Candle)
				}
				ci.cache[c.Symbol][timeframe] = &updatedCandle
			}
		} else {
			// Create new aggregated candle
			// ISSUE: What about applying the 1m candle itself?
			aggregated, err := ci.aggregator.AggregateFrom1m(oneMins, timeframe)
			if err != nil {
				return fmt.Errorf("failed to aggregate to %s: %w", timeframe, err)
			}

			if len(aggregated) > 0 {
				// Add to batch insert
				newCandles = append(newCandles, aggregated...)

				// Update cache
				if ci.cache[c.Symbol] == nil {
					ci.cache[c.Symbol] = make(map[string]*Candle)
				}
				ci.cache[c.Symbol][timeframe] = &aggregated[len(aggregated)-1]
			}
		}
	}

	// ISSUE: Transactional batch update and insert

	// Perform batch database operations
	if len(updatedCandles) > 0 {
		if err := ci.storage.UpdateCandles(updatedCandles); err != nil {
			return fmt.Errorf("failed to update candles: %w", err)
		}
	}

	if len(newCandles) > 0 {
		if err := ci.storage.SaveConstructedCandles(newCandles); err != nil {
			return fmt.Errorf("failed to save constructed candles: %w", err)
		}
	}

	return nil
}

// aggregateSymbolToHigherTimeframes aggregates all 1m candles for a symbol to higher timeframes
func (ci *DefaultIngester) aggregateSymbolToHigherTimeframes(symbol string) error {
	// Get the latest 1m candle to determine the time range
	latest1m, err := ci.storage.GetLatest1mCandle(symbol)
	if err != nil {
		return fmt.Errorf("failed to get latest 1m candle: %w", err)
	}

	if latest1m == nil {
		return nil // No 1m candles to aggregate
	}

	// Aggregate to all higher timeframes
	higherTimeframes := GetAggregationTimeframes()

	// Pre-calculate bucket times for all timeframes
	bucketStarts := make(map[string]time.Time)
	bucketEnds := make(map[string]time.Time)

	for _, timeframe := range higherTimeframes {
		dur := GetTimeframeDuration(timeframe)
		if dur == 0 {
			continue
		}
		bucketStarts[timeframe] = latest1m.Timestamp.Truncate(dur)
		bucketEnds[timeframe] = bucketStarts[timeframe].Add(dur)
	}

	// Get 1m candles for the largest timeframe (which contains all smaller ones)
	largestTimeframe := "1d" // Assuming this is the largest
	start := bucketStarts[largestTimeframe]
	end := bucketEnds[largestTimeframe]

	allOneMins, err := ci.storage.GetCandles(symbol, "1m", start, end)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(allOneMins) == 0 {
		return nil
	}

	// Get all existing latest candles in one batch if possible
	// If the storage interface doesn't support batch operations, we'll do it one by one
	latestCandles := make(map[string]*Candle)
	for _, timeframe := range higherTimeframes {
		latest, err := ci.storage.GetLatestCandle(symbol, timeframe)
		if err != nil {
			return fmt.Errorf("failed to get latest %s candle: %w", timeframe, err)
		}
		latestCandles[timeframe] = latest
	}

	// Prepare batch updates
	var updatedCandles []Candle
	var newCandles []Candle

	for _, timeframe := range higherTimeframes {
		dur := GetTimeframeDuration(timeframe)
		if dur == 0 {
			continue
		}

		bucketStart := bucketStarts[timeframe]
		bucketEnd := bucketEnds[timeframe]

		// Filter 1m candles for this bucket
		var oneMins []Candle
		for i := range allOneMins {
			candle := allOneMins[i]
			if !candle.Timestamp.Before(bucketStart) && candle.Timestamp.Before(bucketEnd) {
				oneMins = append(oneMins, candle)
			}
		}

		if len(oneMins) == 0 {
			continue
		}

		// Aggregate to the target timeframe
		aggregated, err := ci.aggregator.AggregateFrom1m(oneMins, timeframe)
		if err != nil {
			return fmt.Errorf("failed to aggregate to %s: %w", timeframe, err)
		}

		if len(aggregated) == 0 {
			continue
		}

		latestAgg := latestCandles[timeframe]

		// Check if we need to update an existing candle or create a new one
		if latestAgg != nil && latestAgg.Timestamp.Equal(bucketStart) {
			// Update existing candle
			updatedCandle := aggregated[0]
			updatedCandle.Timestamp = latestAgg.Timestamp // Ensure timestamp is preserved
			updatedCandles = append(updatedCandles, updatedCandle)
		} else {
			// Add new candles
			newCandles = append(newCandles, aggregated...)
		}

		// Update cache with the latest aggregated candle
		if ci.cache[symbol] == nil {
			ci.cache[symbol] = make(map[string]*Candle)
		}
		ci.cache[symbol][timeframe] = &aggregated[len(aggregated)-1]
	}

	// ISSUE: Transactional batch update and insert

	// Perform batch database operations
	if len(updatedCandles) > 0 {
		if err := ci.storage.UpdateCandles(updatedCandles); err != nil {
			return fmt.Errorf("failed to update candles: %w", err)
		}
	}

	if len(newCandles) > 0 {
		if err := ci.storage.SaveConstructedCandles(newCandles); err != nil {
			return fmt.Errorf("failed to save constructed candles: %w", err)
		}
	}

	return nil
}

// aggregateSymbolToHigherTimeframesV2 aggregates all 1m candles for a symbol to higher timeframes
// ISSUE: There is still opportunities for optimization
// ISSUE: Not works for 23:55 - 00:15
func (ci *DefaultIngester) aggregateSymbolToHigherTimeframesV2(symbol string) error {
	// Get the latest 1m candle to determine the end time
	latest1m, err := ci.storage.GetLatest1mCandle(symbol)
	if err != nil {
		return fmt.Errorf("failed to get latest 1m candle: %w", err)
	}

	if latest1m == nil {
		return nil // No 1m candles to aggregate
	}

	oneDayDur := GetTimeframeDuration("1d")
	startTime := latest1m.Timestamp.Truncate(oneDayDur)

	// Get all 1m candles for the time range
	allOneMins, err := ci.storage.GetCandles(symbol, "1m", startTime, latest1m.Timestamp)
	if err != nil {
		return fmt.Errorf("failed to get 1m candles: %w", err)
	}

	if len(allOneMins) == 0 {
		return nil
	}

	// Aggregate to all higher timeframes
	higherTimeframes := GetAggregationTimeframes()

	// Prepare batch updates
	var updatedCandles []Candle
	var newCandles []Candle

	for _, timeframe := range higherTimeframes {
		dur := GetTimeframeDuration(timeframe)
		if dur == 0 {
			continue
		}

		// Round down to the nearest timeframe boundary for start time
		firstBucketStart := startTime

		idx := 0

		var latestAggCandle Candle

		// ISSUE: Single iteration is enough. Inefficient.
		// Generate all bucket start times in the range
		for bucketStart := firstBucketStart; !bucketStart.After(latest1m.Timestamp); bucketStart = bucketStart.Add(dur) {
			bucketEnd := bucketStart.Add(dur)

			// Filter 1m candles for this bucket
			var bucketCandles []Candle
			for ; idx < len(allOneMins) && allOneMins[idx].Timestamp.Before(bucketEnd); idx++ {
				bucketCandles = append(bucketCandles, allOneMins[idx])
			}

			if len(bucketCandles) == 0 {
				continue
			}

			// Aggregate candles for this bucket
			aggregated, err := ci.aggregator.AggregateFrom1m(bucketCandles, timeframe)
			if err != nil {
				return fmt.Errorf("failed to aggregate to %s for bucket %v: %w",
					timeframe, bucketStart, err)
			}

			if len(aggregated) == 0 {
				continue
			}

			// ISSUE: Even no need to query. Just insert.
			// Check if we need to update an existing candle or create a new one
			existingCandle, err := ci.storage.GetLatestCandleInRange(
				symbol, timeframe, bucketStart, bucketStart)
			if err != nil {
				return fmt.Errorf("failed to check for existing candle: %w", err)
			}

			if existingCandle != nil && existingCandle.Timestamp.Equal(bucketStart) {
				// Update existing candle
				updatedCandle := aggregated[0]
				updatedCandle.Timestamp = existingCandle.Timestamp // Ensure timestamp is preserved
				updatedCandles = append(updatedCandles, updatedCandle)
			} else {
				// Add new candle
				newCandles = append(newCandles, aggregated...)
			}

			latestAggCandle = aggregated[len(aggregated)-1]
		}

		if ci.cache[symbol] == nil {
			ci.cache[symbol] = make(map[string]*Candle)
		}
		ci.cache[symbol][timeframe] = &latestAggCandle
	}

	// ISSUE: Transactional batch update and insert

	// Perform batch database operations
	if len(updatedCandles) > 0 {
		if err := ci.storage.UpdateCandles(updatedCandles); err != nil {
			return fmt.Errorf("failed to update candles: %w", err)
		}
	}

	if len(newCandles) > 0 {
		if err := ci.storage.SaveConstructedCandles(newCandles); err != nil {
			return fmt.Errorf("failed to save constructed candles: %w", err)
		}
	}

	return nil
}

// BulkAggregateFrom1m performs bulk aggregation of 1m candles to all higher timeframes
// NOTE: start-time must be 00:00
// NOTE: end-time must be the timestamp of the last 1m candle seen
func (ci *DefaultIngester) BulkAggregateFrom1m(symbol string, start, end time.Time) error {
	// Get all 1m candles for the time range in one database query
	oneMCandles, err := ci.storage.GetCandles(symbol, "1m", start, end)
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

	// Track errors but continue processing
	var aggregationErrors []error

	for _, timeframe := range higherTimeframes {
		// Skip invalid timeframes
		if GetTimeframeDuration(timeframe) == 0 {
			continue
		}

		// Aggregate candles for this timeframe
		aggregated, err := ci.aggregator.AggregateFrom1m(oneMCandles, timeframe)
		if err != nil {
			aggregationErrors = append(aggregationErrors, fmt.Errorf("failed to aggregate to %s: %w", timeframe, err))
			continue
		}

		if len(aggregated) > 0 {
			// Add to batch for saving later
			allConstructedCandles = append(allConstructedCandles, aggregated...)

			// Update cache with latest candle
			if ci.cache[symbol] == nil {
				ci.cache[symbol] = make(map[string]*Candle)
			}
			latestCandle := aggregated[len(aggregated)-1]
			ci.cache[symbol][timeframe] = &latestCandle

			// Log the aggregation result
			fmt.Printf("Aggregated %d %s constructed candles for %s\n", len(aggregated), timeframe, symbol)
		}
	}

	// Save all constructed candles in one batch operation
	if len(allConstructedCandles) > 0 {
		if err := ci.storage.SaveConstructedCandles(allConstructedCandles); err != nil {
			return fmt.Errorf("failed to save constructed candles: %w", err)
		}
	}

	// Report any errors that occurred during aggregation
	if len(aggregationErrors) > 0 {
		errMsg := "some aggregation operations failed:"
		for _, err := range aggregationErrors {
			errMsg += "\n" + err.Error()
		}
		return errors.New(errMsg)
	}

	return nil
}

// BulkAggregateAllSymbolsFrom1m performs bulk aggregation for all symbols in a given time range
// using the optimized GetCandlesV2 function which doesn't filter by symbol
// NOTE: start-time must be 00:00
// NOTE: end-time must be the timestamp of the last 1m candle seen
func (ci *DefaultIngester) BulkAggregateAllSymbolsFrom1m(start, end time.Time) error {
	// Get all 1m candles across all symbols in the time range
	allCandles, err := ci.storage.GetCandlesV2("1m", start, end)
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

			// Track errors but continue processing
			var symbolErrors []error

			for _, timeframe := range higherTimeframes {
				// Skip invalid timeframes
				if GetTimeframeDuration(timeframe) == 0 {
					continue
				}

				// Aggregate candles for this timeframe
				aggregated, err := ci.aggregator.AggregateFrom1m(candles, timeframe)
				if err != nil {
					symbolErrors = append(symbolErrors, fmt.Errorf("failed to aggregate %s to %s: %w", symbol, timeframe, err))
					continue
				}

				if len(aggregated) > 0 {
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
					fmt.Printf("Aggregated %d %s constructed candles for %s\n", len(aggregated), timeframe, symbol)
				}
			}

			// Save all constructed candles for this symbol in one batch operation
			if len(allConstructedCandles) > 0 {
				if err := ci.storage.SaveConstructedCandles(allConstructedCandles); err != nil {
					errorCh <- fmt.Errorf("failed to save constructed candles for %s: %w", symbol, err)
					return
				}
			}

			// Report any errors that occurred during aggregation for this symbol
			if len(symbolErrors) > 0 {
				errMsg := fmt.Sprintf("aggregation errors for symbol %s:", symbol)
				for _, err := range symbolErrors {
					errMsg += "\n" + err.Error()
				}
				errorCh <- errors.New(errMsg)
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

// GetLatestCandleFromCache returns the latest candle for a symbol and timeframe
func (ci *DefaultIngester) GetLatestCandleFromCache(symbol, timeframe string) (*Candle, error) {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	// Check cache first
	if ci.cache[symbol] != nil {
		if cached, exists := ci.cache[symbol][timeframe]; exists {
			return cached, nil
		}
	}

	// Fall back to storage
	candle, err := ci.storage.GetLatestCandle(symbol, timeframe)
	if err != nil {
		return nil, err
	}

	ci.cache[symbol][timeframe] = candle

	return candle, nil
}

// GetCandlesFromDB retrieves candles from storage
func (ci *DefaultIngester) GetCandlesFromDB(symbol, timeframe string, start, end time.Time) ([]Candle, error) {
	return ci.storage.GetCandles(symbol, timeframe, start, end)
}

// CleanupOldData removes old candles to prevent database bloat
func (ci *DefaultIngester) CleanupOldData(symbol, timeframe string, retentionDays int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays)
	return ci.storage.DeleteCandles(symbol, timeframe, cutoff)
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

// IsRaw returns true if the candle is raw (from exchange)
func (c *Candle) IsRaw() bool {
	return c.Source != "constructed"
}

// GetSourceType returns the type of candle source
func (c *Candle) GetSourceType() string {
	if c.IsConstructed() {
		return "constructed"
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
