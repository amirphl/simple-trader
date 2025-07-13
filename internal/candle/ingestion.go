package candle

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Exchange interface for fetching candles from different exchanges
type Exchange interface {
	Name() string
	FetchCandles(ctx context.Context, symbol string, timeframe string, start, end int64) ([]Candle, error)
}

type IngestionService interface {
	Start() error
	Stop()
	GetIngestionStats() map[string]any
	Subscribe() <-chan []Candle
	UnSubscribe(ch <-chan []Candle)
}

// DefaultIngestionService handles real-time candle ingestion with automatic aggregation
type DefaultIngestionService struct {
	ingester   Ingester
	storage    Storage
	aggregator Aggregator
	exchanges  map[string]Exchange
	config     IngestionConfig
	mu         sync.RWMutex
	ctx        context.Context
	wg         sync.WaitGroup
}

// IngestionConfig holds configuration for the ingestion service
type IngestionConfig struct {
	Symbols         []string
	FetchCycle      time.Duration
	RetentionDays   int
	MaxRetries      int
	RetryDelay      time.Duration
	DelayUpperbound time.Duration
	EnableCleanup   bool
	CleanupCycle    time.Duration
}

// DefaultIngestionConfig returns a default configuration
func DefaultIngestionConfig() IngestionConfig {
	return IngestionConfig{
		Symbols:         []string{"BTCUSDT", "ETHUSDT"},
		FetchCycle:      30 * time.Second,
		RetentionDays:   30,
		MaxRetries:      3,
		RetryDelay:      3 * time.Second,
		DelayUpperbound: 20 * time.Second,
		EnableCleanup:   false,
		CleanupCycle:    24 * time.Hour, // TODO:Increase value.
	}
}

// NewIngestionService creates a new candle ingestion service
func NewIngestionService(ctx context.Context, storage Storage, aggregator Aggregator, exchanges map[string]Exchange, config IngestionConfig) IngestionService {
	ingester := NewCandleIngester(storage)

	return &DefaultIngestionService{
		ingester:   ingester,
		storage:    storage,
		aggregator: aggregator,
		exchanges:  exchanges,
		config:     config,
		ctx:        ctx,
	}
}

// Start begins the ingestion service with proper error handling and synchronization
func (is *DefaultIngestionService) Start() error {
	log.Printf("Starting candle ingestion service with %d symbols", len(is.config.Symbols))

	if len(is.exchanges) == 0 {
		return fmt.Errorf("no exchanges configured for ingestion service")
	}

	if len(is.config.Symbols) == 0 {
		return fmt.Errorf("no symbols configured for ingestion service")
	}

	// Track active goroutines with WaitGroup
	is.wg.Add(len(is.config.Symbols))

	// Start ingestion loops for each symbol
	for _, symbol := range is.config.Symbols {
		go func(sym string) {
			defer is.wg.Done()
			is.runIngestionLoop(sym)
		}(symbol)
	}

	// Start cleanup routine if enabled
	if is.config.EnableCleanup {
		is.wg.Add(1)

		go func() {
			defer is.wg.Done()
			is.runCleanupLoop()
		}()
	}

	log.Printf("Ingestion service started successfully")
	return nil
}

// Stop gracefully stops the ingestion service
func (is *DefaultIngestionService) Stop() {
	log.Printf("Stopping ingestion service...")

	// Create a timeout context for graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a channel to signal completion
	done := make(chan struct{})

	go func() {
		is.wg.Wait()
		close(done)
	}()

	// TODO: Close Subscriber channels?

	// Wait for either completion or timeout
	select {
	case <-done:
		log.Printf("Ingestion service stopped gracefully")
	case <-ctx.Done():
		log.Printf("Ingestion service stop timed out")
	}
}

// runIngestionLoop runs the main ingestion loop for a symbol with improved error handling
func (is *DefaultIngestionService) runIngestionLoop(symbol string) {
	ticker := time.NewTicker(is.config.FetchCycle)
	defer ticker.Stop()

	const timeframe = "1m"
	log.Printf("[%s %s] Starting ingestion loop", symbol, timeframe)

	// Track consecutive errors for backoff
	// consecutiveErrors := 0
	// maxConsecutiveErrors := 5
	// baseBackoff := time.Second * 5

	for {
		select {
		case <-is.ctx.Done():
			log.Printf("[%s %s] Stopping ingestion loop", symbol, timeframe)
			return
		case <-ticker.C:
			if err := is.fetchAndIngestCandles(symbol); err != nil {
				log.Printf("[%s %s] Error in ingestion loop: %v", symbol, timeframe, err)
			}

			// if err != nil {
			// 	consecutiveErrors++
			// 	log.Printf("[%s %s] Error in ingestion loop (%d/%d consecutive): %v",
			// 		symbol, timeframe, consecutiveErrors, maxConsecutiveErrors, err)
			//
			// 	// Apply exponential backoff for consecutive errors
			// 	if consecutiveErrors >= maxConsecutiveErrors {
			// 		backoff := time.Duration(math.Min(
			// 			float64(baseBackoff)*math.Pow(2, float64(consecutiveErrors-maxConsecutiveErrors)),
			// 			float64(2*time.Minute),
			// 		))
			// 		log.Printf("[%s %s] Too many consecutive errors, backing off for %v",
			// 			symbol, timeframe, backoff)
			//
			// 		// Create a temporary timer for backoff
			// 		backoffTimer := time.NewTimer(backoff)
			// 		select {
			// 		case <-backoffTimer.C:
			// 			// Continue after backoff
			// 		case <-is.ctx.Done():
			// 			if !backoffTimer.Stop() {
			// 				<-backoffTimer.C
			// 			}
			// 			return
			// 		}
			// 	}
			// } else {
			// 	// Reset consecutive errors counter on success
			// 	if consecutiveErrors > 0 {
			// 		log.Printf("[%s %s] Ingestion recovered after %d errors",
			// 			symbol, timeframe, consecutiveErrors)
			// 		consecutiveErrors = 0
			// 	}
			// }
		}
	}
}

func deriveContextWithTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	// Get the parent's deadline, if any
	deadline, hasDeadline := parent.Deadline()
	if !hasDeadline {
		return context.WithTimeout(context.Background(), 30*time.Second)
	}

	// Create a new context with the same deadline
	childCtx, cancel := context.WithDeadline(context.Background(), deadline)
	return childCtx, cancel
}

// fetchAndIngestCandles fetches candles from exchange and ingests them
func (is *DefaultIngestionService) fetchAndIngestCandles(symbol string) error {
	const timeframe = "1m"

	// TODO: Add Tx to ctx.
	// TODO: Rollback on err
	ctx, cancel := deriveContextWithTimeout(is.ctx)
	defer cancel()

	// Get the latest candle to determine the fetch range
	latest, err := is.ingester.GetLatestCandle(ctx, symbol, timeframe)
	if err != nil {
		return fmt.Errorf("failed to get latest candle: %w", err)
	}

	var start time.Time
	now := time.Now().UTC() // TODO: UTC
	end := now.Truncate(time.Minute)

	if latest == nil {
		// No previous candles, fetch last 7 year
		start = end.AddDate(-7, 0, 0).Truncate(24 * time.Hour)
	} else {
		// NOTE: Try to receive duplicated candles to avoid missing candles
		start = latest.Timestamp.Add(-10 * time.Minute) // TODO: UTC?
	}

	// If there's no new data to fetch, return early
	if start.After(end) || start.Equal(end) {
		return nil
	}

	var wg sync.WaitGroup

	// Fetch candles from all exchanges in parallel
	for exchangeName, exchange := range is.exchanges {
		wg.Add(1)
		go func(name string, ex Exchange) {
			defer wg.Done()

			// Use Unix timestamps for consistency
			candles, err := is.fetchCandlesWithRetry(ctx, ex, symbol, timeframe, start.Unix(), end.Unix())
			if err != nil {
				log.Printf("[%s %s] Failed to fetch candles from %s: %v", symbol, timeframe, name, err)
				// NOTE: No need to generate fake candle
				return
			}

			candlesMap := make(map[time.Time]Candle)

			for _, c := range candles {
				// Truncate timestamp to remove seconds
				dur := GetTimeframeDuration(timeframe)
				c.Timestamp = c.Timestamp.Truncate(dur)
				c.Source = name

				// Validate all candles before ingestion
				// TODO: sorted, no missing, same timeframe, same symbol, all are truncated
				if err := c.Validate(); err != nil {
					// TODO: Log
					return
				}

				candlesMap[c.Timestamp] = c
			}

			// Generate synthetic candles for missing minutes
			var completeCandles []Candle

			if latest == nil {
				latest = &candles[0] // NOTE: No problem loosing the first 1m candle
			}

			// Start from the minute after the latest candle
			currentTime := latest.Timestamp.Add(time.Minute)
			// Use the latest candle's close price as the base for synthetic candles
			basePrice := latest.Close

			for !currentTime.After(candles[len(candles)-1].Timestamp) {
				c, ok := candlesMap[currentTime]
				if !ok {
					// Create a synthetic candle for this missing minute
					syntheticCandle := Candle{
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
					log.Printf("[%s %s] Generated synthetic candle for %v", symbol, timeframe, currentTime)
				} else {
					completeCandles = append(completeCandles, c)
					// Update the base price for future synthetic candles
					basePrice = c.Close
				}

				currentTime = currentTime.Add(time.Minute)
			}

			// Sort the complete candles by timestamp
			// sort.Slice(completeCandles, func(i, j int) bool {
			// 	return completeCandles[i].Timestamp.Before(completeCandles[j].Timestamp)
			// })

			// Ingest the complete set of candles (real + synthetic)
			if len(completeCandles) > 0 {
				if err := is.ingester.IngestRaw1mCandles(ctx, completeCandles); err != nil {
					// TODO: Log
					return
				}
				log.Printf("[%s %s] Successfully ingested %d candles (%d real, %d synthetic)",
					symbol, timeframe, len(completeCandles), len(candles), len(completeCandles)-len(candles))
			}
			log.Printf("[%s %s] Fetched %d candles from %s", symbol, timeframe, len(candles), name)
		}(exchangeName, exchange)
	}

	wg.Wait()

	// ISSUE: The code should account for multiple exchanges, but as only one exchange is currently used, it's acceptable for now.

	return nil
}

// fetchCandlesWithRetry fetches candles with exponential backoff retry logic
func (is *DefaultIngestionService) fetchCandlesWithRetry(ctx context.Context, exchange Exchange, symbol, timeframe string, start, end int64) ([]Candle, error) {
	var candles []Candle
	var err error

	baseDelay := is.config.RetryDelay
	maxDelay := is.config.DelayUpperbound

	for attempt := 1; attempt <= is.config.MaxRetries; attempt++ {
		select {
		case <-is.ctx.Done():
			log.Printf("Timeout Fetching from exchange %s", exchange.Name())
			return nil, ctx.Err()
		default:

			candles, err = exchange.FetchCandles(ctx, symbol, timeframe, start, end)
			if err == nil {
				return candles, nil
			}

			// Don't sleep after the last attempt
			if attempt < is.config.MaxRetries {
				// Calculate exponential backoff with jitter
				// Formula: baseDelay * 2^(attempt-1) + small random jitter
				backoff := float64(baseDelay) * math.Pow(2, float64(attempt-1))

				// Add jitter (±20% randomness)
				jitter := rand.Float64()*0.4 - 0.2 // -20% to +20%
				backoffWithJitter := time.Duration(float64(backoff) * (1 + jitter))

				// Cap the maximum delay
				if backoffWithJitter > maxDelay {
					backoffWithJitter = maxDelay
				}

				log.Printf("[%s %s] Fetch attempt %d failed, retrying in %v: %v",
					symbol, timeframe, attempt, backoffWithJitter, err)
				time.Sleep(backoffWithJitter)
			}
		}
	}

	return nil, fmt.Errorf("failed to fetch candles after %d attempts: %w", is.config.MaxRetries, err)
}

// runCleanupLoop runs the cleanup routine to remove old data
func (is *DefaultIngestionService) runCleanupLoop() {
	ticker := time.NewTicker(is.config.CleanupCycle)
	defer ticker.Stop()

	log.Printf("Starting cleanup loop with %d day retention", is.config.RetentionDays)

	// Run cleanup immediately on start instead of waiting for first tick
	if err := is.cleanupOldData(); err != nil {
		log.Printf("Initial cleanup failed: %v", err)
	}

	for {
		select {
		case <-is.ctx.Done():
			log.Printf("Stopping cleanup loop")
			return
		case <-ticker.C:
			startTime := time.Now()
			log.Printf("Starting scheduled data cleanup...")

			if err := is.cleanupOldData(); err != nil {
				log.Printf("Error in cleanup loop: %v", err)
			} else {
				duration := time.Since(startTime)
				log.Printf("Cleanup completed successfully in %v", duration)
			}
		}
	}
}

// cleanupOldData removes old candles to prevent database bloat
func (is *DefaultIngestionService) cleanupOldData() error {
	return nil

	// TODO:

	if is.config.RetentionDays <= 0 {
		log.Printf("Skipping cleanup: RetentionDays is set to %d", is.config.RetentionDays)
		return nil
	}

	// Get all timeframes that need cleanup
	timeframes := GetSupportedTimeframes()

	// Track errors but continue with other symbols/timeframes
	var errors []error
	successCount := 0

	// Use a semaphore to limit concurrent database operations
	sem := make(chan struct{}, 5) // Max 5 concurrent cleanup operations
	var wg sync.WaitGroup

	// Mutex for thread-safe error collection
	var mu sync.Mutex

	for _, symbol := range is.config.Symbols {
		for _, timeframe := range timeframes {
			wg.Add(1)

			// Capture loop variables
			sym, tf := symbol, timeframe

			go func() {
				defer wg.Done()

				// Acquire semaphore
				sem <- struct{}{}
				defer func() { <-sem }()

				// Perform cleanup with timeout
				ctx, cancel := context.WithTimeout(is.ctx, 30*time.Second)
				defer cancel()
				// TODO: Add Tx to ctx.
				// TODO: Rollback on err

				// Create a done channel for the operation
				done := make(chan error, 1)

				go func() {
					done <- is.ingester.CleanupOldData(ctx, sym, tf, is.config.RetentionDays)
				}()

				// Wait for either completion or timeout
				select {
				case err := <-done:
					if err != nil {
						mu.Lock()
						errors = append(errors, fmt.Errorf("[%s %s] cleanup failed: %w", sym, tf, err))
						mu.Unlock()
						log.Printf("[%s %s] Failed to cleanup old data: %v", sym, tf, err)
					} else {
						mu.Lock()
						successCount++
						mu.Unlock()
						log.Printf("[%s %s] Successfully cleaned up old data", sym, tf)
					}
				case <-ctx.Done():
					mu.Lock()
					errors = append(errors, fmt.Errorf("[%s %s] cleanup timed out", sym, tf))
					mu.Unlock()
					log.Printf("[%s %s] Cleanup operation timed out", sym, tf)
				}
			}()
		}
	}

	// Wait for all cleanup operations to complete
	wg.Wait()

	// Report results
	if len(errors) > 0 {
		log.Printf("Cleanup completed with %d successes and %d failures", successCount, len(errors))
		return fmt.Errorf("cleanup encountered %d errors: %v", len(errors), errors[0])
	}

	log.Printf("Cleanup completed successfully for %d symbol-timeframe combinations", successCount)
	return nil
}

// aggregateHistoricalData aggregates historical data for all configured timeframes
// NOTE: Just updates db, not cache
func (is *DefaultIngestionService) aggregateHistoricalData(symbol string) error {
	log.Printf("[%s] Starting historical data aggregation", symbol)

	// Get the base timeframe (usually 1m)
	const baseTimeframe = "1m"

	// If base timeframe is 1m, use optimized bulk aggregation
	if baseTimeframe == "1m" {
		return is.bulkAggregateHistorical1mData(symbol)
	}

	// NOTE: Unused logic

	end := time.Now().UTC() // TODO: UTC?
	// Get historical data for the last 7 years
	start := end.AddDate(-7, 0, 0).Truncate(24 * time.Hour)

	ctx, cancel := deriveContextWithTimeout(is.ctx)
	defer cancel()
	// TODO: Add Tx to ctx.
	// TODO: Rollback on err

	sourceCandles, err := is.storage.GetCandles(ctx, symbol, baseTimeframe, start, end)
	if err != nil {
		log.Printf("[%s] Failed to get source candles for %s: %v", symbol, baseTimeframe, err)
		return err
	}

	if len(sourceCandles) == 0 {
		log.Printf("[%s] No source candles found for %s", symbol, baseTimeframe)
		return nil
	}

	// For other base timeframes, use the original method
	for _, targetTimeframe := range GetAggregationTimeframes() {
		log.Printf("[%s] Aggregating %s to %s", symbol, baseTimeframe, targetTimeframe)

		// Aggregate
		aggregated, err := is.aggregator.Aggregate(sourceCandles, targetTimeframe)
		if err != nil {
			log.Printf("[%s] Failed to aggregate to %s: %v", symbol, targetTimeframe, err)
			return err
		}

		if len(aggregated) > 0 {
			// Save constructed candles
			if err := is.storage.SaveConstructedCandles(ctx, aggregated); err != nil {
				log.Printf("[%s] Failed to save %s constructed candles: %v", symbol, targetTimeframe, err)
				return err
			}

			log.Printf("[%s] Successfully aggregated %d constructed candles to %s", symbol, len(aggregated), targetTimeframe)
		}
	}

	return nil
}

// bulkAggregateHistorical1mData efficiently aggregates historical 1m candles to all higher timeframes
// NOTE: updates both db and cache
// ISSUE: Recheck logic.
func (is *DefaultIngestionService) bulkAggregateHistorical1mData(symbol string) error {
	log.Printf("[%s] Starting bulk aggregation of historical 1m data", symbol)

	ctx, cancel := deriveContextWithTimeout(is.ctx)
	defer cancel()
	// TODO: Add Tx to ctx.
	// TODO: Rollback on err

	// Get the latest 1m candle to determine the time range
	latest1m, err := is.storage.GetLatest1mCandle(ctx, symbol)
	if err != nil {
		return fmt.Errorf("failed to get latest 1m candle: %w", err)
	}

	if latest1m == nil {
		log.Printf("[%s] No 1m candles found for aggregation", symbol)
		return nil
	}

	// Use end time as the latest candle timestamp
	end := latest1m.Timestamp
	// Calculate start time
	start := end.AddDate(-7, 0, 0).Truncate(24 * time.Hour)

	log.Printf("[%s] Aggregating 1m data from %s to %s (%d years)",
		symbol, start.Format(time.RFC3339), end.Format(time.RFC3339), 1)

	// Process in smaller chunks to avoid memory issues with large datasets
	// Use a sliding window approach
	chunkSize := 24 * time.Hour
	currentStart := start

	// Track overall progress
	var totalProcessed int
	var totalErrors []error

	for currentStart.Before(end) {
		// Calculate chunk end, but don't go beyond the overall end
		chunkEnd := currentStart.Add(chunkSize)
		if chunkEnd.After(end) {
			chunkEnd = end
		}

		log.Printf("[%s] Processing chunk from %s to %s",
			symbol, currentStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))

		// Create a channel for the operation result
		resultCh := make(chan error, 1)

		go func(start, end time.Time) {
			// ISSUE: Shared TX
			resultCh <- is.ingester.BulkAggregateFrom1m(ctx, symbol, start, end)
		}(currentStart, chunkEnd)

		// Wait for either completion or timeout
		var chunkErr error
		select {
		case err := <-resultCh:
			chunkErr = err
		case <-ctx.Done():
			chunkErr = fmt.Errorf("aggregation timed out for chunk %s to %s",
				currentStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339))
		}
		// TODO: Listen on is.ctx.Done()

		if chunkErr != nil {
			log.Printf("[%s] Error aggregating chunk from %s to %s: %v",
				symbol, currentStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339), chunkErr)
			totalErrors = append(totalErrors, chunkErr)
		} else {
			// Count processed candles for this chunk
			count, _ := is.storage.GetCandleCount(ctx, symbol, "1m", currentStart, chunkEnd)
			totalProcessed += count
			log.Printf("[%s] Successfully aggregated chunk with %d candles", symbol, count)
		}

		// Move to next chunk
		currentStart = chunkEnd
	}

	// Report overall results
	if len(totalErrors) > 0 {
		log.Printf("[%s] Completed bulk aggregation with %d candles processed and %d errors",
			symbol, totalProcessed, len(totalErrors))
		return fmt.Errorf("bulk aggregation completed with %d errors: %v",
			len(totalErrors), totalErrors[0])
	}

	log.Printf("[%s] Successfully completed bulk aggregation of %d candles",
		symbol, totalProcessed)
	return nil
}

// GetIngestionStats returns statistics about the ingestion service
func (is *DefaultIngestionService) GetIngestionStats() map[string]any {
	stats := make(map[string]any)

	ctx, cancel := deriveContextWithTimeout(is.ctx)
	defer cancel()

	for _, symbol := range is.config.Symbols {
		stats[symbol] = make(map[string]any)

		for _, timeframe := range GetSupportedTimeframes() {
			latest, err := is.ingester.GetLatestCandle(ctx, symbol, timeframe)
			if err != nil {
				stats[symbol].(map[string]any)[timeframe] = map[string]any{
					"error": err.Error(),
				}
				continue
			}

			if latest == nil {
				stats[symbol].(map[string]any)[timeframe] = map[string]any{
					"latest_candle": nil,
					"candle_count":  0,
				}
				continue
			}

			// Get candle count for last 24 hours
			end := time.Now().UTC()
			start := end.Add(-24 * time.Hour)
			count, _ := is.storage.GetCandleCount(ctx, symbol, timeframe, start, end)

			stats[symbol].(map[string]any)[timeframe] = map[string]any{
				"latest_candle":    latest.Timestamp,
				"latest_price":     latest.Close,
				"candle_count_24h": count,
				"is_complete":      latest.IsComplete(),
			}

			// Get aggregation statistics
			aggStats, err := is.storage.GetAggregationStats(ctx, symbol)
			if err == nil {
				stats[symbol].(map[string]any)["aggregation_stats"] = aggStats
			}
		}
	}

	return stats
}

func (is *DefaultIngestionService) Subscribe() <-chan []Candle {
	return is.ingester.Subscribe()
}

func (is *DefaultIngestionService) UnSubscribe(ch <-chan []Candle) {
	is.ingester.Unsubscribe(ch)
}
