package candle

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// IngestionService handles real-time candle ingestion with automatic aggregation
type IngestionService struct {
	ingester   Ingester
	storage    Storage
	aggregator Aggregator
	exchanges  map[string]Exchange
	config     IngestionConfig
	mu         sync.RWMutex
	ctx        context.Context
	wg         sync.WaitGroup
}

// Exchange interface for fetching candles from different exchanges
type Exchange interface {
	Name() string
	FetchCandles(symbol string, timeframe string, start, end int64) ([]Candle, error)
}

// IngestionConfig holds configuration for the ingestion service
type IngestionConfig struct {
	Symbols               []string
	FetchTimeframe        time.Duration
	RetentionDays         int
	BatchSize             int
	MaxRetries            int
	RetryDelay            time.Duration
	EnableAggregation     bool
	EnableCleanup         bool
	CleanupTimeframe      time.Duration
	AggregationTimeframes []string // Timeframes to aggregate to
}

// DefaultIngestionConfig returns a default configuration
func DefaultIngestionConfig() IngestionConfig {
	return IngestionConfig{
		Symbols:               []string{"BTCUSDT", "ETHUSDT"},
		FetchTimeframe:        30 * time.Second,
		RetentionDays:         30,
		BatchSize:             100,
		MaxRetries:            3,
		RetryDelay:            1 * time.Second,
		EnableAggregation:     true,
		EnableCleanup:         false,
		CleanupTimeframe:      24 * time.Hour, // TODO:Increase value.
		AggregationTimeframes: GetAggregationTimeframes(),
	}
}

// NewIngestionService creates a new candle ingestion service
func NewIngestionService(ctx context.Context, storage Storage, aggregator Aggregator, exchanges map[string]Exchange, config IngestionConfig) *IngestionService {
	ingester := NewCandleIngester(storage, aggregator)

	return &IngestionService{
		ingester:   ingester,
		storage:    storage,
		aggregator: aggregator,
		exchanges:  exchanges,
		config:     config,
		ctx:        ctx,
	}
}

// Start begins the ingestion service with proper error handling and synchronization
func (is *IngestionService) Start() error {
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
func (is *IngestionService) Stop() {
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

	// Wait for either completion or timeout
	select {
	case <-done:
		log.Printf("Ingestion service stopped gracefully")
	case <-ctx.Done():
		log.Printf("Ingestion service stop timed out")
	}
}

// runIngestionLoop runs the main ingestion loop for a symbol with improved error handling
func (is *IngestionService) runIngestionLoop(symbol string) {
	ticker := time.NewTicker(is.config.FetchTimeframe) // TODO: What if received duplicated candle?
	defer ticker.Stop()

	timeframe := "1m"
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

// fetchAndIngestCandles fetches candles from exchange and ingests them
func (is *IngestionService) fetchAndIngestCandles(symbol string) error {
	timeframe := "1m"

	// Get the latest candle to determine the fetch range
	latest, err := is.ingester.GetLatestCandleFromCache(symbol, timeframe)
	if err != nil {
		return fmt.Errorf("failed to get latest candle: %w", err)
	}

	var start, end time.Time
	now := time.Now()

	if latest == nil {
		// No previous candles, fetch last year
		end = now.Truncate(time.Minute)
		start = end.Add(365 * 24 * time.Hour)
	} else {
		// Start from the last candle timestamp + 1 minute to avoid duplicate
		start = latest.Timestamp.Add(time.Minute) // TODO: UTC? Try to receive duplicated
		end = now.Truncate(time.Minute)

		// If there's no new data to fetch, return early
		if start.After(end) || start.Equal(end) {
			return nil
		}
	}

	// Use a channel to collect candles from all exchanges in parallel with proper capacity
	exchangeCount := len(is.exchanges)
	type exchangeResult struct {
		name    string
		candles []Candle
		err     error
	}

	resultCh := make(chan exchangeResult, exchangeCount)
	var wg sync.WaitGroup

	// Fetch candles from all exchanges in parallel
	for exchangeName, exchange := range is.exchanges {
		wg.Add(1)
		go func(name string, ex Exchange) {
			defer wg.Done()

			// Use Unix timestamps for consistency
			candles, err := is.fetchCandlesWithRetry(ex, symbol, timeframe, start.Unix(), end.Unix())

			// Check if context is canceled before sending to channel
			select {
			case <-is.ctx.Done():
				return
			case resultCh <- exchangeResult{name: name, candles: candles, err: err}:
			}
		}(exchangeName, exchange)
	}

	// Close channel when all goroutines complete
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Collect results with proper deduplication
	allCandles := make(map[time.Time]Candle)
	var (
		firstCandleTimestamp *time.Time
		lastCandleTimestamp  *time.Time
		firstCandle          *Candle
	)

	for result := range resultCh {
		if result.err != nil {
			log.Printf("[%s %s] Failed to fetch candles from %s: %v", symbol, timeframe, result.name, result.err)
			// NOTE: No need to generate fake candle
			continue
		}

		if len(result.candles) > 0 {
			for _, c := range result.candles {
				// Truncate timestamp to remove seconds
				c.Timestamp = c.Timestamp.Truncate(time.Minute)
				c.Source = result.name

				// Validate all candles before ingestion
				if err := c.Validate(); err != nil {
					continue
				}

				// Deduplicate candles by timestamp
				_, ok := allCandles[c.Timestamp]
				if !ok {
					allCandles[c.Timestamp] = c
				}

				if firstCandleTimestamp == nil || firstCandleTimestamp.After(c.Timestamp) {
					firstCandleTimestamp = &c.Timestamp
					firstCandle = &c
				}
				if lastCandleTimestamp == nil || lastCandleTimestamp.Before(c.Timestamp) {
					lastCandleTimestamp = &c.Timestamp
				}
			}

			log.Printf("[%s %s] Fetched %d candles from %s", symbol, timeframe, len(result.candles), result.name)
		}
	}

	if len(allCandles) == 0 {
		log.Printf("[%s %s] No new candles fetched", symbol, timeframe)
		return nil
	}

	// Generate synthetic candles for missing minutes
	var completeCandles []Candle

	if latest == nil {
		latest = firstCandle // NOTE: No problem loosing the first 1m candle
	}

	// Start from the minute after the latest candle
	currentTime := latest.Timestamp.Add(time.Minute)
	// Use the latest candle's close price as the base for synthetic candles
	basePrice := latest.Close

	for !currentTime.After(*lastCandleTimestamp) {
		c, ok := allCandles[currentTime]
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
		if err := is.ingester.IngestRaw1mCandles(completeCandles); err != nil {
			// NOTE: No need to retry immediately
			return fmt.Errorf("failed to ingest raw 1m candles: %w", err)
		}
		log.Printf("[%s %s] Successfully ingested %d candles (%d real, %d synthetic)",
			symbol, timeframe, len(completeCandles), len(allCandles), len(completeCandles)-len(allCandles))
	}

	// ISSUE: The code should account for multiple exchanges, but as only one exchange is currently used, it's acceptable for now.

	return nil
}

// fetchCandlesWithRetry fetches candles with exponential backoff retry logic
func (is *IngestionService) fetchCandlesWithRetry(exchange Exchange, symbol, timeframe string, start, end int64) ([]Candle, error) {
	var candles []Candle
	var err error

	baseDelay := is.config.RetryDelay
	maxDelay := 25 * time.Second // Cap the maximum delay // TODO: Check less than FetchTimeframe

	for attempt := 1; attempt <= is.config.MaxRetries; attempt++ {
		candles, err = exchange.FetchCandles(symbol, timeframe, start, end)
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

	return nil, fmt.Errorf("failed to fetch candles after %d attempts: %w", is.config.MaxRetries, err)
}

// runCleanupLoop runs the cleanup routine to remove old data
func (is *IngestionService) runCleanupLoop() {
	ticker := time.NewTicker(is.config.CleanupTimeframe)
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
func (is *IngestionService) cleanupOldData() error {
	return nil

	// TODO:

	if is.config.RetentionDays <= 0 {
		log.Printf("Skipping cleanup: RetentionDays is set to %d", is.config.RetentionDays)
		return nil
	}

	// Get all timeframes that need cleanup
	timeframes := append([]string{"1m"}, is.config.AggregationTimeframes...)

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

				// Create a done channel for the operation
				done := make(chan error, 1)

				go func() {
					done <- is.ingester.CleanupOldData(sym, tf, is.config.RetentionDays)
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

// AggregateHistoricalData aggregates historical data for all configured timeframes
// NOTE: Just updates db, not cache (for timeframes other than 1m)
func (is *IngestionService) AggregateHistoricalData(symbol string) error {
	log.Printf("[%s] Starting historical data aggregation", symbol)

	// Get the base timeframe (usually 1m)
	baseTimeframe := "1m"

	// If base timeframe is 1m, use optimized bulk aggregation
	if baseTimeframe == "1m" {
		return is.bulkAggregateHistorical1mData(symbol)
	}

	// NOTE: Unused logic
	// NOTE: Just save into db, dont update cache

	// For other base timeframes, use the original method
	for _, targetTimeframe := range is.config.AggregationTimeframes {
		if targetTimeframe == baseTimeframe {
			continue
		}

		log.Printf("[%s] Aggregating %s to %s", symbol, baseTimeframe, targetTimeframe)

		// Get historical data for the last 4 years
		end := time.Now()
		start := end.AddDate(-4, 0, 0).Truncate(24 * time.Hour)

		sourceCandles, err := is.storage.GetCandles(symbol, baseTimeframe, start, end)
		if err != nil {
			log.Printf("[%s] Failed to get source candles for %s: %v", symbol, baseTimeframe, err)
			continue
		}

		if len(sourceCandles) == 0 {
			log.Printf("[%s] No source candles found for %s", symbol, baseTimeframe)
			continue
		}

		// Aggregate
		aggregated, err := is.aggregator.Aggregate(sourceCandles, targetTimeframe)
		if err != nil {
			log.Printf("[%s] Failed to aggregate to %s: %v", symbol, targetTimeframe, err)
			continue
		}

		if len(aggregated) > 0 {
			// Save constructed candles
			if err := is.storage.SaveConstructedCandles(aggregated); err != nil {
				log.Printf("[%s] Failed to save %s constructed candles: %v", symbol, targetTimeframe, err)
				continue
			}

			log.Printf("[%s] Successfully aggregated %d constructed candles to %s", symbol, len(aggregated), targetTimeframe)
		}
	}

	return nil
}

// bulkAggregateHistorical1mData efficiently aggregates historical 1m candles to all higher timeframes
// NOTE: updates both db and cache
func (is *IngestionService) bulkAggregateHistorical1mData(symbol string) error {
	log.Printf("[%s] Starting bulk aggregation of historical 1m data", symbol)

	// Get the latest 1m candle to determine the time range
	latest1m, err := is.storage.GetLatest1mCandle(symbol)
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
	start := end.AddDate(-4, 0, 0).Truncate(24 * time.Hour)

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

		// Use bulk aggregation with timeout
		ctx, cancel := context.WithTimeout(is.ctx, 2*time.Minute)

		// Create a channel for the operation result
		resultCh := make(chan error, 1)

		go func(start, end time.Time) {
			// NOTE: start-time 00:00
			resultCh <- is.ingester.BulkAggregateFrom1m(symbol, start, end)
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

		cancel() // Always cancel the context

		if chunkErr != nil {
			log.Printf("[%s] Error aggregating chunk from %s to %s: %v",
				symbol, currentStart.Format(time.RFC3339), chunkEnd.Format(time.RFC3339), chunkErr)
			totalErrors = append(totalErrors, chunkErr)
		} else {
			// Count processed candles for this chunk
			count, _ := is.storage.GetCandleCount(symbol, "1m", currentStart, chunkEnd)
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

// FillMissing1mData identifies and fills gaps in 1m candle data
// ISSUE: Need to check the logic!
// Don't use for now!
func (is *IngestionService) FillMissing1mData(symbol string, start, end time.Time) error {
	log.Printf("[%s] Checking for missing 1m data from %s to %s", symbol, start.Format(time.RFC3339), end.Format(time.RFC3339))

	// Get missing ranges
	gaps, err := is.storage.GetMissingCandleRanges(symbol, start, end)
	if err != nil {
		return fmt.Errorf("failed to get missing ranges: %w", err)
	}

	if len(gaps) == 0 {
		log.Printf("[%s] No missing 1m data found", symbol)
		return nil
	}

	log.Printf("[%s] Found %d gaps in 1m data", symbol, len(gaps))

	// Fill each gap
	for i, gap := range gaps {
		log.Printf("[%s] Filling gap %d/%d: %s to %s", symbol, i+1, len(gaps), gap.Start.Format(time.RFC3339), gap.End.Format(time.RFC3339))

		// First try to fetch real data from exchanges
		var fetchedCandles []Candle
		var fetchError bool

		for exchangeName, exchange := range is.exchanges {
			candles, err := is.fetchCandlesWithRetry(exchange, symbol, "1m", gap.Start.Unix(), gap.End.Unix())
			if err != nil {
				log.Printf("[%s] Failed to fetch missing candles from %s: %v", symbol, exchangeName, err)
				continue
			}

			if len(candles) > 0 {
				fetchedCandles = candles
				log.Printf("[%s] Fetched %d candles from %s for gap", symbol, len(candles), exchangeName)
				break // Use first successful exchange
			}
		}

		// If we couldn't fetch any data or fetched incomplete data, generate synthetic candles
		if len(fetchedCandles) == 0 || !isContinuousMinuteRange(fetchedCandles, gap.Start, gap.End) {
			log.Printf("[%s] Generating synthetic candles for gap", symbol)

			// Get the candle before the gap to base synthetic candles on
			beforeGap, err := is.storage.GetLatestCandleInRange(symbol, "1m", gap.Start.Add(-time.Hour), gap.Start)
			if err != nil {
				log.Printf("[%s] Failed to get candle before gap: %v", symbol, err)
				fetchError = true
			}

			// Get the candle after the gap as well (if available)
			afterGap, err := is.storage.GetCandles(symbol, "1m", gap.End, gap.End.Add(time.Hour))
			if err != nil {
				log.Printf("[%s] Failed to get candle after gap: %v", symbol, err)
			}

			var basePrice float64
			var baseVolume float64

			// Determine base price and volume for synthetic candles
			if beforeGap != nil {
				basePrice = beforeGap.Close
				baseVolume = 0 // Set volume to 0 for synthetic candles
			} else if len(afterGap) > 0 {
				basePrice = afterGap[0].Open
				baseVolume = 0
			} else if !fetchError && len(fetchedCandles) > 0 {
				// If we have some fetched candles but not complete, use the first one
				basePrice = fetchedCandles[0].Open
				baseVolume = 0
			} else {
				log.Printf("[%s] Cannot generate synthetic candles: no reference price found", symbol)
				continue
			}

			// Generate synthetic candles for every minute in the gap
			var syntheticCandles []Candle

			// Create a map of timestamps for fetched candles to avoid duplicates
			fetchedTimestamps := make(map[time.Time]bool)
			for _, c := range fetchedCandles {
				fetchedTimestamps[c.Timestamp] = true
			}

			// Generate synthetic candles for each minute in the gap
			current := gap.Start
			for current.Before(gap.End) {
				// Skip if we already have a fetched candle for this timestamp
				if fetchedTimestamps[current] {
					current = current.Add(time.Minute)
					continue
				}

				syntheticCandle := Candle{
					Timestamp: current,
					Open:      basePrice,
					High:      basePrice,
					Low:       basePrice,
					Close:     basePrice,
					Volume:    baseVolume,
					Symbol:    symbol,
					Timeframe: "1m",
					Source:    "synthetic", // Mark as synthetic
				}

				syntheticCandles = append(syntheticCandles, syntheticCandle)
				current = current.Add(time.Minute)
			}

			// Combine fetched and synthetic candles
			allCandles := append(fetchedCandles, syntheticCandles...)

			// Sort by timestamp
			sort.Slice(allCandles, func(i, j int) bool {
				return allCandles[i].Timestamp.Before(allCandles[j].Timestamp)
			})

			// Ingest all candles
			if len(allCandles) > 0 {
				if err := is.ingester.IngestRaw1mCandles(allCandles); err != nil {
					log.Printf("[%s] Failed to ingest candles for gap: %v", symbol, err)
				} else {
					log.Printf("[%s] Filled gap with %d candles (%d fetched, %d synthetic)",
						symbol, len(allCandles), len(fetchedCandles), len(syntheticCandles))
				}
			}
		} else {
			// If we have complete data from exchange, just ingest it
			if err := is.ingester.IngestRaw1mCandles(fetchedCandles); err != nil {
				log.Printf("[%s] Failed to ingest fetched candles: %v", symbol, err)
			} else {
				log.Printf("[%s] Filled gap with %d fetched candles", symbol, len(fetchedCandles))
			}
		}
	}

	return nil
}

// isContinuousMinuteRange checks if the candles form a continuous minute-by-minute sequence
func isContinuousMinuteRange(candles []Candle, start, end time.Time) bool {
	if len(candles) == 0 {
		return false
	}

	// Sort candles by timestamp
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp.Before(candles[j].Timestamp)
	})

	// Check if first candle starts at the gap start
	if !candles[0].Timestamp.Equal(start) {
		return false
	}

	// Check if last candle ends at or after the gap end
	lastCandle := candles[len(candles)-1]
	if lastCandle.Timestamp.Add(time.Minute).Before(end) {
		return false
	}

	// Check for gaps in the sequence
	for i := 1; i < len(candles); i++ {
		prevEnd := candles[i-1].Timestamp.Add(time.Minute)
		if !prevEnd.Equal(candles[i].Timestamp) {
			return false
		}
	}

	return true
}

// GetIngestionStats returns statistics about the ingestion service
func (is *IngestionService) GetIngestionStats() map[string]any {
	stats := make(map[string]any)

	for _, symbol := range is.config.Symbols {
		stats[symbol] = make(map[string]any)

		for _, timeframe := range GetSupportedTimeframes() {
			latest, err := is.ingester.GetLatestCandleFromCache(symbol, timeframe)
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
			end := time.Now()
			start := end.Add(-24 * time.Hour)
			count, _ := is.storage.GetCandleCount(symbol, timeframe, start, end)

			stats[symbol].(map[string]any)[timeframe] = map[string]any{
				"latest_candle":    latest.Timestamp,
				"latest_price":     latest.Close,
				"candle_count_24h": count,
				"is_complete":      latest.IsComplete(),
			}

			// Get aggregation statistics
			aggStats, err := is.storage.GetAggregationStats(symbol)
			if err == nil {
				stats[symbol].(map[string]any)["aggregation_stats"] = aggStats
			}
		}
	}

	return stats
}
