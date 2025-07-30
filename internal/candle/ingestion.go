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

	"github.com/amirphl/simple-trader/internal/tfutils"
)

// Exchange interface for fetching candles from different exchanges
type Exchange interface {
	Name() string
	FetchCandles(ctx context.Context, symbol string, timeframe string, start, end time.Time) ([]Candle, error)
}

type IngestionService interface {
	Start() error
	Stop()
	GetIngestionStats() map[string]map[string]any
	Subscribe() <-chan []Candle
	UnSubscribe(ch <-chan []Candle)
}

// DefaultIngestionService handles real-time candle ingestion with automatic aggregation
type DefaultIngestionService struct {
	ingester Ingester
	cfg      IngestionConfig
	ctx      context.Context
	wg       sync.WaitGroup
}

// IngestionConfig holds configuration for the ingestion service
type IngestionConfig struct {
	Symbols             []string
	Exchange            Exchange
	FetchCycle          time.Duration
	RetentionDays       int
	MaxRetries          int
	RetryDelay          time.Duration
	DelayUpperbound     time.Duration
	EnableCleanup       bool
	CleanupCycle        time.Duration
	LiveFetchOldCandles int // TODO: Rename
}

// DefaultIngestionConfig returns a default configuration
func DefaultIngestionConfig(symbols []string, exchange Exchange) IngestionConfig {
	return IngestionConfig{
		Symbols:             symbols,
		Exchange:            exchange,
		FetchCycle:          30 * time.Second,
		RetentionDays:       30,
		MaxRetries:          3,
		RetryDelay:          3 * time.Second,
		DelayUpperbound:     20 * time.Second,
		EnableCleanup:       false,
		CleanupCycle:        24 * time.Hour,
		LiveFetchOldCandles: 1000,
	}
}

// NewIngestionService creates a new candle ingestion service
func NewIngestionService(ctx context.Context, ingester Ingester, cfg IngestionConfig) IngestionService {
	return &DefaultIngestionService{
		ingester: ingester,
		cfg:      cfg,
		ctx:      ctx,
	}
}

// Start begins the ingestion service with proper error handling and synchronization
func (is *DefaultIngestionService) Start() error {
	log.Printf("IngestionService | Starting candle ingestion service with %d symbols", len(is.cfg.Symbols))

	if is.cfg.Exchange == nil {
		return fmt.Errorf("no exchange configured for ingestion service")
	}

	if len(is.cfg.Symbols) == 0 {
		return fmt.Errorf("no symbols configured for ingestion service")
	}

	// Track active goroutines with WaitGroup
	is.wg.Add(len(is.cfg.Symbols))

	// Start ingestion loops for each symbol
	for _, symbol := range is.cfg.Symbols {
		go func(sym string) {
			defer is.wg.Done()
			is.runIngestionLoop(sym)
		}(symbol)
	}

	// Start cleanup routine if enabled
	if is.cfg.EnableCleanup {
		is.wg.Add(1)

		go func() {
			defer is.wg.Done()
			is.runCleanupLoop()
		}()
	}

	log.Printf("IngestionService | Ingestion service started successfully")
	return nil
}

// Stop gracefully stops the ingestion service
func (is *DefaultIngestionService) Stop() {
	log.Printf("IngestionService | Stopping ingestion service...")

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
		log.Printf("IngestionService | Ingestion service stopped gracefully")
	case <-ctx.Done():
		log.Printf("IngestionService | Ingestion service stop timed out")
	}
}

// runIngestionLoop runs the main ingestion loop for a symbol with improved error handling
func (is *DefaultIngestionService) runIngestionLoop(symbol string) {
	ticker := time.NewTicker(is.cfg.FetchCycle)
	defer ticker.Stop()

	const timeframe = "1m"
	log.Printf("IngestionService | [%s %s] Starting ingestion loop", symbol, timeframe)

	// Track consecutive errors for backoff
	// consecutiveErrors := 0
	// maxConsecutiveErrors := 5
	// baseBackoff := time.Second * 5

	for {
		select {
		case <-is.ctx.Done():
			log.Printf("IngestionService | [%s %s] Stopping ingestion loop", symbol, timeframe)
			return
		case <-ticker.C:
			if err := is.fetchAndIngestCandles(symbol); err != nil {
				log.Printf("IngestionService | [%s %s] Error in ingestion loop: %v", symbol, timeframe, err)
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
	log.Printf("IngestionService | [%s] Fetching candles", symbol)

	const timeframe = "1m"

	// TODO: Add Tx to ctx. THIS IS THE MAIN POINT OF STARTING THE TRANSACTION.
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
		// No previous candles
		start = end.Add(-time.Duration(is.cfg.LiveFetchOldCandles) * time.Minute).Truncate(24 * time.Hour)
	} else {
		// NOTE: Try to receive duplicated candles to avoid missing candles
		start = latest.Timestamp.Add(-10 * time.Minute)
	}

	// If there's no new data to fetch, return early
	if start.After(end) || start.Equal(end) {
		return nil
	}

	candles, err := is.fetchCandlesWithRetry(ctx, symbol, timeframe, start, end)
	if err != nil {
		log.Printf("IngestionService | [%s %s] Failed to fetch candles from %s: %v", symbol, timeframe, is.cfg.Exchange.Name(), err)
		// NOTE: No need to generate fake candle
		return err
	}
	if len(candles) == 0 {
		log.Printf("IngestionService | [%s %s] No candles fetched from %s", symbol, timeframe, is.cfg.Exchange.Name())
		return nil
	}

	log.Printf("IngestionService | [%s %s] Fetched %d candles from %s", symbol, timeframe, len(candles), is.cfg.Exchange.Name())

	candlesMap := make(map[time.Time]Candle)

	for _, c := range candles {
		// Truncate timestamp to remove seconds
		dur := tfutils.GetTimeframeDuration(timeframe)
		c.Timestamp = c.Timestamp.Truncate(dur)
		c.Source = is.cfg.Exchange.Name()

		// Validate all candles before ingestion
		// TODO: not sorted, missing candles, same timeframe, same symbol, all are truncated, has duplicates
		if err := c.Validate(); err != nil {
			log.Printf("IngestionService | [%s %s] Invalid candle: %v", symbol, timeframe, err)
			return fmt.Errorf("invalid candle: %w", err)
		}

		candlesMap[c.Timestamp] = c
	}

	sort.Slice(candles, func(i, j int) bool {
		return candles[i].Timestamp.Before(candles[j].Timestamp)
	})

	// Generate synthetic candles for missing minutes
	var completeCandles []Candle

	if latest == nil {
		latest = &candles[0] // NOTE: No problem loosing the first 1m candle
	}

	// Start from the minute after the latest candle
	currentTime := latest.Timestamp.Add(time.Minute)
	// Use the latest candle's close price as the base for synthetic candles
	basePrice := latest.Close

	lastCandleTimestamp := candles[len(candles)-1].Timestamp

	syntheticCandles := 0

	for !currentTime.After(lastCandleTimestamp) {
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
			log.Printf("IngestionService | [%s %s] Generated synthetic candle for %v", symbol, timeframe, currentTime)
			syntheticCandles++
		} else {
			completeCandles = append(completeCandles, c)
			// Update the base price for future synthetic candles
			basePrice = c.Close
		}

		currentTime = currentTime.Add(time.Minute)
	}

	// Ingest the complete set of candles (real + synthetic)
	if len(completeCandles) > 0 {
		if err := is.ingester.IngestRaw1mCandles(ctx, completeCandles); err != nil {
			log.Printf("IngestionService | [%s %s] Failed to ingest candles: %v", symbol, timeframe, err)
			return fmt.Errorf("failed to ingest candles: %w", err)
		}
		log.Printf("IngestionService | [%s %s] Successfully ingested %d candles (%d real, %d synthetic)",
			symbol, timeframe, len(completeCandles), len(candles), syntheticCandles)
	}

	return nil
}

// fetchCandlesWithRetry fetches candles with exponential backoff retry logic
func (is *DefaultIngestionService) fetchCandlesWithRetry(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error) {
	var candles []Candle
	var err error

	baseDelay := is.cfg.RetryDelay
	maxDelay := is.cfg.DelayUpperbound

	for attempt := 1; attempt <= is.cfg.MaxRetries; attempt++ {
		select {
		case <-is.ctx.Done():
			log.Printf("IngestionService | [%s %s] Timeout fetching candles from %s", symbol, timeframe, is.cfg.Exchange.Name())
			return nil, ctx.Err()
		default:

			candles, err = is.cfg.Exchange.FetchCandles(ctx, symbol, timeframe, start, end)
			if err == nil {
				return candles, nil
			}

			// Don't sleep after the last attempt
			if attempt < is.cfg.MaxRetries {
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

				log.Printf("IngestionService | [%s %s] Fetch attempt %d failed, retrying in %v: %v",
					symbol, timeframe, attempt, backoffWithJitter, err)
				time.Sleep(backoffWithJitter)
			}
		}
	}

	return nil, fmt.Errorf("failed to fetch candles after %d attempts: %w", is.cfg.MaxRetries, err)
}

// runCleanupLoop runs the cleanup routine to remove old data
func (is *DefaultIngestionService) runCleanupLoop() {
	ticker := time.NewTicker(is.cfg.CleanupCycle)
	defer ticker.Stop()

	log.Printf("IngestionService | Starting cleanup loop with %d day retention", is.cfg.RetentionDays)

	// Run cleanup immediately on start instead of waiting for first tick
	if err := is.cleanupOldData(); err != nil {
		log.Printf("Initial cleanup failed: %v", err)
	}

	for {
		select {
		case <-is.ctx.Done():
			log.Printf("IngestionService | Stopping cleanup loop")
			return
		case <-ticker.C:
			startTime := time.Now()
			log.Printf("IngestionService | Starting scheduled data cleanup...")

			if err := is.cleanupOldData(); err != nil {
				log.Printf("IngestionService | Error in cleanup loop: %v", err)
			} else {
				duration := time.Since(startTime)
				log.Printf("IngestionService | Cleanup completed successfully in %v", duration)
			}
		}
	}
}

// cleanupOldData removes old candles to prevent database bloat
func (is *DefaultIngestionService) cleanupOldData() error {
	return nil

	// TODO: Check logic

	if is.cfg.RetentionDays <= 0 {
		log.Printf("IngestionService | Skipping cleanup: RetentionDays is set to %d", is.cfg.RetentionDays)
		return nil
	}

	// Get all timeframes that need cleanup
	timeframes := tfutils.GetSupportedTimeframes()

	// Track errors but continue with other symbols/timeframes
	var errors []error
	successCount := 0

	// Use a semaphore to limit concurrent database operations
	sem := make(chan struct{}, 5) // Max 5 concurrent cleanup operations
	var wg sync.WaitGroup

	// Mutex for thread-safe error collection
	var mu sync.Mutex

	for _, symbol := range is.cfg.Symbols {
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
					done <- is.ingester.CleanupOldData(ctx, sym, tf, is.cfg.RetentionDays)
				}()

				// Wait for either completion or timeout
				select {
				case err := <-done:
					if err != nil {
						mu.Lock()
						errors = append(errors, fmt.Errorf("[%s %s] cleanup failed: %w", sym, tf, err))
						mu.Unlock()
						log.Printf("IngestionService | [%s %s] Failed to cleanup old data: %v", sym, tf, err)
					} else {
						mu.Lock()
						successCount++
						mu.Unlock()
						log.Printf("IngestionService | [%s %s] Successfully cleaned up old data", sym, tf)
					}
				case <-ctx.Done():
					mu.Lock()
					errors = append(errors, fmt.Errorf("[%s %s] cleanup timed out", sym, tf))
					mu.Unlock()
					log.Printf("IngestionService | [%s %s] Cleanup operation timed out", sym, tf)
				}
			}()
		}
	}

	// Wait for all cleanup operations to complete
	wg.Wait()

	// Report results
	if len(errors) > 0 {
		log.Printf("IngestionService | Cleanup completed with %d successes and %d failures", successCount, len(errors))
		return fmt.Errorf("cleanup encountered %d errors: %v", len(errors), errors[0])
	}

	log.Printf("IngestionService | Cleanup completed successfully for %d symbol-timeframe combinations", successCount)
	return nil
}

// GetIngestionStats returns statistics about the ingestion service
func (is *DefaultIngestionService) GetIngestionStats() map[string]map[string]any {
	stats := make(map[string]map[string]any)

	ctx, cancel := deriveContextWithTimeout(is.ctx)
	defer cancel()

	for _, symbol := range is.cfg.Symbols {
		stats[symbol] = make(map[string]any)

		for _, timeframe := range tfutils.GetSupportedTimeframes() {
			latest, err := is.ingester.GetLatestCandle(ctx, symbol, timeframe)
			if err != nil {
				stats[symbol][timeframe] = map[string]any{
					"error": err.Error(),
				}
				continue
			}

			if latest == nil {
				stats[symbol][timeframe] = map[string]any{
					"latest_candle": nil,
					"candle_count":  0,
				}
				continue
			}

			// Get candle count for last 24 hours
			end := time.Now().UTC()
			start := end.Add(-24 * time.Hour)
			count, _ := is.ingester.GetCandleCount(ctx, symbol, timeframe, start, end)

			stats[symbol][timeframe] = map[string]any{
				"latest_candle":    latest.Timestamp,
				"latest_price":     latest.Close,
				"candle_count_24h": count,
				"is_complete":      latest.IsComplete(),
			}

			// Get aggregation statistics
			aggStats, err := is.ingester.GetAggregationStats(ctx, symbol)
			if err == nil {
				stats[symbol]["aggregation_stats"] = aggStats
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
