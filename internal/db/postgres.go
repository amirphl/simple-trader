package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/amirphl/simple-trader/internal/db/conf"
	_ "github.com/lib/pq"
)

// Transaction context key
type txKey struct{}

// WithTransaction adds a transaction to the context
func WithTransaction(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// GetTransaction retrieves a transaction from context, or returns nil if not present
func GetTransaction(ctx context.Context) *sql.Tx {
	if tx, ok := ctx.Value(txKey{}).(*sql.Tx); ok {
		return tx
	}
	return nil
}

// executeWithTransaction executes a function with proper transaction management
// If a transaction exists in context, it uses that. Otherwise, it creates a new one.
func (p *Default) executeWithTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	// Check if transaction exists in context
	if tx := GetTransaction(ctx); tx != nil {
		// Use existing transaction
		return fn(tx)
	}

	// Create new transaction
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Execute the function
	if fnErr := fn(tx); fnErr != nil {
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

// queryWithTransaction executes a query using transaction from context if available
func (p *Default) queryWithTransaction(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if tx := GetTransaction(ctx); tx != nil {
		return tx.QueryContext(ctx, query, args...)
	}
	return p.db.QueryContext(ctx, query, args...)
}

type Default struct {
	db *sql.DB
}

func New(c conf.Config) (*Default, error) {
	return &Default{db: c.DB}, nil
}

func (p *Default) GetDB() *sql.DB {
	return p.db
}

// SaveCandle saves a single candle to the database
func (p *Default) SaveCandle(ctx context.Context, c Candle) error {
	// Validate candle before saving
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid candle for %s %s at %s: %w", c.Symbol, c.Timeframe, c.Timestamp, err)
	}

	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
		INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume, source)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (symbol, timeframe, timestamp, source) DO UPDATE SET 
			open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, 
			close=EXCLUDED.close, volume=EXCLUDED.volume`,
			c.Symbol, c.Timeframe, c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume, c.Source)
		if err != nil {
			return fmt.Errorf("failed to save candle for %s %s at %s: %w", c.Symbol, c.Timeframe, c.Timestamp, err)
		}

		return nil
	})
}

func (p *Default) SaveCandles(ctx context.Context, candles []Candle) error {
	if len(candles) == 0 {
		return nil
	}

	// Validate all candles first
	for i, c := range candles {
		if err := c.Validate(); err != nil {
			return fmt.Errorf("invalid candle at index %d for %s %s at %s: %w",
				i, c.Symbol, c.Timeframe, c.Timestamp, err)
		}
	}

	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		// Prepare the insert statement
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume, source)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (symbol, timeframe, timestamp, source) DO UPDATE SET 
				open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, 
				close=EXCLUDED.close, volume=EXCLUDED.volume
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare insert statement: %w", err)
		}
		defer stmt.Close()

		// Insert each candle individually
		savedCount := 0
		for i, c := range candles {
			result, err := stmt.ExecContext(ctx,
				c.Symbol, c.Timeframe, c.Timestamp, c.Open, c.High, c.Low, c.Close, c.Volume, c.Source)
			if err != nil {
				return fmt.Errorf("failed to save candle at index %d (%s %s at %s): %w",
					i, c.Symbol, c.Timeframe, c.Timestamp, err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected for candle at index %d (%s %s at %s): %w",
					i, c.Symbol, c.Timeframe, c.Timestamp, err)
			}

			if rowsAffected > 0 {
				savedCount++
			}
		}

		// if savedCount == 0 {
		// 	return fmt.Errorf("no candles were saved out of %d attempted", len(candles))
		// }

		return nil
	})
}

// SaveConstructedCandles efficiently saves constructed (aggregated) candles
func (p *Default) SaveConstructedCandles(ctx context.Context, candles []Candle) error {
	if len(candles) == 0 {
		return nil
	}

	// Set source to "constructed" for all candles
	for i := range candles {
		candles[i].Source = "constructed"
	}

	return p.SaveCandles(ctx, candles)
}

// SaveRaw1mCandles efficiently saves raw 1m candles with optimized batch processing
func (p *Default) SaveRaw1mCandles(ctx context.Context, candles []Candle) error {
	if len(candles) == 0 {
		return nil
	}

	for _, c := range candles {
		if c.Timeframe != "1m" {
			return fmt.Errorf("failed to save candle for %s %s at %s: timeframe is not 1m", c.Symbol, c.Timeframe, c.Timestamp)
		}
	}

	return p.SaveCandles(ctx, candles)
}

// Get1mCandlesForAggregation efficiently retrieves 1m candles for aggregation
func (p *Default) Get1mCandlesForAggregation(ctx context.Context, symbol string, start, end time.Time) ([]Candle, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe='1m' AND timestamp >= $2 AND timestamp < $3 
		ORDER BY timestamp ASC`,
		symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query 1m candles: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var candles []Candle
	for rows.Next() {
		var c Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	return candles, nil
}

// GetCandle retrieves a single candle by symbol, timeframe, timestamp, and source
func (p *Default) GetCandle(ctx context.Context, symbol, timeframe string, timestamp time.Time, source string) (*Candle, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp=$3 AND source=$4 
		LIMIT 1`,
		symbol, timeframe, timestamp, source)
	if err != nil {
		return nil, fmt.Errorf("failed to query candle: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var c Candle
	if rows.Next() {
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		return &c, nil
	}

	return nil, nil
}

// GetCandlesV2 retrieves candles with a specific timeframe in a time range without filtering by symbol
// This is useful for bulk operations across all symbols
func (p *Default) GetCandlesV2(ctx context.Context, timeframe string, start, end time.Time) ([]Candle, error) {
	query := `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE timeframe=$1 AND timestamp >= $2 AND timestamp < $3 
		ORDER BY symbol, timestamp ASC`

	rows, err := p.queryWithTransaction(ctx, query, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query candles: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	// Pre-allocate slice with reasonable capacity to reduce reallocations
	candles := make([]Candle, 0, 1000)

	for rows.Next() {
		var c Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume,
			&c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating candle rows: %w", err)
	}

	return candles, nil
}

// GetCandles retrieves candles in a specific time range for a symbol and timeframe and source
func (p *Default) GetCandles(ctx context.Context, symbol, timeframe, source string, start, end time.Time) ([]Candle, error) {
	query := `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4`
	args := []any{symbol, timeframe, start, end}

	if source != "" {
		query += " AND source=$5"
		args = append(args, source)
	}

	query += " ORDER BY timestamp ASC"

	rows, err := p.queryWithTransaction(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query candles in range: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var candles []Candle
	for rows.Next() {
		var c Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan candle: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating candle rows: %w", err)
	}

	return candles, nil
}

// GetRawCandles retrieves only raw candles (not constructed)
func (p *Default) GetRawCandles(ctx context.Context, symbol, timeframe string, start, end time.Time) ([]Candle, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source != 'constructed' AND timestamp >= $3 AND timestamp < $4 
		ORDER BY timestamp ASC`,
		symbol, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query raw candles: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var candles []Candle
	for rows.Next() {
		var c Candle
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan raw candle: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		candles = append(candles, c)
	}

	return candles, nil
}

// GetLatestCandle retrieves the latest candle with optimized querying
func (p *Default) GetLatestCandle(ctx context.Context, symbol, timeframe string) (*Candle, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 
		ORDER BY timestamp DESC LIMIT 1`,
		symbol, timeframe)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest candle: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var c Candle
	if rows.Next() {
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan latest candle: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		return &c, nil
	}

	return nil, nil
}

// GetLatestCandleInRange retrieves the latest candle within a specific time range
// This is useful when you need the most recent candle before a certain time
func (p *Default) GetLatestCandleInRange(ctx context.Context, symbol, timeframe string, start, end time.Time) (*Candle, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4
		ORDER BY timestamp DESC LIMIT 1`,
		symbol, timeframe, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest candle in range: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var c Candle
	if rows.Next() {
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan latest candle in range: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		return &c, nil
	}

	return nil, nil
}

// GetLatestConstructedCandle retrieves the latest constructed candle
func (p *Default) GetLatestConstructedCandle(ctx context.Context, symbol, timeframe string) (*Candle, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT timestamp, open, high, low, close, volume, symbol, timeframe, source 
		FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source='constructed' 
		ORDER BY timestamp DESC LIMIT 1`,
		symbol, timeframe)
	if err != nil {
		return nil, fmt.Errorf("failed to query latest constructed candle: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var c Candle
	if rows.Next() {
		if err := rows.Scan(&c.Timestamp, &c.Open, &c.High, &c.Low, &c.Close, &c.Volume, &c.Symbol, &c.Timeframe, &c.Source); err != nil {
			return nil, fmt.Errorf("failed to scan latest constructed candle: %w", err)
		}
		c.Timestamp = c.Timestamp.UTC() // TODO:
		return &c, nil
	}

	return nil, nil
}

// DeleteCandle removes a specific candle
func (p *Default) DeleteCandle(ctx context.Context, symbol, timeframe, source string, timestamp time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp=$3 AND source=$4`,
			symbol, timeframe, timestamp, source)
		if err != nil {
			return fmt.Errorf("failed to delete candle: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return fmt.Errorf("no candle found to delete for %s %s at %s", symbol, timeframe, timestamp)
		}

		return nil
	})
}

// DeleteCandles removes old candles with optimized deletion
func (p *Default) DeleteCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp < $3`,
			symbol, timeframe, before)
		if err != nil {
			return fmt.Errorf("failed to delete candles: %w", err)
		}
		return nil
	})
}

// DeleteCandlesInRange removes candles in a specific time range for a symbol and timeframe
func (p *Default) DeleteCandlesInRange(ctx context.Context, symbol, timeframe, source string, start, end time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		query := `DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4`
		args := []any{symbol, timeframe, start, end}

		if source != "" {
			query += " AND source=$5"
			args = append(args, source)
		}

		_, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("failed to delete candles in range: %w", err)
		}
		return nil
	})
}

// DeleteConstructedCandles removes old constructed candles
func (p *Default) DeleteConstructedCandles(ctx context.Context, symbol, timeframe string, before time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM candles WHERE symbol=$1 AND timeframe=$2 AND source='constructed' AND timestamp < $3`,
			symbol, timeframe, before)
		if err != nil {
			return fmt.Errorf("failed to delete constructed candles: %w", err)
		}
		return nil
	})
}

// GetCandleCount returns the count of candles in a time range
func (p *Default) GetCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	var count int
	rows, err := p.queryWithTransaction(ctx, `
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND timestamp >= $3 AND timestamp < $4`,
		symbol, timeframe, start, end)
	if err != nil {
		return 0, fmt.Errorf("failed to get candle count: %w", err)
	}
	if rows == nil {
		return 0, nil
	}
	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to scan candle count: %w", err)
		}
		return count, nil
	}

	return 0, nil
}

// GetConstructedCandleCount returns the count of constructed candles
func (p *Default) GetConstructedCandleCount(ctx context.Context, symbol, timeframe string, start, end time.Time) (int, error) {
	var count int
	rows, err := p.queryWithTransaction(ctx, `
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND timeframe=$2 AND source='constructed' AND timestamp >= $3 AND timestamp < $4`,
		symbol, timeframe, start, end)
	if err != nil {
		return 0, fmt.Errorf("failed to get constructed candle count: %w", err)
	}
	if rows == nil {
		return 0, nil
	}
	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to scan constructed candle count: %w", err)
		}
		return count, nil
	}

	return 0, nil
}

// UpdateCandle updates an existing candle
func (p *Default) UpdateCandle(ctx context.Context, c Candle) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("invalid candle for update: %w", err)
	}

	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
			UPDATE candles 
			SET open=$1, high=$2, low=$3, close=$4, volume=$5 
			WHERE symbol=$6 AND timeframe=$7 AND timestamp=$8 AND source=$9`,
			c.Open, c.High, c.Low, c.Close, c.Volume, c.Symbol, c.Timeframe, c.Timestamp, c.Source)
		if err != nil {
			return fmt.Errorf("failed to update candle: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected == 0 {
			return fmt.Errorf("no candle found to update for %s %s at %s", c.Symbol, c.Timeframe, c.Timestamp)
		}

		return nil
	})
}

// UpdateCandles updates multiple candles in a single transaction using a bulk update approach
func (p *Default) UpdateCandles(ctx context.Context, candles []Candle) error {
	if len(candles) == 0 {
		return nil
	}

	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		// Prepare the update statement
		stmt, err := tx.PrepareContext(ctx, `
			UPDATE candles 
			SET open=$1, high=$2, low=$3, close=$4, volume=$5 
			WHERE symbol=$6 AND timeframe=$7 AND timestamp=$8 AND source=$9
		`)
		if err != nil {
			return fmt.Errorf("failed to prepare update statement: %w", err)
		}
		defer stmt.Close()

		// Update each candle individually
		updatedCount := 0
		for _, c := range candles {
			// Validate candle before updating
			if err := c.Validate(); err != nil {
				return fmt.Errorf("invalid candle for update (%s %s at %s): %w",
					c.Symbol, c.Timeframe, c.Timestamp, err)
			}

			result, err := stmt.ExecContext(ctx,
				c.Open, c.High, c.Low, c.Close, c.Volume,
				c.Symbol, c.Timeframe, c.Timestamp, c.Source)
			if err != nil {
				return fmt.Errorf("failed to update candle (%s %s at %s): %w",
					c.Symbol, c.Timeframe, c.Timestamp, err)
			}

			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("failed to get rows affected for candle (%s %s at %s): %w",
					c.Symbol, c.Timeframe, c.Timestamp, err)
			}

			if rowsAffected > 0 {
				updatedCount++
			}
		}

		// if updatedCount == 0 {
		// 	return fmt.Errorf("no candles were updated out of %d attempted", len(candles))
		// }

		return nil
	})
}

// GetAggregationStats returns statistics useful for aggregation
func (p *Default) GetAggregationStats(ctx context.Context, symbol string) (map[string]any, error) {
	stats := make(map[string]any)

	// Get latest 1m candle
	latest1m, err := p.GetLatestCandle(ctx, symbol, "1m")
	if err != nil {
		return nil, fmt.Errorf("failed to get latest 1m candle: %w", err)
	}

	if latest1m != nil {
		stats["latest_1m"] = map[string]any{
			"timestamp":   latest1m.Timestamp,
			"close":       latest1m.Close,
			"is_complete": latest1m.IsComplete(),
		}
	}

	// Get candle counts for different timeframes
	timeframes := []string{"1m", "5m", "15m", "30m", "1h", "4h", "1d"}
	end := time.Now()
	start := end.Add(-24 * time.Hour)

	for _, timeframe := range timeframes {
		count, err := p.GetCandleCount(ctx, symbol, timeframe, start, end)
		if err != nil {
			continue // Skip if error
		}
		stats[fmt.Sprintf("count_24h_%s", timeframe)] = count
	}

	return stats, nil
}

// GetMissingCandleRanges identifies gaps in 1m candle data
func (p *Default) GetMissingCandleRanges(ctx context.Context, symbol string, start, end time.Time) ([]struct{ Start, End time.Time }, error) {
	// TODO: Logic
	// Query for gaps in 1m candles
	rows, err := p.queryWithTransaction(ctx, `
		WITH time_series AS (
			SELECT generate_series($1::timestamp, $2::timestamp, '1 minute'::timeframe) as expected_time
		),
		actual_candles AS (
			SELECT timestamp FROM candles 
			WHERE symbol=$3 AND timeframe='1m' AND timestamp >= $1 AND timestamp < $2
		)
		SELECT 
			ts.expected_time as gap_start,
			LEAD(ts.expected_time) OVER (ORDER BY ts.expected_time) as gap_end
		FROM time_series ts
		LEFT JOIN actual_candles ac ON ts.expected_time = ac.timestamp
		WHERE ac.timestamp IS NULL
		ORDER BY ts.expected_time`,
		start, end, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to query missing ranges: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var gaps []struct{ Start, End time.Time }
	for rows.Next() {
		var gap struct{ Start, End time.Time }
		if err := rows.Scan(&gap.Start, &gap.End); err != nil {
			return nil, fmt.Errorf("failed to scan gap: %w", err)
		}
		gaps = append(gaps, gap)
	}

	return gaps, nil
}

// GetCandleSourceStats returns statistics about candle sources
func (p *Default) GetCandleSourceStats(ctx context.Context, symbol string, start, end time.Time) (map[string]any, error) {
	stats := make(map[string]any)

	// Get source distribution
	rows, err := p.queryWithTransaction(ctx, `
		SELECT source, timeframe, COUNT(*) as count
		FROM candles 
		WHERE symbol=$1 AND timestamp >= $2 AND timestamp < $3
		GROUP BY source, timeframe
		ORDER BY source, timeframe`,
		symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to query source stats: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	sourceStats := make(map[string]map[string]int)
	for rows.Next() {
		var source, timeframe string
		var count int
		if err := rows.Scan(&source, &timeframe, &count); err != nil {
			return nil, fmt.Errorf("failed to scan source stat: %w", err)
		}

		if sourceStats[source] == nil {
			sourceStats[source] = make(map[string]int)
		}
		sourceStats[source][timeframe] = count
	}

	stats["source_distribution"] = sourceStats

	// Get constructed vs raw candle counts
	var constructedCount, rawCount int

	rows, err = p.queryWithTransaction(ctx, `
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND source='constructed' AND timestamp >= $2 AND timestamp < $3`,
		symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to get constructed count: %w", err)
	}
	if rows == nil {
		constructedCount = 0
	} else {
		defer rows.Close()
		if rows.Next() {
			err = rows.Scan(&constructedCount)
			if err != nil {
				return nil, fmt.Errorf("failed to scan constructed count: %w", err)
			}
		}
	}

	rows, err = p.queryWithTransaction(ctx, `
		SELECT COUNT(*) FROM candles 
		WHERE symbol=$1 AND source != 'constructed' AND timestamp >= $2 AND timestamp < $3`,
		symbol, start, end)
	if err != nil {
		return nil, fmt.Errorf("failed to get raw count: %w", err)
	}
	if rows == nil {
		rawCount = 0
	} else {
		defer rows.Close()
		if rows.Next() {
			err = rows.Scan(&rawCount)
			if err != nil {
				return nil, fmt.Errorf("failed to scan raw count: %w", err)
			}
		}
	}

	stats["constructed_count"] = constructedCount
	stats["raw_count"] = rawCount
	stats["total_count"] = constructedCount + rawCount

	return stats, nil
}

func (p *Default) SaveOrderBook(ctx context.Context, ob OrderBook) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		bids, _ := json.Marshal(ob.Bids)
		asks, _ := json.Marshal(ob.Asks)
		_, err := tx.ExecContext(ctx, `INSERT INTO orderbooks (symbol, timestamp, bids, asks) VALUES ($1,$2,$3,$4)`,
			ob.Symbol, ob.Timestamp, bids, asks)
		if err != nil {
			return fmt.Errorf("failed to save orderbook: %w", err)
		}
		return nil
	})
}

func (p *Default) GetOrderBooks(ctx context.Context, symbol string, start, end time.Time) ([]OrderBook, error) {
	rows, err := p.queryWithTransaction(ctx, `SELECT symbol, timestamp, bids, asks FROM orderbooks WHERE symbol=$1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC`, symbol, start, end)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var obs []OrderBook
	for rows.Next() {
		var ob OrderBook
		var bids, asks []byte
		if err := rows.Scan(&ob.Symbol, &ob.Timestamp, &bids, &asks); err != nil {
			return nil, err
		}
		json.Unmarshal(bids, &ob.Bids)
		json.Unmarshal(asks, &ob.Asks)
		ob.Timestamp = ob.Timestamp.UTC() // TODO:
		obs = append(obs, ob)
	}
	return obs, nil
}

func (p *Default) DeleteOrderBooks(ctx context.Context, symbol string, before time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM orderbooks WHERE symbol=$1 AND timestamp < $2`, symbol, before)
		if err != nil {
			return fmt.Errorf("failed to delete orderbooks: %w", err)
		}
		return nil
	})
}

func (p *Default) SaveTick(ctx context.Context, tick Tick) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO ticks (symbol, price, quantity, side, timestamp) VALUES ($1,$2,$3,$4,$5)`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx, tick.Symbol, tick.Price, tick.Quantity, tick.Side, tick.Timestamp)
		if err != nil {
			return fmt.Errorf("failed to save tick: %w", err)
		}
		return nil
	})
}

func (p *Default) SaveTicks(ctx context.Context, ticks []Tick) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO ticks (symbol, price, quantity, side, timestamp) VALUES ($1,$2,$3,$4,$5)`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement: %w", err)
		}
		defer stmt.Close()
		for _, t := range ticks {
			_, err := stmt.ExecContext(ctx, t.Symbol, t.Price, t.Quantity, t.Side, t.Timestamp)
			if err != nil {
				return fmt.Errorf("failed to save tick: %w", err)
			}
		}
		return nil
	})
}

func (p *Default) GetTicks(ctx context.Context, symbol string, start, end time.Time) ([]Tick, error) {
	rows, err := p.queryWithTransaction(ctx, `SELECT symbol, price, quantity, side, timestamp FROM ticks WHERE symbol=$1 AND timestamp >= $2 AND timestamp < $3 ORDER BY timestamp ASC`, symbol, start, end)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var ticks []Tick
	for rows.Next() {
		var t Tick
		if err := rows.Scan(&t.Symbol, &t.Price, &t.Quantity, &t.Side, &t.Timestamp); err != nil {
			return nil, err
		}
		t.Timestamp = t.Timestamp.UTC() // TODO:
		ticks = append(ticks, t)
	}
	return ticks, nil
}

func (p *Default) DeleteTicks(ctx context.Context, symbol string, before time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM ticks WHERE symbol=$1 AND timestamp < $2`, symbol, before)
		if err != nil {
			return fmt.Errorf("failed to delete ticks: %w", err)
		}
		return nil
	})
}

func (p *Default) SaveOrder(ctx context.Context, o Order) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO orders (order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (order_id) DO UPDATE SET status=EXCLUDED.status, filled_qty=EXCLUDED.filled_qty, avg_price=EXCLUDED.avg_price, updated_at=EXCLUDED.updated_at`,
			o.OrderID, o.Symbol, o.Side, o.Type, o.Price, o.Quantity, o.Status, o.FilledQty, o.AvgPrice, o.Timestamp, o.Timestamp)
		if err != nil {
			return fmt.Errorf("failed to save order: %w", err)
		}
		return nil
	})
}

func (p *Default) GetOrder(ctx context.Context, orderID string) (*Order, error) {
	rows, err := p.queryWithTransaction(ctx, `SELECT order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at FROM orders WHERE order_id=$1`, orderID)
	if err != nil {
		return nil, fmt.Errorf("failed to query order: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var o Order
	if rows.Next() {
		if err := rows.Scan(&o.OrderID, &o.Symbol, &o.Side, &o.Type, &o.Price, &o.Quantity, &o.Status, &o.FilledQty, &o.AvgPrice, &o.Timestamp, &o.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan order: %w", err)
		}
		o.Timestamp = o.Timestamp.UTC() // TODO:
		o.UpdatedAt = o.UpdatedAt.UTC() // TODO:
		return &o, nil
	}

	return nil, nil
}

func (p *Default) GetOpenOrders(ctx context.Context) ([]Order, error) {
	rows, err := p.queryWithTransaction(ctx, `SELECT order_id, symbol, side, type, price, quantity, status, filled_qty, avg_price, created_at, updated_at FROM orders WHERE status NOT IN ('FILLED', 'CANCELED', 'EXPIRED')`)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var orders []Order
	for rows.Next() {
		var o Order
		if err := rows.Scan(&o.OrderID, &o.Symbol, &o.Side, &o.Type, &o.Price, &o.Quantity, &o.Status, &o.FilledQty, &o.AvgPrice, &o.Timestamp, &o.UpdatedAt); err != nil {
			return nil, err
		}
		o.Timestamp = o.Timestamp.UTC() // TODO:
		o.UpdatedAt = o.UpdatedAt.UTC() // TODO:
		orders = append(orders, o)
	}
	return orders, nil
}

func (p *Default) CloseOrder(ctx context.Context, orderID string) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE orders SET status='CLOSED', updated_at=$1 WHERE order_id=$2`, time.Now(), orderID)
		if err != nil {
			return fmt.Errorf("failed to close order: %w", err)
		}
		return nil
	})
}

func (p *Default) UpdateOrderStatus(ctx context.Context, orderID, status string, filledQty, avgPrice float64, updatedAt time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE orders SET status=$1, filled_qty=$2, avg_price=$3, updated_at=$4 WHERE order_id=$5`,
			status, filledQty, avgPrice, updatedAt, orderID)
		if err != nil {
			return fmt.Errorf("failed to update order status: %w", err)
		}
		return nil
	})
}

func (p *Default) DeleteOrder(ctx context.Context, orderID string) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM orders WHERE order_id=$1`, orderID)
		if err != nil {
			return fmt.Errorf("failed to delete order: %w", err)
		}
		return nil
	})
}

func (p *Default) LogEvent(ctx context.Context, event Event) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		data, _ := json.Marshal(event.Data)
		_, err := tx.ExecContext(ctx, `INSERT INTO events (time, type, description, data) VALUES ($1,$2,$3,$4)`,
			event.Time, event.Type, event.Description, data)
		if err != nil {
			return fmt.Errorf("failed to log event: %w", err)
		}
		return nil
	})
}

func (p *Default) GetEvents(ctx context.Context, eventType string, start, end time.Time) ([]Event, error) {
	rows, err := p.queryWithTransaction(ctx, `SELECT time, type, description, data FROM events WHERE type=$1 AND time >= $2 AND time <= $3 ORDER BY time ASC`, eventType, start, end)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var e Event
		var data []byte
		if err := rows.Scan(&e.Time, &e.Type, &e.Description, &data); err != nil {
			return nil, err
		}
		json.Unmarshal(data, &e.Data)
		e.Time = e.Time.UTC() // TODO:
		events = append(events, e)
	}
	return events, nil
}

func (p *Default) DeleteEvents(ctx context.Context, eventType string, before time.Time) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM events WHERE type=$1 AND time < $2`, eventType, before)
		if err != nil {
			return fmt.Errorf("failed to delete events: %w", err)
		}
		return nil
	})
}

func (p *Default) SaveState(ctx context.Context, state map[string]any) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		for k, v := range state {
			val, _ := json.Marshal(v)
			_, err := tx.ExecContext(ctx, `INSERT INTO state (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value`, k, val)
			if err != nil {
				return fmt.Errorf("failed to save state for key %s: %w", k, err)
			}
		}
		return nil
	})
}

func (p *Default) LoadState(ctx context.Context) (map[string]any, error) {
	rows, err := p.queryWithTransaction(ctx, `SELECT key, value FROM state`)
	if err != nil {
		return nil, err
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	state := make(map[string]any)
	for rows.Next() {
		var k string
		var val []byte
		if err := rows.Scan(&k, &val); err != nil {
			return nil, err
		}
		var v any
		json.Unmarshal(val, &v)
		state[k] = v
	}
	return state, nil
}

func (p *Default) DeleteState(ctx context.Context, key string) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM state WHERE key=$1`, key)
		if err != nil {
			return fmt.Errorf("failed to delete state: %w", err)
		}
		return nil
	})
}

// SavePosition saves a position to the database and returns the generated ID
func (p *Default) SavePosition(ctx context.Context, pos Position) (int64, error) {
	var id int64
	err := p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		// Marshal JSON fields
		liveWinPnls, err := json.Marshal(pos.LiveWinPnls)
		if err != nil {
			return fmt.Errorf("failed to marshal live_win_pnls: %w", err)
		}

		liveLossPnls, err := json.Marshal(pos.LiveLossPnls)
		if err != nil {
			return fmt.Errorf("failed to marshal live_loss_pnls: %w", err)
		}

		liveEquityCurve, err := json.Marshal(pos.LiveEquityCurve)
		if err != nil {
			return fmt.Errorf("failed to marshal live_equity_curve: %w", err)
		}

		liveTradeLog, err := json.Marshal(pos.LiveTradeLog)
		if err != nil {
			return fmt.Errorf("failed to marshal live_trade_log: %w", err)
		}

		entryLegs, err := json.Marshal(pos.EntryLegs)
		if err != nil {
			return fmt.Errorf("failed to marshal entry_legs: %w", err)
		}

		riskParams, err := json.Marshal(pos.RiskParams)
		if err != nil {
			return fmt.Errorf("failed to marshal risk_params: %w", err)
		}

		orderSpec, err := json.Marshal(pos.OrderSpec)
		if err != nil {
			return fmt.Errorf("failed to marshal order_spec: %w", err)
		}

		now := time.Now()
		err = tx.QueryRowContext(ctx, `
			INSERT INTO positions (
				strategy_name, symbol, side, entry, size, order_id, time, active, trading_disabled,
				balance, last_pnl, mean_pnl, std_pnl, sharpe, expectancy, trailing_stop,
				live_equity, live_max_equity, live_max_drawdown, live_wins, live_losses, live_trades,
				live_win_rate, profit_factor, live_win_pnls, live_loss_pnls, live_equity_curve,
				live_trade_log, entry_legs, risk_params, order_spec, created_at, updated_at
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
				$17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33
			) RETURNING id`,
			pos.StrategyName, pos.Symbol, pos.Side, pos.Entry, pos.Size, pos.OrderID, pos.Time,
			pos.Active, pos.TradingDisabled, pos.Balance, pos.LastPNL, pos.MeanPNL, pos.StdPNL,
			pos.Sharpe, pos.Expectancy, pos.TrailingStop, pos.LiveEquity, pos.LiveMaxEquity,
			pos.LiveMaxDrawdown, pos.LiveWins, pos.LiveLosses, pos.LiveTrades, pos.LiveWinRate,
			pos.ProfitFactor, liveWinPnls, liveLossPnls, liveEquityCurve, liveTradeLog, entryLegs,
			riskParams, orderSpec, now, now).Scan(&id)
		if err != nil {
			return fmt.Errorf("failed to save position [%s %s]: %w", pos.StrategyName, pos.Symbol, err)
		}
		return nil
	})
	return id, err
}

// UpdatePosition updates an existing position in the database by ID
func (p *Default) UpdatePosition(ctx context.Context, pos Position) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		// Marshal JSON fields
		liveWinPnls, err := json.Marshal(pos.LiveWinPnls)
		if err != nil {
			return fmt.Errorf("failed to marshal live_win_pnls: %w", err)
		}

		liveLossPnls, err := json.Marshal(pos.LiveLossPnls)
		if err != nil {
			return fmt.Errorf("failed to marshal live_loss_pnls: %w", err)
		}

		liveEquityCurve, err := json.Marshal(pos.LiveEquityCurve)
		if err != nil {
			return fmt.Errorf("failed to marshal live_equity_curve: %w", err)
		}

		liveTradeLog, err := json.Marshal(pos.LiveTradeLog)
		if err != nil {
			return fmt.Errorf("failed to marshal live_trade_log: %w", err)
		}

		entryLegs, err := json.Marshal(pos.EntryLegs)
		if err != nil {
			return fmt.Errorf("failed to marshal entry_legs: %w", err)
		}

		riskParams, err := json.Marshal(pos.RiskParams)
		if err != nil {
			return fmt.Errorf("failed to marshal risk_params: %w", err)
		}

		orderSpec, err := json.Marshal(pos.OrderSpec)
		if err != nil {
			return fmt.Errorf("failed to marshal order_spec: %w", err)
		}

		now := time.Now()
		result, err := tx.ExecContext(ctx, `
			UPDATE positions SET
				side=$1, entry=$2, size=$3, order_id=$4, time=$5, active=$6, trading_disabled=$7,
				balance=$8, last_pnl=$9, mean_pnl=$10, std_pnl=$11, sharpe=$12, expectancy=$13,
				trailing_stop=$14, live_equity=$15, live_max_equity=$16, live_max_drawdown=$17,
				live_wins=$18, live_losses=$19, live_trades=$20, live_win_rate=$21, profit_factor=$22,
				live_win_pnls=$23, live_loss_pnls=$24, live_equity_curve=$25, live_trade_log=$26,
				entry_legs=$27, risk_params=$28, order_spec=$29, updated_at=$30
			WHERE id=$31`,
			pos.Side, pos.Entry, pos.Size, pos.OrderID, pos.Time, pos.Active, pos.TradingDisabled,
			pos.Balance, pos.LastPNL, pos.MeanPNL, pos.StdPNL, pos.Sharpe, pos.Expectancy,
			pos.TrailingStop, pos.LiveEquity, pos.LiveMaxEquity, pos.LiveMaxDrawdown,
			pos.LiveWins, pos.LiveLosses, pos.LiveTrades, pos.LiveWinRate, pos.ProfitFactor,
			liveWinPnls, liveLossPnls, liveEquityCurve, liveTradeLog, entryLegs, riskParams, orderSpec, now,
			pos.ID)
		if err != nil {
			return fmt.Errorf("failed to update position [ID %d]: %w", pos.ID, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("no position found to update for ID %d", pos.ID)
		}

		return nil
	})
}

// GetPosition retrieves a position from the database
func (p *Default) GetPosition(ctx context.Context, strategyName, symbol string) (*Position, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT id, strategy_name, symbol, side, entry, size, order_id, time, active, trading_disabled,
			balance, last_pnl, mean_pnl, std_pnl, sharpe, expectancy, trailing_stop,
			live_equity, live_max_equity, live_max_drawdown, live_wins, live_losses, live_trades,
			live_win_rate, profit_factor, live_win_pnls, live_loss_pnls, live_equity_curve,
			live_trade_log, entry_legs, risk_params, order_spec, created_at, updated_at
		FROM positions WHERE strategy_name=$1 AND symbol=$2 AND active=true`,
		strategyName, symbol)
	if err != nil {
		return nil, fmt.Errorf("failed to query position: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var pos Position
	if rows.Next() {
		var liveWinPnls, liveLossPnls, liveEquityCurve, liveTradeLog, entryLegs, riskParams, orderSpec []byte
		if err := rows.Scan(
			&pos.ID, &pos.StrategyName, &pos.Symbol, &pos.Side, &pos.Entry, &pos.Size, &pos.OrderID, &pos.Time,
			&pos.Active, &pos.TradingDisabled, &pos.Balance, &pos.LastPNL, &pos.MeanPNL, &pos.StdPNL,
			&pos.Sharpe, &pos.Expectancy, &pos.TrailingStop, &pos.LiveEquity, &pos.LiveMaxEquity,
			&pos.LiveMaxDrawdown, &pos.LiveWins, &pos.LiveLosses, &pos.LiveTrades, &pos.LiveWinRate,
			&pos.ProfitFactor, &liveWinPnls, &liveLossPnls, &liveEquityCurve, &liveTradeLog, &entryLegs,
			&riskParams, &orderSpec, &pos.CreatedAt, &pos.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan position: %w", err)
		}

		// Unmarshal JSON fields
		if err := json.Unmarshal(liveWinPnls, &pos.LiveWinPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_win_pnls: %w", err)
		}
		if err := json.Unmarshal(liveLossPnls, &pos.LiveLossPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_loss_pnls: %w", err)
		}
		if err := json.Unmarshal(liveEquityCurve, &pos.LiveEquityCurve); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_equity_curve: %w", err)
		}
		if err := json.Unmarshal(liveTradeLog, &pos.LiveTradeLog); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_trade_log: %w", err)
		}
		if err := json.Unmarshal(entryLegs, &pos.EntryLegs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry_legs: %w", err)
		}
		if err := json.Unmarshal(riskParams, &pos.RiskParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal risk_params: %w", err)
		}
		if err := json.Unmarshal(orderSpec, &pos.OrderSpec); err != nil {
			return nil, fmt.Errorf("failed to unmarshal order_spec: %w", err)
		}

		// Convert timestamps to UTC
		pos.Time = pos.Time.UTC()
		pos.CreatedAt = pos.CreatedAt.UTC()
		pos.UpdatedAt = pos.UpdatedAt.UTC()

		return &pos, nil
	}

	return nil, nil
}

// GetPositionByID retrieves a position from the database by ID
func (p *Default) GetPositionByID(ctx context.Context, id int64) (*Position, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT id, strategy_name, symbol, side, entry, size, order_id, time, active, trading_disabled,
			balance, last_pnl, mean_pnl, std_pnl, sharpe, expectancy, trailing_stop,
			live_equity, live_max_equity, live_max_drawdown, live_wins, live_losses, live_trades,
			live_win_rate, profit_factor, live_win_pnls, live_loss_pnls, live_equity_curve,
			live_trade_log, entry_legs, risk_params, order_spec, created_at, updated_at
		FROM positions WHERE id=$1`,
		id)
	if err != nil {
		return nil, fmt.Errorf("failed to query position by ID: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var pos Position
	if rows.Next() {
		var liveWinPnls, liveLossPnls, liveEquityCurve, liveTradeLog, entryLegs, riskParams, orderSpec []byte
		if err := rows.Scan(
			&pos.ID, &pos.StrategyName, &pos.Symbol, &pos.Side, &pos.Entry, &pos.Size, &pos.OrderID, &pos.Time,
			&pos.Active, &pos.TradingDisabled, &pos.Balance, &pos.LastPNL, &pos.MeanPNL, &pos.StdPNL,
			&pos.Sharpe, &pos.Expectancy, &pos.TrailingStop, &pos.LiveEquity, &pos.LiveMaxEquity,
			&pos.LiveMaxDrawdown, &pos.LiveWins, &pos.LiveLosses, &pos.LiveTrades, &pos.LiveWinRate,
			&pos.ProfitFactor, &liveWinPnls, &liveLossPnls, &liveEquityCurve, &liveTradeLog, &entryLegs,
			&riskParams, &orderSpec, &pos.CreatedAt, &pos.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan position: %w", err)
		}
		if err := json.Unmarshal(liveWinPnls, &pos.LiveWinPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_win_pnls: %w", err)
		}
		if err := json.Unmarshal(liveLossPnls, &pos.LiveLossPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_loss_pnls: %w", err)
		}
		if err := json.Unmarshal(liveEquityCurve, &pos.LiveEquityCurve); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_equity_curve: %w", err)
		}
		if err := json.Unmarshal(liveTradeLog, &pos.LiveTradeLog); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_trade_log: %w", err)
		}
		if err := json.Unmarshal(entryLegs, &pos.EntryLegs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry_legs: %w", err)
		}
		if err := json.Unmarshal(riskParams, &pos.RiskParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal risk_params: %w", err)
		}
		if err := json.Unmarshal(orderSpec, &pos.OrderSpec); err != nil {
			return nil, fmt.Errorf("failed to unmarshal order_spec: %w", err)
		}
		pos.Time = pos.Time.UTC()
		pos.CreatedAt = pos.CreatedAt.UTC()
		pos.UpdatedAt = pos.UpdatedAt.UTC()
		return &pos, nil
	}
	return nil, nil
}

// GetAllPositions retrieves all positions from the database
func (p *Default) GetAllPositions(ctx context.Context) ([]Position, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT id, strategy_name, symbol, side, entry, size, order_id, time, active, trading_disabled,
			balance, last_pnl, mean_pnl, std_pnl, sharpe, expectancy, trailing_stop,
			live_equity, live_max_equity, live_max_drawdown, live_wins, live_losses, live_trades,
			live_win_rate, profit_factor, live_win_pnls, live_loss_pnls, live_equity_curve,
			live_trade_log, entry_legs, risk_params, order_spec, created_at, updated_at
		FROM positions ORDER BY strategy_name, symbol`)
	if err != nil {
		return nil, fmt.Errorf("failed to query positions: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var positions []Position
	for rows.Next() {
		var pos Position
		var liveWinPnls, liveLossPnls, liveEquityCurve, liveTradeLog, entryLegs, riskParams, orderSpec []byte
		if err := rows.Scan(
			&pos.ID, &pos.StrategyName, &pos.Symbol, &pos.Side, &pos.Entry, &pos.Size, &pos.OrderID, &pos.Time,
			&pos.Active, &pos.TradingDisabled, &pos.Balance, &pos.LastPNL, &pos.MeanPNL, &pos.StdPNL,
			&pos.Sharpe, &pos.Expectancy, &pos.TrailingStop, &pos.LiveEquity, &pos.LiveMaxEquity,
			&pos.LiveMaxDrawdown, &pos.LiveWins, &pos.LiveLosses, &pos.LiveTrades, &pos.LiveWinRate,
			&pos.ProfitFactor, &liveWinPnls, &liveLossPnls, &liveEquityCurve, &liveTradeLog, &entryLegs,
			&riskParams, &orderSpec, &pos.CreatedAt, &pos.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan position: %w", err)
		}
		if err := json.Unmarshal(liveWinPnls, &pos.LiveWinPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_win_pnls: %w", err)
		}
		if err := json.Unmarshal(liveLossPnls, &pos.LiveLossPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_loss_pnls: %w", err)
		}
		if err := json.Unmarshal(liveEquityCurve, &pos.LiveEquityCurve); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_equity_curve: %w", err)
		}
		if err := json.Unmarshal(liveTradeLog, &pos.LiveTradeLog); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_trade_log: %w", err)
		}
		if err := json.Unmarshal(entryLegs, &pos.EntryLegs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry_legs: %w", err)
		}
		if err := json.Unmarshal(riskParams, &pos.RiskParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal risk_params: %w", err)
		}
		if err := json.Unmarshal(orderSpec, &pos.OrderSpec); err != nil {
			return nil, fmt.Errorf("failed to unmarshal order_spec: %w", err)
		}
		pos.Time = pos.Time.UTC()
		pos.CreatedAt = pos.CreatedAt.UTC()
		pos.UpdatedAt = pos.UpdatedAt.UTC()
		positions = append(positions, pos)
	}
	return positions, nil
}

// GetActivePositions retrieves all active positions from the database
func (p *Default) GetActivePositions(ctx context.Context) ([]Position, error) {
	rows, err := p.queryWithTransaction(ctx, `
		SELECT id, strategy_name, symbol, side, entry, size, order_id, time, active, trading_disabled,
			balance, last_pnl, mean_pnl, std_pnl, sharpe, expectancy, trailing_stop,
			live_equity, live_max_equity, live_max_drawdown, live_wins, live_losses, live_trades,
			live_win_rate, profit_factor, live_win_pnls, live_loss_pnls, live_equity_curve,
			live_trade_log, entry_legs, risk_params, order_spec, created_at, updated_at
		FROM positions WHERE active=true ORDER BY strategy_name, symbol`)
	if err != nil {
		return nil, fmt.Errorf("failed to query active positions: %w", err)
	}
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var positions []Position
	for rows.Next() {
		var pos Position
		var liveWinPnls, liveLossPnls, liveEquityCurve, liveTradeLog, entryLegs, riskParams, orderSpec []byte
		if err := rows.Scan(
			&pos.ID, &pos.StrategyName, &pos.Symbol, &pos.Side, &pos.Entry, &pos.Size, &pos.OrderID, &pos.Time,
			&pos.Active, &pos.TradingDisabled, &pos.Balance, &pos.LastPNL, &pos.MeanPNL, &pos.StdPNL,
			&pos.Sharpe, &pos.Expectancy, &pos.TrailingStop, &pos.LiveEquity, &pos.LiveMaxEquity,
			&pos.LiveMaxDrawdown, &pos.LiveWins, &pos.LiveLosses, &pos.LiveTrades, &pos.LiveWinRate,
			&pos.ProfitFactor, &liveWinPnls, &liveLossPnls, &liveEquityCurve, &liveTradeLog, &entryLegs,
			&riskParams, &orderSpec, &pos.CreatedAt, &pos.UpdatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan position: %w", err)
		}
		if err := json.Unmarshal(liveWinPnls, &pos.LiveWinPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_win_pnls: %w", err)
		}
		if err := json.Unmarshal(liveLossPnls, &pos.LiveLossPnls); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_loss_pnls: %w", err)
		}
		if err := json.Unmarshal(liveEquityCurve, &pos.LiveEquityCurve); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_equity_curve: %w", err)
		}
		if err := json.Unmarshal(liveTradeLog, &pos.LiveTradeLog); err != nil {
			return nil, fmt.Errorf("failed to unmarshal live_trade_log: %w", err)
		}
		if err := json.Unmarshal(entryLegs, &pos.EntryLegs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal entry_legs: %w", err)
		}
		if err := json.Unmarshal(riskParams, &pos.RiskParams); err != nil {
			return nil, fmt.Errorf("failed to unmarshal risk_params: %w", err)
		}
		if err := json.Unmarshal(orderSpec, &pos.OrderSpec); err != nil {
			return nil, fmt.Errorf("failed to unmarshal order_spec: %w", err)
		}
		pos.Time = pos.Time.UTC()
		pos.CreatedAt = pos.CreatedAt.UTC()
		pos.UpdatedAt = pos.UpdatedAt.UTC()
		positions = append(positions, pos)
	}
	return positions, nil
}

// DeletePosition deletes a position from the database
func (p *Default) DeletePosition(ctx context.Context, strategyName, symbol string) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM positions WHERE strategy_name=$1 AND symbol=$2`,
			strategyName, symbol)
		if err != nil {
			return fmt.Errorf("failed to delete position: %w", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("no position found to delete for [%s %s]", strategyName, symbol)
		}

		return nil
	})
}

// DeletePositionByID deletes a position from the database by ID
func (p *Default) DeletePositionByID(ctx context.Context, id int64) error {
	return p.executeWithTransaction(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `DELETE FROM positions WHERE id=$1`, id)
		if err != nil {
			return fmt.Errorf("failed to delete position [ID %d]: %w", id, err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if rowsAffected == 0 {
			return fmt.Errorf("no position found to delete for ID %d", id)
		}

		return nil
	})
}
