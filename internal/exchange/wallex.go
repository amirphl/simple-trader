// Package exchange
package exchange

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/tfutils"
	wallex "github.com/wallexchange/wallex-go"
)

// TODO: Use context

type WallexExchange struct {
	client   *wallex.Client
	notifier notifier.Notifier
}

func NewWallexExchange(apiKey string, n notifier.Notifier) Exchange {
	return &WallexExchange{
		client:   wallex.New(wallex.ClientOptions{APIKey: apiKey}),
		notifier: n,
	}
}

func (w *WallexExchange) Name() string {
	return "wallex"
}

// retry wraps a function with retry logic for transient errors, using exponential backoff and error logging.
func retry(attempts int, delay time.Duration, fn func() error) error {
	backoff := delay
	for i := 1; i <= attempts; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		log.Printf("Exchange | %s Retry attempt %d/%d failed: %v. Backing off for %v", "Wallex", i, attempts, err, backoff)
		time.Sleep(backoff)
		// Exponential backoff, but cap at 5 minutes
		if backoff < 5*time.Minute {
			backoff *= 2
			if backoff > 5*time.Minute {
				backoff = 5 * time.Minute
			}
		}
	}
	return errors.New("all retry attempts failed")
}

func (w *WallexExchange) FetchCandles(ctx context.Context, symbol string, timeframe string, start, end time.Time) ([]Candle, error) {
	// Validate timeframe
	if !tfutils.IsValidTimeframe(timeframe) {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}

	normalizedTimeframe := NormalizedTimeframe(timeframe)
	normalizedSymbol := NormalizeSymbol(symbol)

	var wallexCandles []*wallex.Candle

	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s FetchCandles timeout", w.Name())
		return nil, ctx.Err()

	default:
		err := retry(3, 2*time.Second, func() error {
			var err error
			wallexCandles, err = w.client.Candles(normalizedSymbol, normalizedTimeframe, start, end)
			if err != nil {
				return fmt.Errorf("fetching candles: %w", err)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("FetchCandles failed: %w", err)
		}
	}

	var candles []Candle
	for _, wc := range wallexCandles {
		open, _ := strconv.ParseFloat(string(wc.Open), 64)
		high, _ := strconv.ParseFloat(string(wc.High), 64)
		low, _ := strconv.ParseFloat(string(wc.Low), 64)
		close, _ := strconv.ParseFloat(string(wc.Close), 64)
		volume, _ := strconv.ParseFloat(string(wc.Volume), 64)

		c := Candle{
			Timestamp: wc.Timestamp.UTC().Truncate(time.Minute),
			Open:      open,
			High:      high,
			Low:       low,
			Close:     close,
			Volume:    volume,
			Symbol:    symbol,
			Timeframe: timeframe,
			Source:    w.Name(),
		}

		// ISSUE: Side effect on strategies
		// Validate candle before adding
		if err := c.Validate(); err != nil {
			continue // Skip invalid candles
		}

		candles = append(candles, c)
	}

	return candles, nil
}

// FetchLatestCandles fetches the most recent candles for a symbol and timeframe
func (w *WallexExchange) FetchLatestCandles(ctx context.Context, symbol string, timeframe string, count int) ([]Candle, error) {
	end := time.Now().UTC()
	duration := tfutils.GetTimeframeDuration(timeframe)
	if duration == 0 {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}

	// Calculate start time based on count and timeframe
	start := end.Add(-duration * time.Duration(count))

	return w.FetchCandles(ctx, symbol, timeframe, start, end)
}

func (w *WallexExchange) SubmitOrder(ctx context.Context, req OrderRequest) (Order, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s SubmitOrder timeout", w.Name())
		return Order{}, ctx.Err()

	default:
		price := strconv.FormatFloat(req.Price, 'f', 8, 64)
		qty := strconv.FormatFloat(req.Quantity, 'f', 8, 64)

		normalizedSymbol := NormalizeSymbol(req.Symbol)
		uppercasedType := strings.ToUpper(req.Type)
		uppercasedSide := strings.ToUpper(req.Side)

		params := &wallex.OrderParams{
			Symbol:   normalizedSymbol,
			Type:     uppercasedType,
			Side:     uppercasedSide,
			Price:    wallex.Number(price),
			Quantity: wallex.Number(qty),
		}
		resp, err := w.client.PlaceOrder(params)
		if err != nil {
			return Order{}, err
		}

		return Order{
			OrderID:   resp.ClientOrderID,
			Status:    strings.ToUpper(resp.Status),
			FilledQty: float64Ptr(resp.ExecutedQty),
			AvgPrice:  float64Ptr(resp.ExecutedPrice),
			Timestamp: resp.CreatedAt.UTC(),
			Symbol:    req.Symbol,
			Side:      req.Side,
			Type:      req.Type,
			Price:     req.Price,
			Quantity:  req.Quantity,
			UpdatedAt: resp.CreatedAt,
		}, nil
	}
}

func (w *WallexExchange) SubmitOrderWithRetry(ctx context.Context, req OrderRequest, maxAttempts int, delay time.Duration) (Order, error) {
	var resp Order
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		resp, err = w.SubmitOrder(ctx, req)
		if err == nil {
			return resp, nil
		}
		log.Printf("Exchange | %s Order submission failed (attempt %d/%d): %v", w.Name(), attempt, maxAttempts, err)
		msg := fmt.Sprintf("Order submission failed (attempt %d/%d): %v", attempt, maxAttempts, err)
		w.notifier.SendWithRetry(msg)
		time.Sleep(delay)
	}
	return resp, err
}

func (w *WallexExchange) CancelOrder(ctx context.Context, orderID string) error {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s CancelOrder timeout", w.Name())
		return ctx.Err()

	default:
		return w.client.CancelOrder(orderID)
	}
}

func (w *WallexExchange) GetOrderStatus(ctx context.Context, orderID string) (Order, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s GetOrderStatus timeout", w.Name())
		return Order{}, ctx.Err()

	default:
		resp, err := w.client.Order(orderID)
		if err != nil {
			return Order{}, err
		}

		symbol := ""
		if strings.HasSuffix(resp.Symbol, "USDT") {
			symbol = strings.ReplaceAll(resp.Symbol, "USDT", "-USDT")
		} else if strings.HasSuffix(resp.Symbol, "TMN") {
			symbol = strings.ReplaceAll(resp.Symbol, "TMN", "-TMN")
		} else {
			// replace last three characters with hyphen
			symbol = resp.Symbol[:len(resp.Symbol)-3] + "-" + resp.Symbol[len(resp.Symbol)-3:]
		}
		lowercasedSide := strings.ToLower(resp.Side)
		lowercasedType := strings.ToLower(resp.Type)

		return Order{
			OrderID:   resp.ClientOrderID,
			Status:    strings.ToUpper(resp.Status),
			FilledQty: float64Ptr(resp.ExecutedQty),
			AvgPrice:  float64Ptr(resp.ExecutedPrice),
			Timestamp: resp.CreatedAt.UTC(),
			Symbol:    symbol,
			Side:      lowercasedSide,
			Type:      lowercasedType,
			Price:     float64Ptr(&resp.Price),
			Quantity:  float64Ptr(&resp.OrigQty),
			UpdatedAt: resp.CreatedAt,
		}, nil
	}
}

// FetchBalances retrieves the current balance of all assets from the Wallex exchange
func (w *WallexExchange) FetchBalances(ctx context.Context) (map[string]Balance, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s FetchBalances timeout", w.Name())
		return nil, ctx.Err()

	default:
		var wallexBalances map[string]*wallex.Balance
		err := retry(3, 2*time.Second, func() error {
			var err error
			wallexBalances, err = w.client.Balances()
			if err != nil {
				return fmt.Errorf("fetching balances: %w", err)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("FetchBalances failed: %w", err)
		}

		balances := make(map[string]Balance)
		for asset, wb := range wallexBalances {
			// Parse the balance values from wallex.Number to float64
			available, _ := strconv.ParseFloat(string(wb.Value), 64)
			locked, _ := strconv.ParseFloat(string(wb.Locked), 64)
			total := available + locked

			balances[asset] = Balance{
				Asset:     asset,
				Available: available,
				Locked:    locked,
				Total:     total,
				Fiat:      wb.Fiat,
			}
		}

		return balances, nil
	}
}

// Helper to safely dereference *wallex.Number
func float64Ptr(n *wallex.Number) float64 {
	if n == nil {
		return 0
	}
	out, _ := strconv.ParseFloat(string(*n), 64)
	return out
}
