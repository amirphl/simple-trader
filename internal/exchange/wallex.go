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

	"github.com/amirphl/simple-trader/internal/candle"
	"github.com/amirphl/simple-trader/internal/market"
	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/order"
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

func (w *WallexExchange) FetchCandles(ctx context.Context, symbol string, timeframe string, start, end time.Time) ([]candle.Candle, error) {
	// Validate timeframe
	if !candle.IsValidTimeframe(timeframe) {
		return nil, fmt.Errorf("unsupported timeframe: %s", timeframe)
	}

	trimmedTimeframe := strings.TrimSuffix(timeframe, "m")

	symbolNoHyphen := strings.ReplaceAll(symbol, "-", "")
	uppercasedSymbol := strings.ToUpper(symbolNoHyphen)

	var wallexCandles []*wallex.Candle

	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s FetchCandles timeout", w.Name())
		return nil, ctx.Err()

	default:
		err := retry(3, 2*time.Second, func() error {
			var err error
			wallexCandles, err = w.client.Candles(uppercasedSymbol, trimmedTimeframe, start, end)
			if err != nil {
				return fmt.Errorf("fetching candles: %w", err)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("FetchCandles failed: %w", err)
		}
	}

	var candles []candle.Candle
	for _, wc := range wallexCandles {
		open, _ := strconv.ParseFloat(string(wc.Open), 64)
		high, _ := strconv.ParseFloat(string(wc.High), 64)
		low, _ := strconv.ParseFloat(string(wc.Low), 64)
		close, _ := strconv.ParseFloat(string(wc.Close), 64)
		volume, _ := strconv.ParseFloat(string(wc.Volume), 64)

		c := candle.Candle{
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
func (w *WallexExchange) FetchLatestCandles(ctx context.Context, symbol string, timeframe string, count int) ([]candle.Candle, error) {
	end := time.Now().UTC()
	duration := candle.GetTimeframeDuration(timeframe)
	if duration == 0 {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}

	// Calculate start time based on count and timeframe
	start := end.Add(-duration * time.Duration(count))

	return w.FetchCandles(ctx, symbol, timeframe, start, end)
}

func (w *WallexExchange) FetchOrderBook(ctx context.Context, symbol string) (market.OrderBook, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s FetchOrderBook timeout", w.Name())
		return market.OrderBook{}, ctx.Err()

	default:
		symbolNoHyphen := strings.ReplaceAll(symbol, "-", "")
		uppercasedSymbol := strings.ToUpper(symbolNoHyphen)

		asks, bids, err := w.client.MarketOrders(uppercasedSymbol)
		if err != nil {
			return market.OrderBook{}, err
		}
		var ob market.OrderBook
		ob.Symbol = symbol
		ob.Timestamp = time.Now().UTC()
		for _, a := range asks {
			price, _ := strconv.ParseFloat(string(a.Price), 64)
			qty, _ := strconv.ParseFloat(string(a.Quantity), 64)
			ob.Asks = append(ob.Asks, [2]float64{price, qty})
		}
		for _, b := range bids {
			price, _ := strconv.ParseFloat(string(b.Price), 64)
			qty, _ := strconv.ParseFloat(string(b.Quantity), 64)
			ob.Bids = append(ob.Bids, [2]float64{price, qty})
		}
		return ob, nil
	}
}

func (w *WallexExchange) FetchTick(ctx context.Context, symbol string) (market.Tick, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s FetchTick timeout", w.Name())
		return market.Tick{}, ctx.Err()

	default:
		symbolNoHyphen := strings.ReplaceAll(symbol, "-", "")
		uppercasedSymbol := strings.ToUpper(symbolNoHyphen)

		trades, err := w.client.MarketTrades(uppercasedSymbol)
		if err != nil || len(trades) == 0 {
			return market.Tick{}, err
		}
		last := trades[len(trades)-1]
		price, _ := strconv.ParseFloat(string(last.Price), 64)
		qty, _ := strconv.ParseFloat(string(last.Quantity), 64)

		return market.Tick{
			Symbol:    symbol,
			Price:     price,
			Quantity:  qty,
			Side:      "", // Wallex may not provide side; TODO: map if available
			Timestamp: last.Timestamp.UTC(),
		}, nil
	}
}

func (w *WallexExchange) FetchTicks(ctx context.Context, symbol string, from, to time.Time) ([]market.Tick, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange %s FetchTicks timeout", w.Name())
		return nil, ctx.Err()

	default:
		symbolNoHyphen := strings.ReplaceAll(symbol, "-", "")
		uppercasedSymbol := strings.ToUpper(symbolNoHyphen)

		trades, err := w.client.MarketTrades(uppercasedSymbol)
		if err != nil {
			return nil, err
		}
		var ticks []market.Tick
		for _, t := range trades {
			if t.Timestamp.UTC().Before(from) || t.Timestamp.UTC().After(to) {
				continue
			}
			price, _ := strconv.ParseFloat(string(t.Price), 64)
			qty, _ := strconv.ParseFloat(string(t.Quantity), 64)
			ticks = append(ticks, market.Tick{
				Symbol:    symbol,
				Price:     price,
				Quantity:  qty,
				Side:      "", // Wallex may not provide side; TODO: map if available
				Timestamp: t.Timestamp.UTC(),
			})
		}
		return ticks, nil
	}
}

func (w *WallexExchange) SubmitOrder(ctx context.Context, req order.OrderRequest) (order.OrderResponse, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s SubmitOrder timeout", w.Name())
		return order.OrderResponse{}, ctx.Err()

	default:
		price := strconv.FormatFloat(req.Price, 'f', 8, 64)
		qty := strconv.FormatFloat(req.Quantity, 'f', 8, 64)

		symbolNoHyphen := strings.ReplaceAll(req.Symbol, "-", "")
		uppercasedSymbol := strings.ToUpper(symbolNoHyphen)
		uppercasedType := strings.ToUpper(req.Type)
		uppercasedSide := strings.ToUpper(req.Side)

		params := &wallex.OrderParams{
			Symbol:   uppercasedSymbol,
			Type:     uppercasedType,
			Side:     uppercasedSide,
			Price:    wallex.Number(price),
			Quantity: wallex.Number(qty),
		}
		resp, err := w.client.PlaceOrder(params)
		if err != nil {
			return order.OrderResponse{}, err
		}

		return order.OrderResponse{
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
		}, nil
	}
}

func (w *WallexExchange) SubmitOrderWithRetry(ctx context.Context, req order.OrderRequest, maxAttempts int, delay time.Duration) (order.OrderResponse, error) {
	var resp order.OrderResponse
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

func (w *WallexExchange) GetOrderStatus(ctx context.Context, orderID string) (order.OrderResponse, error) {
	select {
	case <-ctx.Done():
		log.Printf("Exchange | %s GetOrderStatus timeout", w.Name())
		return order.OrderResponse{}, ctx.Err()

	default:
		resp, err := w.client.Order(orderID)
		if err != nil {
			return order.OrderResponse{}, err
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

		return order.OrderResponse{
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

// Helper to safely dereference *wallex.Number
func float64Ptr(n *wallex.Number) float64 {
	if n == nil {
		return 0
	}
	out, _ := strconv.ParseFloat(string(*n), 64)
	return out
}
