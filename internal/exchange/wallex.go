// Package exchange
package exchange

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/amirphl/simple-trader/internal/notifier"
	"github.com/amirphl/simple-trader/internal/tfutils"
	"github.com/amirphl/simple-trader/internal/utils"
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
		utils.GetLogger().Printf("Exchange | %s Retry attempt %d/%d failed: %v. Backing off for %v", "Wallex", i, attempts, err, backoff)
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
		utils.GetLogger().Printf("Exchange | %s FetchCandles timeout", w.Name())
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
		utils.GetLogger().Printf("Exchange | %s SubmitOrder timeout", w.Name())
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
		utils.GetLogger().Printf("Exchange | %s Order submission failed (attempt %d/%d): %v", w.Name(), attempt, maxAttempts, err)
		msg := fmt.Sprintf("Order submission failed (attempt %d/%d): %v", attempt, maxAttempts, err)
		w.notifier.SendWithRetry(msg)
		time.Sleep(delay)
	}
	return resp, err
}

func (w *WallexExchange) CancelOrder(ctx context.Context, orderID string) error {
	select {
	case <-ctx.Done():
		utils.GetLogger().Printf("Exchange | %s CancelOrder timeout", w.Name())
		return ctx.Err()

	default:
		return w.client.CancelOrder(orderID)
	}
}

func (w *WallexExchange) GetOrderStatus(ctx context.Context, orderID string) (Order, error) {
	select {
	case <-ctx.Done():
		utils.GetLogger().Printf("Exchange | %s GetOrderStatus timeout", w.Name())
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
		utils.GetLogger().Printf("Exchange | %s FetchBalances timeout", w.Name())
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

// FetchOrderBook retrieves the current orderbook for a symbol
func (w *WallexExchange) FetchOrderBook(ctx context.Context, symbol string) (map[string]OrderBook, error) {
	select {
	case <-ctx.Done():
		utils.GetLogger().Printf("Exchange | %s FetchOrderBook timeout", w.Name())
		return nil, ctx.Err()

	default:
		var asks []*wallex.MarketOrder
		var bids []*wallex.MarketOrder
		err := retry(3, 2*time.Second, func() error {
			var err error
			normalizedSymbol := NormalizeSymbol(symbol)
			asks, bids, err = w.client.MarketOrders(normalizedSymbol)
			if err != nil {
				return fmt.Errorf("fetching orderbook: %w", err)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("orderbook failed: %w", err)
		}

		orderbook := make(map[string]OrderBook)

		// Process asks (sell orders) - sellers want to sell at these prices
		sellOrderBook := make(OrderBook)
		for _, ask := range asks {
			price := string(ask.Price)
			quantity := float64Ptr(&ask.Quantity)
			sum := float64Ptr(&ask.Sum)

			sellOrderBook[price] = OrderBookEntry{
				Quantity: quantity,
				Price:    price,
				Sum:      sum,
			}
		}
		orderbook[GetSellDepthKey(symbol)] = sellOrderBook

		// Process bids (buy orders) - buyers want to buy at these prices
		buyOrderBook := make(OrderBook)
		for _, bid := range bids {
			price := string(bid.Price)
			quantity := float64Ptr(&bid.Quantity)
			sum := float64Ptr(&bid.Sum)

			buyOrderBook[price] = OrderBookEntry{
				Quantity: quantity,
				Price:    price,
				Sum:      sum,
			}
		}
		orderbook[GetBuyDepthKey(symbol)] = buyOrderBook

		return orderbook, nil
	}
}

// FetchLatestTick fetches the latest tick for a symbol
func (w *WallexExchange) FetchLatestTick(ctx context.Context, symbol string) (Tick, error) {
	select {
	case <-ctx.Done():
		utils.GetLogger().Printf("Exchange | %s FetchLatestTick timeout", w.Name())
		return Tick{}, ctx.Err()

	default:
		var trades []*wallex.MarketTrade
		err := retry(3, 2*time.Second, func() error {
			var err error
			normalizedSymbol := NormalizeSymbol(symbol)
			trades, err = w.client.MarketTrades(normalizedSymbol)
			if err != nil {
				return fmt.Errorf("fetching latest tick: %w", err)
			}
			return nil
		})
		if err != nil {
			return Tick{}, fmt.Errorf("latest tick failed: %w", err)
		}

		if len(trades) == 0 {
			return Tick{}, fmt.Errorf("no trades found for symbol: %s", symbol)
		}

		trade := trades[0]
		return Tick{
			Symbol:    symbol,
			Price:     float64Ptr(&trade.Price),
			Quantity:  float64Ptr(&trade.Quantity),
			Side:      "", // TODO: Get side from trade
			Timestamp: trade.Timestamp.UTC(),
		}, nil
	}
}

// FetchMarketStats fetches the market stats
func (w *WallexExchange) FetchMarketStats(ctx context.Context) (map[string]MarketCap, error) {
	select {
	case <-ctx.Done():
		utils.GetLogger().Printf("Exchange | %s FetchMarketStats timeout", w.Name())
		return nil, ctx.Err()

	default:
		var markets []*wallex.Market
		err := retry(3, 2*time.Second, func() error {
			var err error
			markets, err = w.client.Markets()
			if err != nil {
				return fmt.Errorf("fetching market stats: %w", err)
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("market stats failed: %w", err)
		}

		if len(markets) == 0 {
			return nil, fmt.Errorf("no markets found")
		}

		marketStats := make(map[string]MarketCap)
		for _, market := range markets {
			marketStats[market.Symbol] = MarketCap{
				Symbol:         market.Symbol,
				BidPrice:       string(market.Stats.BidPrice),
				AskPrice:       string(market.Stats.AskPrice),
				Ch24h:          float64Ptr(&market.Stats.Change24H),
				Ch7d:           float64Ptr(&market.Stats.Change7D),
				Volume24h:      string(market.Stats.Volume24H),
				Volume7d:       string(market.Stats.Volume7D),
				QuoteVolume24h: string(market.Stats.QuoteVolume24H),
				HighPrice24h:   string(market.Stats.HighPrice24H),
				LowPrice24h:    string(market.Stats.LowPrice24H),
				LastPrice:      string(market.Stats.LastPrice),
				LastQty:        string(market.Stats.LastQty),
				LastTradeSide:  market.Stats.LastTradeSide,
				BidVolume:      string(market.Stats.BidVolume),
				AskVolume:      string(market.Stats.AskVolume),
				BidCount:       int(float64Ptr(&market.Stats.BidCount)),
				AskCount:       int(float64Ptr(&market.Stats.AskCount)),
				Direction: struct {
					SELL int "json:\"SELL\""
					BUY  int "json:\"BUY\""
				}{
					SELL: market.Stats.Direction.Sell,
					BUY:  market.Stats.Direction.Buy,
				},
				CreatedAt: market.CreatedAt.UTC().Format(time.RFC3339),
			}
		}

		return marketStats, nil
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
