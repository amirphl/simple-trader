// Package backtest

package backtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/amirphl/simple-trader/internal/config"
	"github.com/amirphl/simple-trader/internal/db"
	"github.com/amirphl/simple-trader/internal/strategy"
)

// BinanceSymbolInfo represents symbol information from Binance ticker API
type BinanceSymbolInfo struct {
	Symbol             string `json:"symbol"`
	PriceChange        string `json:"priceChange"`
	PriceChangePercent string `json:"priceChangePercent"`
	WeightedAvgPrice   string `json:"weightedAvgPrice"`
	PrevClosePrice     string `json:"prevClosePrice"`
	LastPrice          string `json:"lastPrice"`
	LastQty            string `json:"lastQty"`
	BidPrice           string `json:"bidPrice"`
	BidQty             string `json:"bidQty"`
	AskPrice           string `json:"askPrice"`
	AskQty             string `json:"askQty"`
	OpenPrice          string `json:"openPrice"`
	HighPrice          string `json:"highPrice"`
	LowPrice           string `json:"lowPrice"`
	Volume             string `json:"volume"`
	QuoteVolume        string `json:"quoteVolume"`
	OpenTime           int64  `json:"openTime"`
	CloseTime          int64  `json:"closeTime"`
	Count              int64  `json:"count"`
}

// MultiSymbolBacktestResults holds results for multiple symbols
type MultiSymbolBacktestResults struct {
	Results        map[string]BacktestResults `json:"results"`
	OverallMetrics map[string]float64         `json:"overall_metrics"`
	StartTime      time.Time                  `json:"start_time"`
	EndTime        time.Time                  `json:"end_time"`
	Strategy       string                     `json:"strategy"`
	TotalSymbols   int                        `json:"total_symbols"`
	SuccessfulRuns int                        `json:"successful_runs"`
	FailedRuns     int                        `json:"failed_runs"`
}

// fetchTopBinanceSymbols fetches the top N USDT symbols by 24h volume from Binance
func fetchTopBinanceSymbols(ctx context.Context, topN int, proxyURL string) ([]string, error) {
	return fetchTopBinanceSymbolsWithRetry(ctx, topN, proxyURL, 3, 2*time.Second, 15*time.Second)
}

// fetchTopBinanceSymbolsWithRetry fetches symbols with configurable retry parameters
func fetchTopBinanceSymbolsWithRetry(ctx context.Context, topN int, proxyURL string, maxRetries int, baseDelay, maxDelay time.Duration) ([]string, error) {
	const (
		backoffFactor = 2.0
		jitterRange   = 0.1
	)

	apiURL := "https://api.binance.com/api/v3/ticker/24hr"

	// Create HTTP client with optional proxy
	transport := &http.Transport{}
	if proxyURL != "" {
		proxyParsed, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL: %w", err)
		}
		transport.Proxy = http.ProxyURL(proxyParsed)
		log.Printf("fetchTopBinanceSymbols | Using proxy: %s", proxyURL)
	}

	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: transport,
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "application/json")

		log.Printf("fetchTopBinanceSymbols | Attempt %d/%d", attempt+1, maxRetries)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("network error on attempt %d: %w", attempt+1, err)
			log.Printf("fetchTopBinanceSymbols | %v", lastErr)

			if attempt < maxRetries-1 {
				delay := calculateRetryDelay(attempt, baseDelay, maxDelay, backoffFactor, jitterRange)
				log.Printf("fetchTopBinanceSymbols | Retrying in %v...", delay)

				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			continue
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			lastErr = fmt.Errorf("API returned status %d on attempt %d: %s", resp.StatusCode, attempt+1, string(body))
			log.Printf("fetchTopBinanceSymbols | %v", lastErr)

			if isRetryableHTTPStatus(resp.StatusCode) && attempt < maxRetries-1 {
				delay := calculateRetryDelay(attempt, baseDelay, maxDelay, backoffFactor, jitterRange)
				log.Printf("fetchTopBinanceSymbols | Retrying in %v...", delay)

				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			continue
		}

		// Success! Process the response
		var symbols []BinanceSymbolInfo
		if err := json.NewDecoder(resp.Body).Decode(&symbols); err != nil {
			resp.Body.Close()
			lastErr = fmt.Errorf("failed to decode response on attempt %d: %w", attempt+1, err)
			log.Printf("fetchTopBinanceSymbols | %v", lastErr)

			if attempt < maxRetries-1 {
				delay := calculateRetryDelay(attempt, baseDelay, maxDelay, backoffFactor, jitterRange)
				log.Printf("fetchTopBinanceSymbols | Retrying in %v...", delay)

				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(delay):
					continue
				}
			}
			continue
		}
		resp.Body.Close()

		// Filter USDT pairs and sort by quote volume
		var usdtSymbols []BinanceSymbolInfo
		for _, symbol := range symbols {
			exclusionSymbols := []string{"BTC", "USDC"}
			include := true
			for _, exclusion := range exclusionSymbols {
				if strings.HasPrefix(symbol.Symbol, exclusion) {
					include = false
					break
				}
			}

			if include && strings.HasSuffix(symbol.Symbol, "USDT") && symbol.Symbol != "USDT" {
				// Parse quote volume for sorting
				if _, err := strconv.ParseFloat(symbol.QuoteVolume, 64); err == nil {
					usdtSymbols = append(usdtSymbols, symbol)
				}
			}
		}

		// Sort by quote volume (descending)
		sort.Slice(usdtSymbols, func(i, j int) bool {
			volI, _ := strconv.ParseFloat(usdtSymbols[i].QuoteVolume, 64)
			volJ, _ := strconv.ParseFloat(usdtSymbols[j].QuoteVolume, 64)
			return volI > volJ
		})

		// Get top N symbols and convert to our format (SYMBOL-USDT)
		var result []string
		count := topN
		if count > len(usdtSymbols) {
			count = len(usdtSymbols)
		}

		for i := 0; i < count; i++ {
			symbol := usdtSymbols[i].Symbol
			// Convert BTCUSDT to BTC-USDT format
			if len(symbol) > 4 && strings.HasSuffix(symbol, "USDT") {
				base := symbol[:len(symbol)-4]
				result = append(result, base+"-USDT")
			}
		}

		log.Printf("fetchTopBinanceSymbols | Successfully fetched top %d USDT symbols on attempt %d", count, attempt+1)
		return result, nil
	}

	return nil, fmt.Errorf("failed to fetch symbols after %d attempts, last error: %w", maxRetries, lastErr)
}

// runMultiSymbolBacktest runs backtests for multiple symbols and generates comprehensive HTML report
func RunMultiSymbolBacktest(
	ctx context.Context,
	cfg config.Config,
	storage db.Storage,
) {
	if len(cfg.Strategies) == 0 {
		log.Fatal("No strategies specified in config")
	}

	strategyName := cfg.Strategies[0] // Use first strategy
	// Fetch top N symbols from Binance
	log.Printf("Fetching top %d symbols from Binance...", cfg.TopSymbolsCount)
	symbols, err := fetchTopBinanceSymbols(ctx, cfg.TopSymbolsCount, cfg.ProxyURL)
	if err != nil {
		log.Fatalf("Failed to fetch top symbols: %v", err)
	}

	log.Printf("Running backtest for %d symbols: %v", len(symbols), symbols[:min(5, len(symbols))])

	// Initialize multi-symbol results
	multiResults := MultiSymbolBacktestResults{
		Results:        make(map[string]BacktestResults),
		OverallMetrics: make(map[string]float64),
		StartTime:      time.Now(),
		Strategy:       strategyName,
		TotalSymbols:   len(symbols),
	}

	// Run backtest for each symbol
	var allChartData []map[string]any

	for i, symbol := range symbols {
		log.Printf("[%d/%d] Running backtest for symbol: %s", i+1, len(symbols), symbol)

		// Create strategy instance for this symbol
		var strat strategy.Strategy
		switch strategyName {
		case "Engulfing Heikin Ashi":
			strat = strategy.NewEngulfingHeikinAshi(symbol, storage)
		case "Stochastic Heikin Ashi":
			strat = strategy.NewStochasticHeikinAshi(symbol, storage, 24, 10, 3)
		case "Four Stochastic Heikin Ashi":
			strat = strategy.NewFourStochasticHeikinAshi(symbol, storage)
		case "rsi":
			strat = strategy.NewRSIStrategy(symbol, 14, 70, 30, storage)
		default:
			log.Printf("Unknown strategy: %s, using StochasticHeikinAshi", strategyName)
			strat = strategy.NewStochasticHeikinAshi(symbol, storage, 24, 10, 3)
		}

		// Load candles for this symbol
		candles, err := loadBacktestCandles(ctx, storage, symbol, strat.Timeframe(), cfg.BacktestFrom.Time, cfg.BacktestTo.Time, cfg)
		if err != nil {
			log.Printf("Error loading candles for %s: %v", symbol, err)
			multiResults.FailedRuns++
			continue
		}

		if len(candles) == 0 {
			log.Printf("No candles found for symbol %s, skipping", symbol)
			multiResults.FailedRuns++
			continue
		}

		log.Printf("Loaded %d candles for %s [%s-%s]",
			len(candles), symbol,
			cfg.BacktestFrom.Time.Format(time.RFC3339),
			cfg.BacktestTo.Time.Format(time.RFC3339))

		// Run backtest for this symbol
		backtestResults := runStrategyBacktest(strat, candles, cfg)
		multiResults.Results[symbol] = backtestResults
		multiResults.SuccessfulRuns++

		// Read chart data for this symbol
		chartDataFile := "backtest_chart_data.json"
		if chartData, err := os.ReadFile(chartDataFile); err == nil {
			var symbolChartData []map[string]any
			if err := json.Unmarshal(chartData, &symbolChartData); err == nil {
				// Add symbol identifier to each chart data point
				for j := range symbolChartData {
					symbolChartData[j]["symbol"] = symbol
				}
				allChartData = append(allChartData, map[string]any{
					"symbol":  symbol,
					"data":    symbolChartData,
					"results": backtestResults,
				})
			} else {
				log.Printf("Error unmarshalling chart data for %s: %v", symbol, err)
			}
		}

		// Print progress
		if (i+1)%10 == 0 || i+1 == len(symbols) {
			log.Printf("Progress: %d/%d symbols completed", i+1, len(symbols))
		}
	}

	multiResults.EndTime = time.Now()

	// Calculate overall metrics
	calculateOverallMetrics(&multiResults)

	// Save multi-symbol backtest data as JSON for external HTML report
	saveMultiSymbolBacktestData(multiResults, allChartData)

	// Save comprehensive metrics for all symbols
	saveComprehensiveMetrics(multiResults)

	// Print summary
	printMultiSymbolSummary(multiResults)
}

// calculateOverallMetrics calculates aggregate metrics across all symbols
func calculateOverallMetrics(results *MultiSymbolBacktestResults) {
	if len(results.Results) == 0 {
		return
	}

	var (
		totalTrades       int
		totalWins         int
		totalLosses       int
		totalPnL          float64
		totalMaxDrawdown  float64
		totalEquity       float64
		profitableSymbols int
	)

	for _, result := range results.Results {
		totalTrades += result.Trades
		totalWins += result.Wins
		totalLosses += result.Losses
		totalPnL += (result.Equity - result.StartingBalance)
		totalMaxDrawdown += result.MaxDrawdown
		totalEquity += result.Equity

		if result.Equity > result.StartingBalance {
			profitableSymbols++
		}
	}

	symbolCount := float64(len(results.Results))

	results.OverallMetrics["total_trades"] = float64(totalTrades)
	results.OverallMetrics["total_wins"] = float64(totalWins)
	results.OverallMetrics["total_losses"] = float64(totalLosses)
	results.OverallMetrics["overall_win_rate"] = float64(totalWins) / float64(totalTrades)
	results.OverallMetrics["total_pnl"] = totalPnL
	results.OverallMetrics["avg_pnl_per_symbol"] = totalPnL / symbolCount
	results.OverallMetrics["avg_max_drawdown"] = totalMaxDrawdown / symbolCount
	results.OverallMetrics["avg_equity"] = totalEquity / symbolCount
	results.OverallMetrics["profitable_symbols_ratio"] = float64(profitableSymbols) / symbolCount
	results.OverallMetrics["profitable_symbols_count"] = float64(profitableSymbols)
}

// printMultiSymbolSummary prints a summary of the multi-symbol backtest results
func printMultiSymbolSummary(results MultiSymbolBacktestResults) {
	log.Println("===== MULTI-SYMBOL BACKTEST SUMMARY =====")
	log.Printf("Strategy: %s\n", results.Strategy)
	log.Printf("Period: %s to %s\n", results.StartTime.Format("2006-01-02"), results.EndTime.Format("2006-01-02"))
	log.Printf("Duration: %v\n", results.EndTime.Sub(results.StartTime).Round(time.Second))
	log.Printf("Total Symbols: %d\n", results.TotalSymbols)
	log.Printf("Successful Runs: %d\n", results.SuccessfulRuns)
	log.Printf("Failed Runs: %d\n", results.FailedRuns)
	log.Println()

	log.Println("=== OVERALL METRICS ===")
	log.Printf("Total Trades: %.0f\n", results.OverallMetrics["total_trades"])
	log.Printf("Overall Win Rate: %.2f%%\n", results.OverallMetrics["overall_win_rate"]*100)
	log.Printf("Total PnL: $%.2f\n", results.OverallMetrics["total_pnl"])
	log.Printf("Average PnL per Symbol: $%.2f\n", results.OverallMetrics["avg_pnl_per_symbol"])
	log.Printf("Average Max Drawdown: $%.2f\n", results.OverallMetrics["avg_max_drawdown"])
	log.Printf("Profitable Symbols: %.0f/%.0f (%.1f%%)\n",
		results.OverallMetrics["profitable_symbols_count"],
		float64(results.SuccessfulRuns),
		results.OverallMetrics["profitable_symbols_ratio"]*100)
	log.Println()

	// Top performing symbols
	type symbolPerf struct {
		Symbol string
		PnL    float64
	}

	var performances []symbolPerf
	for symbol, result := range results.Results {
		pnl := result.Equity - result.StartingBalance
		performances = append(performances, symbolPerf{Symbol: symbol, PnL: pnl})
	}

	sort.Slice(performances, func(i, j int) bool {
		return performances[i].PnL > performances[j].PnL
	})

	log.Println("=== TOP 10 PERFORMING SYMBOLS ===")
	count := min(10, len(performances))
	for i := 0; i < count; i++ {
		log.Printf("%d. %s: $%.2f\n", i+1, performances[i].Symbol, performances[i].PnL)
	}

	log.Println("\n=== BOTTOM 5 PERFORMING SYMBOLS ===")
	start := max(0, len(performances)-5)
	for i := start; i < len(performances); i++ {
		log.Printf("%d. %s: $%.2f\n", len(performances)-i, performances[i].Symbol, performances[i].PnL)
	}

}

// saveMultiSymbolBacktestData saves multi-symbol backtest data as JSON for external HTML report
func saveMultiSymbolBacktestData(results MultiSymbolBacktestResults, allChartData []map[string]any) {
	// Combine results and chart data into a single structure
	reportData := map[string]any{
		"results":   results,
		"chartData": allChartData,
		"timestamp": time.Now().UTC(),
	}

	// Save to JSON file
	filename := "multi_symbol_backtest_data.json"
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create JSON data file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ") // Pretty print
	if err := encoder.Encode(reportData); err != nil {
		log.Printf("Failed to encode JSON data: %v", err)
		return
	}

	log.Printf("Multi-symbol backtest data saved to: %s", filename)
	log.Printf("Open multi_symbol_backtest_report.html in your browser to view the interactive report")
}

// saveComprehensiveMetrics saves detailed metrics for all symbols in multiple formats
func saveComprehensiveMetrics(results MultiSymbolBacktestResults) {
	// Create comprehensive metrics structure
	comprehensiveMetrics := map[string]interface{}{
		"summary": map[string]interface{}{
			"strategy":        results.Strategy,
			"start_time":      results.StartTime,
			"end_time":        results.EndTime,
			"duration":        results.EndTime.Sub(results.StartTime).String(),
			"total_symbols":   results.TotalSymbols,
			"successful_runs": results.SuccessfulRuns,
			"failed_runs":     results.FailedRuns,
			"overall_metrics": results.OverallMetrics,
		},
		"symbol_details": make(map[string]interface{}),
		"performance_rankings": map[string]interface{}{
			"top_performers":       []map[string]interface{}{},
			"bottom_performers":    []map[string]interface{}{},
			"profitable_symbols":   []string{},
			"unprofitable_symbols": []string{},
		},
		"statistical_analysis": map[string]interface{}{
			"pnl_distribution":      map[string]float64{},
			"win_rate_distribution": map[string]float64{},
			"drawdown_distribution": map[string]float64{},
		},
	}

	// Collect all PnL values for statistical analysis
	var allPnLs []float64
	var allWinRates []float64
	var allDrawdowns []float64
	var profitableSymbols []string
	var unprofitableSymbols []string

	// Process each symbol's results
	for symbol, result := range results.Results {
		pnl := result.Equity - result.StartingBalance
		winRate := 0.0
		if result.Trades > 0 {
			winRate = float64(result.Wins) / float64(result.Trades)
		}

		// Symbol details
		symbolDetail := map[string]interface{}{
			"symbol":                  symbol,
			"starting_balance":        result.StartingBalance,
			"ending_balance":          result.Equity,
			"pnl":                     pnl,
			"pnl_percentage":          (pnl / result.StartingBalance) * 100,
			"trades":                  result.Trades,
			"wins":                    result.Wins,
			"losses":                  result.Losses,
			"win_rate":                winRate,
			"max_drawdown":            result.MaxDrawdown,
			"max_drawdown_percentage": (result.MaxDrawdown / result.StartingBalance) * 100,
			"max_consecutive_wins":    result.MaxConsecWins,
			"max_consecutive_losses":  result.MaxConsecLosses,
			"max_equity":              result.MaxEquity,
			"equity_curve_length":     len(result.EquityCurve),
			"trade_log_length":        len(result.TradeLog),
			"long_trades":             result.LongTrades,
			"short_trades":            result.ShortTrades,
		}

		comprehensiveMetrics["symbol_details"].(map[string]interface{})[symbol] = symbolDetail

		// Collect data for statistical analysis
		allPnLs = append(allPnLs, pnl)
		allWinRates = append(allWinRates, winRate)
		allDrawdowns = append(allDrawdowns, result.MaxDrawdown)

		if pnl > 0 {
			profitableSymbols = append(profitableSymbols, symbol)
		} else {
			unprofitableSymbols = append(unprofitableSymbols, symbol)
		}
	}

	// Sort symbols by performance
	type symbolPerf struct {
		Symbol   string
		PnL      float64
		WinRate  float64
		Drawdown float64
	}

	var performances []symbolPerf
	for symbol, result := range results.Results {
		pnl := result.Equity - result.StartingBalance
		winRate := 0.0
		if result.Trades > 0 {
			winRate = float64(result.Wins) / float64(result.Trades)
		}
		performances = append(performances, symbolPerf{
			Symbol:   symbol,
			PnL:      pnl,
			WinRate:  winRate,
			Drawdown: result.MaxDrawdown,
		})
	}

	// Sort by PnL (descending)
	sort.Slice(performances, func(i, j int) bool {
		return performances[i].PnL > performances[j].PnL
	})

	// Top 10 performers
	topCount := min(10, len(performances))
	for i := 0; i < topCount; i++ {
		perf := performances[i]
		comprehensiveMetrics["performance_rankings"].(map[string]interface{})["top_performers"] = append(
			comprehensiveMetrics["performance_rankings"].(map[string]interface{})["top_performers"].([]map[string]interface{}),
			map[string]interface{}{
				"rank":     i + 1,
				"symbol":   perf.Symbol,
				"pnl":      perf.PnL,
				"win_rate": perf.WinRate,
				"drawdown": perf.Drawdown,
			},
		)
	}

	// Bottom 5 performers
	bottomCount := min(5, len(performances))
	start := max(0, len(performances)-bottomCount)
	for i := start; i < len(performances); i++ {
		perf := performances[i]
		comprehensiveMetrics["performance_rankings"].(map[string]interface{})["bottom_performers"] = append(
			comprehensiveMetrics["performance_rankings"].(map[string]interface{})["bottom_performers"].([]map[string]interface{}),
			map[string]interface{}{
				"rank":     len(performances) - i,
				"symbol":   perf.Symbol,
				"pnl":      perf.PnL,
				"win_rate": perf.WinRate,
				"drawdown": perf.Drawdown,
			},
		)
	}

	// Statistical analysis
	if len(allPnLs) > 0 {
		sort.Float64s(allPnLs)
		sort.Float64s(allWinRates)
		sort.Float64s(allDrawdowns)

		comprehensiveMetrics["statistical_analysis"].(map[string]interface{})["pnl_distribution"] = map[string]float64{
			"min":    allPnLs[0],
			"max":    allPnLs[len(allPnLs)-1],
			"median": allPnLs[len(allPnLs)/2],
			"mean":   calculateMean(allPnLs),
		}

		comprehensiveMetrics["statistical_analysis"].(map[string]interface{})["win_rate_distribution"] = map[string]float64{
			"min":    allWinRates[0],
			"max":    allWinRates[len(allWinRates)-1],
			"median": allWinRates[len(allWinRates)/2],
			"mean":   calculateMean(allWinRates),
		}

		comprehensiveMetrics["statistical_analysis"].(map[string]interface{})["drawdown_distribution"] = map[string]float64{
			"min":    allDrawdowns[0],
			"max":    allDrawdowns[len(allDrawdowns)-1],
			"median": allDrawdowns[len(allDrawdowns)/2],
			"mean":   calculateMean(allDrawdowns),
		}
	}

	comprehensiveMetrics["performance_rankings"].(map[string]interface{})["profitable_symbols"] = profitableSymbols
	comprehensiveMetrics["performance_rankings"].(map[string]interface{})["unprofitable_symbols"] = unprofitableSymbols

	// Save comprehensive metrics in multiple formats
	saveMetricsAsJSON(comprehensiveMetrics, "comprehensive_metrics.json")
	saveMetricsAsCSV(results, "comprehensive_metrics.csv")
	saveMetricsAsHTML(comprehensiveMetrics, "comprehensive_metrics.html")

	log.Printf("Comprehensive metrics saved in multiple formats")
}

// calculateMean calculates the mean of a slice of float64 values
func calculateMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// saveMetricsAsJSON saves metrics as JSON file
func saveMetricsAsJSON(metrics map[string]interface{}, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create JSON metrics file: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metrics); err != nil {
		log.Printf("Failed to encode JSON metrics: %v", err)
		return
	}

	log.Printf("Comprehensive metrics saved to: %s", filename)
}

// saveMetricsAsCSV saves metrics as CSV file
func saveMetricsAsCSV(results MultiSymbolBacktestResults, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Printf("Failed to create CSV metrics file: %v", err)
		return
	}
	defer file.Close()

	// Write CSV header
	file.WriteString("Symbol,StartingBalance,EndingBalance,PnL,PnLPercentage,Trades,Wins,Losses,WinRate,MaxDrawdown,MaxDrawdownPercentage,MaxConsecutiveWins,MaxConsecutiveLosses,MaxEquity,EquityCurveLength,TradeLogLength,LongTrades,ShortTrades\n")

	// Write data for each symbol
	for symbol, result := range results.Results {
		pnl := result.Equity - result.StartingBalance
		winRate := 0.0
		if result.Trades > 0 {
			winRate = float64(result.Wins) / float64(result.Trades)
		}

		line := fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f,%d,%d,%d,%.4f,%.2f,%.2f,%d,%d,%.2f,%d,%d,%d,%d\n",
			symbol,
			result.StartingBalance,
			result.Equity,
			pnl,
			(pnl/result.StartingBalance)*100,
			result.Trades,
			result.Wins,
			result.Losses,
			winRate,
			result.MaxDrawdown,
			(result.MaxDrawdown/result.StartingBalance)*100,
			result.MaxConsecWins,
			result.MaxConsecLosses,
			result.MaxEquity,
			len(result.EquityCurve),
			len(result.TradeLog),
			result.LongTrades,
			result.ShortTrades,
		)
		file.WriteString(line)
	}

	log.Printf("CSV metrics saved to: %s", filename)
}

// saveMetricsAsHTML saves metrics as HTML report
func saveMetricsAsHTML(metrics map[string]interface{}, filename string) {
	// For now, save a simple HTML version
	htmlContent := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head>
    <title>Comprehensive Trading Metrics Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 5px; }
        .metric { display: inline-block; margin: 10px; padding: 10px; background: #f8f9fa; border-radius: 3px; }
        .positive { color: green; }
        .negative { color: red; }
    </style>
</head>
<body>
    <h1>Comprehensive Trading Metrics Report</h1>
    <div class="section">
        <h2>Summary</h2>
        <div class="metric">Strategy: %s</div>
        <div class="metric">Total Symbols: %d</div>
        <div class="metric">Successful Runs: %d</div>
        <div class="metric">Failed Runs: %d</div>
    </div>
    <div class="section">
        <h2>Overall Performance</h2>
        <div class="metric">Total PnL: $%.2f</div>
        <div class="metric">Average PnL per Symbol: $%.2f</div>
        <div class="metric">Overall Win Rate: %.2f%%</div>
    </div>
    <div class="section">
        <h2>Note</h2>
        <p>Detailed metrics are available in the JSON and CSV files.</p>
        <p>Open comprehensive_metrics.json for full data or comprehensive_metrics.csv for spreadsheet analysis.</p>
    </div>
</body>
</html>`,
		metrics["summary"].(map[string]interface{})["strategy"],
		metrics["summary"].(map[string]interface{})["total_symbols"],
		metrics["summary"].(map[string]interface{})["successful_runs"],
		metrics["summary"].(map[string]interface{})["failed_runs"],
		metrics["summary"].(map[string]interface{})["overall_metrics"].(map[string]float64)["total_pnl"],
		metrics["summary"].(map[string]interface{})["overall_metrics"].(map[string]float64)["avg_pnl_per_symbol"],
		metrics["summary"].(map[string]interface{})["overall_metrics"].(map[string]float64)["overall_win_rate"]*100,
	)

	err := os.WriteFile(filename, []byte(htmlContent), 0644)
	if err != nil {
		log.Printf("Failed to create HTML metrics file: %v", err)
		return
	}

	log.Printf("HTML metrics saved to: %s", filename)
}

// Helper functions
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
