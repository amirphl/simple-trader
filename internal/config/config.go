// Package config
package config

import (
	"flag"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

/*
YAML config example:
wallex_api_key: "..."
db_conn_str: "..."
db_max_open: 10
db_max_idle: 5
mode: "live"
symbols: ["BTCIRT", "ETHIRT"]
timeframes: ["1m", "5m"]
strategy_map:
  BTCIRT:
    1m: "ema"
    5m: "rsi"
  ETHIRT:
    1m: "macd"
risk_map:
  BTCIRT:
    1m: { risk_percent: 1.0, stop_loss_percent: 2.0, trailing_stop_percent: 0.5 }
    5m: { risk_percent: 0.5, stop_loss_percent: 1.5, trailing_stop_percent: 0.2 }
  ETHIRT:
    1m: { risk_percent: 0.7, stop_loss_percent: 1.0, trailing_stop_percent: 0.3 }
...
*/

type Config struct {
	WallexAPIKey        string
	DBConnStr           string
	DBMaxOpen           int
	DBMaxIdle           int
	Mode                string
	Symbol              string
	Timeframe           string
	BacktestFrom        time.Time
	BacktestTo          time.Time
	Strategy            string
	OrderSize           float64
	TelegramToken       string
	TelegramChatID      string
	RiskPercent         float64
	StopLossPercent     float64
	TrailingStopPercent float64
	TakeProfitPercent   float64
	MaxDailyLoss        float64
	OrderType           string
	LimitSpread         float64
	NotificationRetries int
	NotificationDelay   time.Duration
	Symbols             []string
	Timeframes          []string
	StrategyMap         map[string]map[string]string
	RiskMap             map[string]map[string]RiskParams
	SlippagePercent     float64
	CommissionPercent   float64
}

type RiskParams struct {
	RiskPercent         float64
	StopLossPercent     float64
	TrailingStopPercent float64
}

// getRiskParams returns risk parameters for a specific symbol and timeframe
func getRiskParams(cfg Config, symbol, timeframe string) RiskParams {
	// Check if we have specific risk parameters for this symbol/timeframe
	if cfg.RiskMap != nil {
		if symbolRisk, exists := cfg.RiskMap[symbol]; exists {
			if timeframeRisk, exists := symbolRisk[timeframe]; exists {
				return timeframeRisk
			}
		}
	}

	// Return default risk parameters
	return RiskParams{
		RiskPercent:         cfg.RiskPercent,
		StopLossPercent:     cfg.StopLossPercent,
		TrailingStopPercent: cfg.TrailingStopPercent,
	}
}

func loadConfig() Config {
	mode := flag.String("mode", "live", "Mode: live or backtest")
	symbol := flag.String("symbol", "BTCIRT", "Trading symbol")
	timeframe := flag.String("timeframe", "1m", "Candle timeframe for backtest")
	from := flag.String("from", time.Now().AddDate(-2, 0, 0).Format("2006-01-02"), "Backtest start date (YYYY-MM-DD)")
	to := flag.String("to", time.Now().Format("2006-01-02"), "Backtest end date (YYYY-MM-DD)")
	strategyName := flag.String("strategy", "sma", "Strategy: sma or ema or rsi or macd or composite or rsi-obos")
	orderSize := flag.Float64("order-size", 0.001, "Order size (quantity) for live trading")
	telegramToken := flag.String("telegram-token", "", "Telegram bot token for notifications")
	telegramChatID := flag.String("telegram-chat", "", "Telegram chat ID for notifications")
	riskPercent := flag.Float64("risk-percent", 1.0, "Risk percent per trade (e.g., 1.0 for 1%)")
	stopLossPercent := flag.Float64("stop-loss-percent", 2.0, "Stop loss percent (e.g., 2.0 for 2%)")
	trailingStopPercent := flag.Float64("trailing-stop-percent", 0.0, "Trailing stop percent (e.g., 1.0 for 1%)")
	takeProfitPercent := flag.Float64("take-profit-percent", 0.0, "Take profit percent (e.g., 2.0 for 2%)")
	maxDailyLoss := flag.Float64("max-daily-loss", 0.0, "Max daily loss in account currency (e.g., 100.0)")
	orderType := flag.String("order-type", "market", "Order type: market or limit or stop-limit or oco")
	limitSpread := flag.Float64("limit-spread", 0.0, "Limit order spread as percent (e.g., 0.1 for 0.1%)")
	notificationRetries := flag.Int("notification-retries", 3, "Number of notification send attempts")
	notificationDelay := flag.Duration("notification-delay", 5*time.Second, "Delay between notification retries (e.g., 5s)")
	symbolsFlag := flag.String("symbols", "BTCIRT", "Comma-separated list of trading symbols")
	timeframesFlag := flag.String("timeframes", "1m", "Comma-separated list of candle timeframes")
	strategyMapFlag := flag.String("strategy-map", "", "Comma-separated symbol:timeframe:strategy triples (e.g., BTCIRT:1m:ema,ETHIRT:5m:rsi)")
	riskMapFlag := flag.String("risk-map", "", "Comma-separated symbol:timeframe:risk:stoploss:trailing triples (e.g., BTCIRT:1m:1.0:2.0:0.5)")
	slippagePercent := flag.Float64("slippage-percent", 0.0, "Slippage percent per trade (e.g., 0.05 for 0.05%)")
	commissionPercent := flag.Float64("commission-percent", 0.0, "Commission percent per trade (e.g., 0.1 for 0.1%)")
	configFile := flag.String("config", "", "Path to YAML config file")
	flag.Parse()
	fromTime, _ := time.Parse("2006-01-02", *from)
	toTime, _ := time.Parse("2006-01-02", *to)
	strategyMap := make(map[string]map[string]string)
	if *strategyMapFlag != "" {
		for triple := range strings.SplitSeq(*strategyMapFlag, ",") {
			parts := strings.Split(triple, ":")
			if len(parts) == 3 {
				sym, timeframe, strat := parts[0], parts[1], parts[2]
				if _, ok := strategyMap[sym]; !ok {
					strategyMap[sym] = make(map[string]string)
				}
				strategyMap[sym][timeframe] = strat
			}
		}
	}
	riskMap := make(map[string]map[string]RiskParams)
	if *riskMapFlag != "" {
		for triple := range strings.SplitSeq(*riskMapFlag, ",") {
			parts := strings.Split(triple, ":")
			if len(parts) == 5 {
				sym, timeframe := parts[0], parts[1]
				risk, _ := strconv.ParseFloat(parts[2], 64)
				sl, _ := strconv.ParseFloat(parts[3], 64)
				trailing, _ := strconv.ParseFloat(parts[4], 64)
				if _, ok := riskMap[sym]; !ok {
					riskMap[sym] = make(map[string]RiskParams)
				}
				riskMap[sym][timeframe] = RiskParams{risk, sl, trailing}
			}
		}
	}
	if *configFile != "" {
		data, err := os.ReadFile(*configFile)
		if err != nil {
			log.Fatalf("Failed to read config file: %v", err)
		}
		var fileCfg Config
		err = yaml.Unmarshal(data, &fileCfg)
		if err != nil {
			log.Fatalf("Failed to parse config file: %v", err)
		}
		return fileCfg
	}
	return Config{
		WallexAPIKey:        os.Getenv("WALLEX_API_KEY"),
		DBConnStr:           os.Getenv("DB_CONN_STR"),
		DBMaxOpen:           10,
		DBMaxIdle:           5,
		Mode:                *mode,
		Symbol:              *symbol,
		Timeframe:           *timeframe,
		BacktestFrom:        fromTime,
		BacktestTo:          toTime,
		Strategy:            *strategyName,
		OrderSize:           *orderSize,
		TelegramToken:       *telegramToken,
		TelegramChatID:      *telegramChatID,
		RiskPercent:         *riskPercent,
		StopLossPercent:     *stopLossPercent,
		TrailingStopPercent: *trailingStopPercent,
		TakeProfitPercent:   *takeProfitPercent,
		MaxDailyLoss:        *maxDailyLoss,
		OrderType:           *orderType,
		LimitSpread:         *limitSpread,
		NotificationRetries: *notificationRetries,
		NotificationDelay:   *notificationDelay,
		Symbols:             strings.Split(*symbolsFlag, ","),
		Timeframes:          strings.Split(*timeframesFlag, ","),
		StrategyMap:         strategyMap,
		RiskMap:             riskMap,
		SlippagePercent:     *slippagePercent,
		CommissionPercent:   *commissionPercent,
	}
}
