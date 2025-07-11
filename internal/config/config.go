// Package config provides configuration management for the trading system
package config

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config holds all application configuration parameters
type Config struct {
	WallexAPIKey        string                `yaml:"wallex_api_key"`
	DBConnStr           string                `yaml:"db_conn_str"`
	DBMaxOpen           int                   `yaml:"db_max_open"`
	DBMaxIdle           int                   `yaml:"db_max_idle"`
	Mode                string                `yaml:"mode"`
	Symbols             []string              `yaml:"symbols"`
	Strategies          []string              `yaml:"strategies"`
	BacktestFrom        time.Time             `yaml:"backtest_from"`
	BacktestTo          time.Time             `yaml:"backtest_to"`
	OrderSize           float64               `yaml:"order_size"`
	OrderType           string                `yaml:"order_type"`
	RiskPercent         float64               `yaml:"risk_percent"`
	StopLossPercent     float64               `yaml:"stop_loss_percent"`
	TrailingStopPercent float64               `yaml:"trailing_stop_percent"`
	TakeProfitPercent   float64               `yaml:"take_profit_percent"`
	MaxDailyLoss        float64               `yaml:"max_daily_loss"`
	LimitSpread         float64               `yaml:"limit_spread"`
	TelegramToken       string                `yaml:"telegram_token"`
	TelegramChatID      string                `yaml:"telegram_chat_id"`
	NotificationRetries int                   `yaml:"notification_retries"`
	NotificationDelay   time.Duration         `yaml:"notification_delay"`
	RiskMap             map[string]RiskParams `yaml:"risk_map"`
	SlippagePercent     float64               `yaml:"slippage_percent"`
	CommissionPercent   float64               `yaml:"commission_percent"`
}

// RiskParams defines risk management parameters for trading strategies
type RiskParams struct {
	RiskPercent         float64 `yaml:"risk_percent"`
	StopLossPercent     float64 `yaml:"stop_loss_percent"`
	TrailingStopPercent float64 `yaml:"trailing_stop_percent"`
}

// GetRiskParams returns risk parameters for a specific strategy
func GetRiskParams(cfg Config, stratName string) RiskParams {
	// Check if we have specific risk parameters for this strategy
	if cfg.RiskMap != nil {
		if params, exists := cfg.RiskMap[stratName]; exists {
			return params
		}
	}

	// Return default risk parameters
	return RiskParams{
		RiskPercent:         cfg.RiskPercent,
		StopLossPercent:     cfg.StopLossPercent,
		TrailingStopPercent: cfg.TrailingStopPercent,
	}
}

// LoadConfig loads configuration from command-line flags, environment variables, and/or a YAML config file
func LoadConfig() (Config, error) {
	// Define command-line flags
	mode := flag.String("mode", "live", "Mode: live or backtest")
	from := flag.String("from", time.Now().AddDate(-2, 0, 0).Format("2006-01-02"), "Backtest start date (YYYY-MM-DD)")
	to := flag.String("to", time.Now().Format("2006-01-02"), "Backtest end date (YYYY-MM-DD)")
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
	symbols := flag.String("symbols", "btc-usdt", "Comma-separated list of trading symbols")
	strategies := flag.String("strategies", "rsi", "Comma-separated list of strategies")
	riskMapFlag := flag.String("risk-map", "", "Comma-separated strategy:risk:stoploss:trailing triples (e.g., rsi:1.0:2.0:0.5)")
	slippagePercent := flag.Float64("slippage-percent", 0.0, "Slippage percent per trade (e.g., 0.05 for 0.05%)")
	commissionPercent := flag.Float64("commission-percent", 0.0, "Commission percent per trade (e.g., 0.1 for 0.1%)")
	configFile := flag.String("config", "", "Path to YAML config file")

	flag.Parse()

	// Parse dates
	fromTime, err := time.Parse("2006-01-02", *from)
	if err != nil {
		return Config{}, fmt.Errorf("invalid from date format: %w", err)
	}

	toTime, err := time.Parse("2006-01-02", *to)
	if err != nil {
		return Config{}, fmt.Errorf("invalid to date format: %w", err)
	}

	// Parse risk map
	riskMap := make(map[string]RiskParams)
	if *riskMapFlag != "" {
		for _, triple := range strings.Split(*riskMapFlag, ",") {
			parts := strings.Split(triple, ":")
			if len(parts) == 4 {
				stratName := parts[0]

				risk, err := strconv.ParseFloat(parts[1], 64)
				if err != nil {
					return Config{}, fmt.Errorf("invalid risk value for strategy %s: %w", stratName, err)
				}

				sl, err := strconv.ParseFloat(parts[2], 64)
				if err != nil {
					return Config{}, fmt.Errorf("invalid stop loss value for strategy %s: %w", stratName, err)
				}

				trailing, err := strconv.ParseFloat(parts[3], 64)
				if err != nil {
					return Config{}, fmt.Errorf("invalid trailing stop value for strategy %s: %w", stratName, err)
				}

				riskMap[stratName] = RiskParams{
					RiskPercent:         risk,
					StopLossPercent:     sl,
					TrailingStopPercent: trailing,
				}
			}
		}
	}

	// Load from YAML file if specified
	if *configFile != "" {
		data, err := os.ReadFile(*configFile)
		if err != nil {
			return Config{}, fmt.Errorf("failed to read config file: %w", err)
		}

		var fileCfg Config
		err = yaml.Unmarshal(data, &fileCfg)
		if err != nil {
			return Config{}, fmt.Errorf("failed to parse config file: %w", err)
		}

		return fileCfg, nil
	}

	// Create config from flags and environment variables
	return Config{
		WallexAPIKey:        os.Getenv("WALLEX_API_KEY"),
		DBConnStr:           os.Getenv("DB_CONN_STR"),
		DBMaxOpen:           10,
		DBMaxIdle:           5,
		Mode:                *mode,
		BacktestFrom:        fromTime,
		BacktestTo:          toTime,
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
		Symbols:             strings.Split(*symbols, ","),
		Strategies:          strings.Split(*strategies, ","),
		RiskMap:             riskMap,
		SlippagePercent:     *slippagePercent,
		CommissionPercent:   *commissionPercent,
	}, nil
}

// MustLoadConfig loads configuration and panics on error
func MustLoadConfig() Config {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	return cfg
}
