// Package config provides configuration management for the trading system
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/amirphl/simple-trader/internal/utils"
	"gopkg.in/yaml.v3"
)

// FlexibleTime is a wrapper around time.Time that supports unmarshaling from
// both date-only format (YYYY-MM-DD) and full RFC3339 format
type FlexibleTime struct {
	time.Time
}

// UnmarshalYAML implements the yaml.Unmarshaler interface
func (ft *FlexibleTime) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var timeStr string
	if err := unmarshal(&timeStr); err != nil {
		return err
	}

	// Try parsing as RFC3339 first
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		// If that fails, try parsing as date only
		t, err = time.Parse("2006-01-02", timeStr)
		if err != nil {
			return fmt.Errorf("cannot parse time %q as either RFC3339 or YYYY-MM-DD: %v", timeStr, err)
		}
	}

	ft.Time = t
	return nil
}

// Config holds all application configuration parameters
type Config struct {
	WallexAPIKey        string                `yaml:"wallex_api_key"`
	DBConnStr           string                `yaml:"db_conn_str"`
	DBMaxOpen           int                   `yaml:"db_max_open"`
	DBMaxIdle           int                   `yaml:"db_max_idle"`
	RunMigration        bool                  `yaml:"run_migration"`
	Mode                string                `yaml:"mode"`
	Symbols             []string              `yaml:"symbols"`
	Strategies          []string              `yaml:"strategies"`
	BacktestFrom        FlexibleTime          `yaml:"backtest_from"`
	BacktestTo          FlexibleTime          `yaml:"backtest_to"`
	OrderType           string                `yaml:"order_type"`
	RiskPercent         float64               `yaml:"risk_percent"`
	StopLossPercent     float64               `yaml:"stop_loss_percent"`
	TrailingStopPercent float64               `yaml:"trailing_stop_percent"`
	TakeProfitPercent   float64               `yaml:"take_profit_percent"`
	MaxDailyLoss        float64               `yaml:"max_daily_loss"`
	LimitSpread         float64               `yaml:"limit_spread"`
	InitialBalance      float64               `yaml:"initial_balance"`
	TelegramToken       string                `yaml:"telegram_token"`
	TelegramChatID      string                `yaml:"telegram_chat_id"`
	NotificationRetries int                   `yaml:"notification_retries"`
	NotificationDelay   time.Duration         `yaml:"notification_delay"`
	RiskMap             map[string]RiskParams `yaml:"risk_map"`
	SlippagePercent     float64               `yaml:"slippage_percent"`
	CommissionPercent   float64               `yaml:"commission_percent"`
	ProxyURL            string                `yaml:"proxy_url"`

	// Multi-symbol backtest configuration
	MultiSymbolBacktest bool `yaml:"multi_symbol_backtest"`
	TopSymbolsCount     int  `yaml:"top_symbols_count"`

	// API retry configuration
	APIRetryMaxAttempts int           `yaml:"api_retry_max_attempts"`
	APIRetryBaseDelay   time.Duration `yaml:"api_retry_base_delay"`
	APIRetryMaxDelay    time.Duration `yaml:"api_retry_max_delay"`
}

// RiskParams defines risk management parameters for trading strategies
type RiskParams struct {
	RiskPercent         float64 `yaml:"risk_percent" json:"risk_percent"`
	StopLossPercent     float64 `yaml:"stop_loss_percent" json:"stop_loss_percent"`
	TrailingStopPercent float64 `yaml:"trailing_stop_percent" json:"trailing_stop_percent"`
	TakeProfitPercent   float64 `yaml:"take_profit_percent" json:"take_profit_percent"`
	MaxDailyLoss        float64 `yaml:"max_daily_loss" json:"max_daily_loss"`
	LimitSpread         float64 `yaml:"limit_spread" json:"limit_spread"`
	InitialBalance      float64 `yaml:"initial_balance" json:"initial_balance"`
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
		TakeProfitPercent:   cfg.TakeProfitPercent,
		MaxDailyLoss:        cfg.MaxDailyLoss,
		LimitSpread:         cfg.LimitSpread,
		InitialBalance:      cfg.InitialBalance,
	}
}

// LoadConfig loads configuration from command-line flags, environment variables, and/or a YAML config file
func LoadConfig() (Config, error) {
	// Define command-line flags
	runMigration := flag.Bool("run-migration", false, "Run database migrations")
	mode := flag.String("mode", "live", "Mode: live or backtest")
	symbols := flag.String("symbols", "btc-usdt", "Comma-separated list of trading symbols")
	strategies := flag.String("strategies", "rsi", "Comma-separated list of strategies")
	from := flag.String("from", time.Now().AddDate(-2, 0, 0).Format("2006-01-02"), "Backtest start date (YYYY-MM-DD)")
	to := flag.String("to", time.Now().Format("2006-01-02"), "Backtest end date (YYYY-MM-DD)")
	orderType := flag.String("order-type", "market", "Order type: market or limit or stop-limit or oco")
	telegramToken := flag.String("telegram-token", "", "Telegram bot token for notifications")
	telegramChatID := flag.String("telegram-chat", "", "Telegram chat ID for notifications")
	notificationRetries := flag.Int("notification-retries", 3, "Number of notification send attempts")
	notificationDelay := flag.Duration("notification-delay", 5*time.Second, "Delay between notification retries (e.g., 5s)")
	riskPercent := flag.Float64("risk-percent", 1.0, "Risk percent per trade (e.g., 1.0 for 1%)")
	stopLossPercent := flag.Float64("stop-loss-percent", 2.0, "Stop loss percent (e.g., 2.0 for 2%)")
	trailingStopPercent := flag.Float64("trailing-stop-percent", 0.0, "Trailing stop percent (e.g., 1.0 for 1%)")
	takeProfitPercent := flag.Float64("take-profit-percent", 0.0, "Take profit percent (e.g., 2.0 for 2%)")
	maxDailyLoss := flag.Float64("max-daily-loss", 0.0, "Max daily loss in account currency (e.g., 100.0)")
	limitSpread := flag.Float64("limit-spread", 0.0, "Limit order spread as percent (e.g., 0.1 for 0.1%)")
	initialBalance := flag.Float64("initial-balance", 0.0, "Initial balance in account currency (e.g., 100.0)")
	riskMapFlag := flag.String("risk-map", "", "Comma-separated strategy:risk:stoploss:trailing triples (e.g., rsi:1.0:2.0:0.5)")
	commissionPercent := flag.Float64("commission-percent", 0.1, "Commission percent (e.g., 0.1 for 0.1%)")
	slippagePercent := flag.Float64("slippage-percent", 0.05, "Slippage percent (e.g., 0.05 for 0.05%)")
	proxyURL := flag.String("proxy-url", "", "Proxy URL for API calls")

	// Multi-symbol backtest flags
	multiSymbolBacktest := flag.Bool("multi-symbol-backtest", false, "Run backtest on top N symbols from Binance")
	topSymbolsCount := flag.Int("top-symbols-count", 100, "Number of top symbols to backtest (default: 100)")

	// API retry flags
	apiRetryMaxAttempts := flag.Int("api-retry-max-attempts", 5, "Maximum retry attempts for API calls (default: 5)")
	apiRetryBaseDelay := flag.Duration("api-retry-base-delay", 1*time.Second, "Base delay for API retry (default: 1s)")
	apiRetryMaxDelay := flag.Duration("api-retry-max-delay", 30*time.Second, "Maximum delay for API retry (default: 30s)")

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
			if len(parts) != 8 {
				return Config{}, fmt.Errorf("invalid risk map format: %s", *riskMapFlag)
			}
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

			tp, err := strconv.ParseFloat(parts[4], 64)
			if err != nil {
				return Config{}, fmt.Errorf("invalid take profit value for strategy %s: %w", stratName, err)
			}

			maxDailyLoss, err := strconv.ParseFloat(parts[5], 64)
			if err != nil {
				return Config{}, fmt.Errorf("invalid max daily loss value for strategy %s: %w", stratName, err)
			}

			limitSpread, err := strconv.ParseFloat(parts[6], 64)
			if err != nil {
				return Config{}, fmt.Errorf("invalid limit spread value for strategy %s: %w", stratName, err)
			}

			initialbalance, err := strconv.ParseFloat(parts[7], 64)
			if err != nil {
				return Config{}, fmt.Errorf("invalid balance value for strategy %s: %w", stratName, err)
			}

			riskMap[stratName] = RiskParams{
				RiskPercent:         risk,
				StopLossPercent:     sl,
				TrailingStopPercent: trailing,
				TakeProfitPercent:   tp,
				MaxDailyLoss:        maxDailyLoss,
				LimitSpread:         limitSpread,
				InitialBalance:      initialbalance,
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
		RunMigration:        *runMigration,
		Mode:                *mode,
		BacktestFrom:        FlexibleTime{Time: fromTime},
		BacktestTo:          FlexibleTime{Time: toTime},
		OrderType:           *orderType,
		TelegramToken:       *telegramToken,
		TelegramChatID:      *telegramChatID,
		NotificationRetries: *notificationRetries,
		NotificationDelay:   *notificationDelay,
		RiskPercent:         *riskPercent,
		StopLossPercent:     *stopLossPercent,
		TrailingStopPercent: *trailingStopPercent,
		TakeProfitPercent:   *takeProfitPercent,
		MaxDailyLoss:        *maxDailyLoss,
		LimitSpread:         *limitSpread,
		InitialBalance:      *initialBalance,
		Symbols:             strings.Split(*symbols, ","),
		Strategies:          strings.Split(*strategies, ","),
		RiskMap:             riskMap,
		SlippagePercent:     *slippagePercent,
		CommissionPercent:   *commissionPercent,
		ProxyURL:            *proxyURL,
		MultiSymbolBacktest: *multiSymbolBacktest,
		TopSymbolsCount:     *topSymbolsCount,
		APIRetryMaxAttempts: *apiRetryMaxAttempts,
		APIRetryBaseDelay:   *apiRetryBaseDelay,
		APIRetryMaxDelay:    *apiRetryMaxDelay,
	}, nil
}

// MustLoadConfig loads configuration and panics on error
func MustLoadConfig() Config {
	cfg, err := LoadConfig()
	if err != nil {
		// TODO: Return error
		utils.GetLogger().Fatalf("Failed to load configuration: %v", err)
	}
	return cfg
}
