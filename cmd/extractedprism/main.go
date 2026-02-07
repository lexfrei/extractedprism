// Package main provides the entry point for the extractedprism load balancer.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/lexfrei/extractedprism/internal/config"
	"github.com/lexfrei/extractedprism/internal/server"
)

// Build-time variables set via ldflags.
var (
	Version  = "development"
	Revision = "unknown"
)

var rootCmd = &cobra.Command{
	Use:     "extractedprism",
	Short:   "TCP load balancer for Kubernetes control plane endpoints",
	Version: Version + " (" + Revision + ")",
	RunE:    run,
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

//nolint:gochecknoinits // cobra requires init for flag registration
func init() {
	registerFlags()
	bindEnvVars()
}

func registerFlags() {
	flags := rootCmd.PersistentFlags()
	flags.String("bind-address", "127.0.0.1", "address to bind the LB listener")
	flags.Int("bind-port", 7445, "port for the LB listener")
	flags.Int("health-port", 7446, "port for the health HTTP server")
	flags.String("endpoints", "", "comma-separated control plane endpoints (required)")
	flags.Duration("health-interval", 20*time.Second, "interval between health checks")
	flags.Duration("health-timeout", 15*time.Second, "timeout for each health check")
	flags.Bool("enable-discovery", true, "enable Kubernetes endpoint discovery")
	flags.String("log-level", "info", "log level (debug, info, warn, error)")
}

func bindEnvVars() {
	viper.SetEnvPrefix("EP")

	_ = viper.BindPFlag("bind_address", rootCmd.PersistentFlags().Lookup("bind-address"))
	_ = viper.BindPFlag("bind_port", rootCmd.PersistentFlags().Lookup("bind-port"))
	_ = viper.BindPFlag("health_port", rootCmd.PersistentFlags().Lookup("health-port"))
	_ = viper.BindPFlag("endpoints", rootCmd.PersistentFlags().Lookup("endpoints"))
	_ = viper.BindPFlag("health_interval", rootCmd.PersistentFlags().Lookup("health-interval"))
	_ = viper.BindPFlag("health_timeout", rootCmd.PersistentFlags().Lookup("health-timeout"))
	_ = viper.BindPFlag("enable_discovery", rootCmd.PersistentFlags().Lookup("enable-discovery"))
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))

	viper.AutomaticEnv()
}

func run(_ *cobra.Command, _ []string) error {
	cfg := buildConfig()

	logger, err := buildLogger(cfg.LogLevel)
	if err != nil {
		return errors.Wrap(err, "create logger")
	}

	defer func() { _ = logger.Sync() }()

	srv, err := server.New(cfg, logger)
	if err != nil {
		return errors.Wrap(err, "create server")
	}

	ctx := signalContext()

	logger.Info("starting extractedprism",
		zap.String("version", Version),
		zap.String("bind", cfg.BindAddress),
		zap.Int("port", cfg.BindPort),
	)

	runErr := srv.Run(ctx)
	if runErr != nil {
		return errors.Wrap(runErr, "server run")
	}

	return nil
}

func buildConfig() *config.Config {
	return &config.Config{
		BindAddress:     viper.GetString("bind_address"),
		BindPort:        viper.GetInt("bind_port"),
		HealthPort:      viper.GetInt("health_port"),
		Endpoints:       config.ParseEndpoints(viper.GetString("endpoints")),
		HealthInterval:  viper.GetDuration("health_interval"),
		HealthTimeout:   viper.GetDuration("health_timeout"),
		EnableDiscovery: viper.GetBool("enable_discovery"),
		LogLevel:        viper.GetString("log_level"),
	}
}

func buildLogger(level string) (*zap.Logger, error) {
	zapCfg := zap.NewProductionConfig()
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	parsedLevel, err := zapcore.ParseLevel(level)
	if err != nil {
		return nil, errors.Wrap(err, "parse log level")
	}

	zapCfg.Level.SetLevel(parsedLevel)

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, errors.Wrap(err, "build logger")
	}

	return logger, nil
}

func signalContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
	}()

	return ctx
}
