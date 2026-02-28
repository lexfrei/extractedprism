// Package main provides the entry point for the extractedprism load balancer.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
	base := config.NewBaseConfig()
	flags := rootCmd.PersistentFlags()
	flags.String("bind-address", base.BindAddress, "address to bind the LB listener")
	flags.Int("bind-port", base.BindPort, "port for the LB listener")
	flags.Int("health-port", base.HealthPort, "port for the health HTTP server")
	flags.String("endpoints", "", "comma-separated control plane endpoints (required)")
	flags.Duration("health-interval", base.HealthInterval, "interval between health checks")
	flags.Duration("health-timeout", base.HealthTimeout, "timeout for each health check")
	flags.Bool("enable-discovery", base.EnableDiscovery, "enable Kubernetes endpoint discovery")
	flags.String("log-level", base.LogLevel, "log level (debug, info, warn, error, dpanic, panic, fatal)")
}

func bindEnvVars() {
	viper.SetEnvPrefix("EP")

	flags := rootCmd.PersistentFlags()

	mustBindPFlag("bind_address", flags.Lookup("bind-address"))
	mustBindPFlag("bind_port", flags.Lookup("bind-port"))
	mustBindPFlag("health_port", flags.Lookup("health-port"))
	mustBindPFlag("endpoints", flags.Lookup("endpoints"))
	mustBindPFlag("health_interval", flags.Lookup("health-interval"))
	mustBindPFlag("health_timeout", flags.Lookup("health-timeout"))
	mustBindPFlag("enable_discovery", flags.Lookup("enable-discovery"))
	mustBindPFlag("log_level", flags.Lookup("log-level"))

	viper.AutomaticEnv()
}

func mustBindPFlag(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic(fmt.Sprintf("bind flag %q: %v", key, err))
	}
}

func run(_ *cobra.Command, _ []string) error {
	cfg := buildConfig()

	if err := cfg.Validate(); err != nil {
		return errors.Wrap(err, "invalid configuration")
	}

	logger, err := buildLogger(cfg.LogLevel)
	if err != nil {
		return errors.Wrap(err, "create logger")
	}

	defer func() { _ = logger.Sync() }()

	srv, err := server.New(cfg, logger)
	if err != nil {
		return errors.Wrap(err, "create server")
	}

	ctx, stop := signalContext()
	defer stop()

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
	// NewBaseConfig is used for allocation; actual defaults come through the
	// cobra/viper pipeline (registerFlags sets cobra defaults from NewBaseConfig).
	cfg := config.NewBaseConfig()
	cfg.BindAddress = viper.GetString("bind_address")
	cfg.BindPort = viper.GetInt("bind_port")
	cfg.HealthPort = viper.GetInt("health_port")
	cfg.Endpoints = config.ParseEndpoints(viper.GetString("endpoints"))
	cfg.HealthInterval = viper.GetDuration("health_interval")
	cfg.HealthTimeout = viper.GetDuration("health_timeout")
	cfg.EnableDiscovery = viper.GetBool("enable_discovery")
	cfg.LogLevel = viper.GetString("log_level")

	return cfg
}

// buildLogger creates a production zap logger at the given level.
// The level is expected to be pre-validated by cfg.Validate(); the ParseLevel
// error check is retained as defense-in-depth.
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

func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
}
