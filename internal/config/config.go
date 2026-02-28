// Package config provides configuration types and validation for extractedprism.
package config

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	defaultBindAddress    = "127.0.0.1"
	defaultBindPort       = 7445
	defaultHealthPort     = 7446
	defaultHealthInterval = 20 * time.Second
	defaultHealthTimeout  = 15 * time.Second
	defaultLogLevel       = "info"

	minPort = 1
	maxPort = 65535
)

// Sentinel errors for configuration validation.
var (
	ErrNoEndpoints         = errors.New("no endpoints configured")
	ErrInvalidEndpoint     = errors.New("invalid endpoint")
	ErrInvalidPort         = errors.New("invalid port number")
	ErrPortConflict        = errors.New("bind port and health port must differ")
	ErrInvalidHealthTiming = errors.New("health timeout must be less than health interval")
)

// Config holds all configuration for the extractedprism proxy.
type Config struct {
	BindAddress     string
	BindPort        int
	HealthPort      int
	Endpoints       []string
	HealthInterval  time.Duration
	HealthTimeout   time.Duration
	EnableDiscovery bool
	LogLevel        string
}

// NewDefault returns a Config populated with default values.
func NewDefault() *Config {
	return &Config{
		BindAddress:     defaultBindAddress,
		BindPort:        defaultBindPort,
		HealthPort:      defaultHealthPort,
		HealthInterval:  defaultHealthInterval,
		HealthTimeout:   defaultHealthTimeout,
		EnableDiscovery: true,
		LogLevel:        defaultLogLevel,
	}
}

// Validate checks that the configuration is valid.
func (cfg *Config) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return ErrNoEndpoints
	}

	err := validateEndpoints(cfg.Endpoints)
	if err != nil {
		return err
	}

	err = validatePorts(cfg.BindPort, cfg.HealthPort)
	if err != nil {
		return err
	}

	if cfg.HealthTimeout >= cfg.HealthInterval {
		return ErrInvalidHealthTiming
	}

	return nil
}

func validateEndpoints(endpoints []string) error {
	for _, endpoint := range endpoints {
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil || host == "" || port == "" {
			return errors.Wrap(ErrInvalidEndpoint, endpoint)
		}

		portNum, parseErr := strconv.Atoi(port)
		if parseErr != nil || portNum < minPort || portNum > maxPort {
			return errors.Wrapf(ErrInvalidEndpoint, "%s: port must be a number between 1 and 65535", endpoint)
		}
	}

	return nil
}

func validatePorts(bindPort, healthPort int) error {
	if bindPort < minPort || bindPort > maxPort {
		return errors.Wrapf(ErrInvalidPort, "bind port %d", bindPort)
	}

	if healthPort < minPort || healthPort > maxPort {
		return errors.Wrapf(ErrInvalidPort, "health port %d", healthPort)
	}

	if bindPort == healthPort {
		return ErrPortConflict
	}

	return nil
}

// ParseEndpoints splits a comma-separated string into a slice of endpoints.
func ParseEndpoints(csv string) []string {
	if strings.TrimSpace(csv) == "" {
		return []string{}
	}

	parts := strings.Split(csv, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}

	return result
}
