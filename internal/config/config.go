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
	ErrNoEndpoints           = errors.New("no endpoints configured")
	ErrInvalidEndpoint       = errors.New("invalid endpoint")
	ErrInvalidPort           = errors.New("invalid port number")
	ErrPortConflict          = errors.New("bind port and health port must differ")
	ErrInvalidHealthTiming   = errors.New("health timeout must be less than health interval")
	ErrInvalidBindAddress    = errors.New("invalid bind address")
	ErrInvalidHealthDuration = errors.New("invalid health duration")
)

const minHealthDuration = 1 * time.Second

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

// NewBaseConfig returns a Config populated with sensible defaults for optional
// fields. Required fields such as Endpoints must be set by the caller before
// the config passes Validate.
func NewBaseConfig() *Config {
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

	err := validateBindAddress(cfg.BindAddress)
	if err != nil {
		return err
	}

	err = validateEndpoints(cfg.Endpoints)
	if err != nil {
		return err
	}

	err = validatePorts(cfg.BindPort, cfg.HealthPort)
	if err != nil {
		return err
	}

	if cfg.HealthInterval < minHealthDuration {
		return errors.Wrapf(ErrInvalidHealthDuration, "health interval %s: must be at least %s", cfg.HealthInterval, minHealthDuration)
	}

	if cfg.HealthTimeout < minHealthDuration {
		return errors.Wrapf(ErrInvalidHealthDuration, "health timeout %s: must be at least %s", cfg.HealthTimeout, minHealthDuration)
	}

	if cfg.HealthTimeout >= cfg.HealthInterval {
		return ErrInvalidHealthTiming
	}

	return nil
}

// validateBindAddress checks that addr is a valid IP address or a syntactically
// valid hostname per RFC 1123. DNS resolution is intentionally not performed
// here because it would introduce a startup dependency on DNS infrastructure,
// which may not be available in early boot (e.g., before CNI starts).
func validateBindAddress(addr string) error {
	if addr == "" {
		return errors.Wrap(ErrInvalidBindAddress, "must not be empty")
	}

	if net.ParseIP(addr) != nil {
		return nil
	}

	if !isValidHostname(addr) {
		return errors.Wrapf(ErrInvalidBindAddress, "%s: must be a valid IP address or hostname", addr)
	}

	return nil
}

const maxHostnameLength = 253

func isValidHostname(host string) bool {
	// Trim a single trailing dot (FQDN notation per RFC 1035).
	host = strings.TrimSuffix(host, ".")

	if host == "" || len(host) > maxHostnameLength {
		return false
	}

	for label := range strings.SplitSeq(host, ".") {
		if !isValidHostnameLabel(label) {
			return false
		}
	}

	return true
}

const maxLabelLength = 63

func isValidHostnameLabel(label string) bool {
	if label == "" || len(label) > maxLabelLength {
		return false
	}

	if label[0] == '-' || label[len(label)-1] == '-' {
		return false
	}

	for _, c := range label {
		if !isHostnameChar(c) {
			return false
		}
	}

	return true
}

func isHostnameChar(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-'
}

func validateEndpoints(endpoints []string) error {
	for _, endpoint := range endpoints {
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil || host == "" || port == "" {
			return errors.Wrapf(ErrInvalidEndpoint, "endpoint %q", endpoint)
		}

		portNum, parseErr := strconv.Atoi(port)
		if parseErr != nil || portNum < minPort || portNum > maxPort {
			return errors.Wrapf(ErrInvalidEndpoint, "endpoint %q: port must be a number between 1 and 65535", endpoint)
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
