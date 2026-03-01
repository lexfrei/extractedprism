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
	defaultBindAddress       = "127.0.0.1"
	defaultBindPort          = 7445
	defaultHealthPort        = 7446
	defaultHealthInterval    = 20 * time.Second
	defaultHealthTimeout     = 15 * time.Second
	defaultLogLevel          = "info"
	defaultLivenessInterval  = 5 * time.Second
	defaultLivenessThreshold = 15 * time.Second

	minPort = 1
	maxPort = 65535
)

// Sentinel errors for configuration validation.
var (
	ErrNoEndpoints              = errors.New("no endpoints configured")
	ErrInvalidEndpoint          = errors.New("invalid endpoint")
	ErrInvalidPort              = errors.New("invalid port number")
	ErrPortConflict             = errors.New("bind port and health port must differ")
	ErrInvalidHealthTiming      = errors.New("health timeout must be less than health interval")
	ErrInvalidBindAddress       = errors.New("invalid bind address")
	ErrInvalidHealthDuration    = errors.New("invalid health duration")
	ErrInvalidLogLevel          = errors.New("invalid log level")
	ErrInvalidLivenessDuration  = errors.New("invalid liveness duration")
	ErrInvalidLivenessTiming    = errors.New("liveness threshold must be greater than liveness interval")
	ErrInvalidHealthBindAddress = errors.New("invalid health bind address")
)

const minDuration = 1 * time.Second

func isValidLogLevel(level string) bool {
	switch level {
	case "debug", "info", "warn", "error", "dpanic", "panic", "fatal":
		return true
	default:
		return false
	}
}

// Config holds all configuration for the extractedprism proxy.
type Config struct {
	BindAddress       string
	BindPort          int
	HealthPort        int
	HealthBindAddress string
	Endpoints         []string
	HealthInterval    time.Duration
	HealthTimeout     time.Duration
	EnableDiscovery   bool
	LogLevel          string
	LivenessInterval  time.Duration
	LivenessThreshold time.Duration
}

// NewBaseConfig returns a Config populated with sensible defaults for optional
// fields. Required fields such as Endpoints must be set by the caller before
// the config passes Validate.
func NewBaseConfig() *Config {
	return &Config{
		BindAddress:       defaultBindAddress,
		BindPort:          defaultBindPort,
		HealthPort:        defaultHealthPort,
		HealthInterval:    defaultHealthInterval,
		HealthTimeout:     defaultHealthTimeout,
		EnableDiscovery:   true,
		LogLevel:          defaultLogLevel,
		LivenessInterval:  defaultLivenessInterval,
		LivenessThreshold: defaultLivenessThreshold,
	}
}

// Validate checks that the configuration is valid.
// Note: Validate mutates cfg.HealthBindAddress, defaulting it to cfg.BindAddress
// when empty. Callers should not assume Config is unchanged after Validate.
func (cfg *Config) Validate() error {
	if len(cfg.Endpoints) == 0 {
		return ErrNoEndpoints
	}

	err := validateBindAddress(cfg.BindAddress)
	if err != nil {
		return err
	}

	if cfg.HealthBindAddress == "" {
		cfg.HealthBindAddress = cfg.BindAddress
	}

	err = validateHealthBindAddress(cfg.HealthBindAddress)
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

	if cfg.HealthInterval < minDuration {
		return errors.Wrapf(ErrInvalidHealthDuration, "health interval %s: must be at least %s", cfg.HealthInterval, minDuration)
	}

	if cfg.HealthTimeout < minDuration {
		return errors.Wrapf(ErrInvalidHealthDuration, "health timeout %s: must be at least %s", cfg.HealthTimeout, minDuration)
	}

	if cfg.HealthTimeout >= cfg.HealthInterval {
		return ErrInvalidHealthTiming
	}

	if !isValidLogLevel(cfg.LogLevel) {
		return errors.Wrapf(ErrInvalidLogLevel, "%q: must be one of debug, info, warn, error, dpanic, panic, fatal", cfg.LogLevel)
	}

	if cfg.LivenessInterval < minDuration {
		return errors.Wrapf(ErrInvalidLivenessDuration,
			"liveness interval %s: must be at least %s", cfg.LivenessInterval, minDuration)
	}

	if cfg.LivenessThreshold < minDuration {
		return errors.Wrapf(ErrInvalidLivenessDuration,
			"liveness threshold %s: must be at least %s", cfg.LivenessThreshold, minDuration)
	}

	if cfg.LivenessThreshold <= cfg.LivenessInterval {
		return ErrInvalidLivenessTiming
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

	if !isValidHost(addr) {
		return errors.Wrapf(ErrInvalidBindAddress, "%s: must be a valid IP address or hostname", addr)
	}

	return nil
}

func validateHealthBindAddress(addr string) error {
	if addr == "" {
		return errors.Wrap(ErrInvalidHealthBindAddress, "must not be empty")
	}

	if !isValidHost(addr) {
		return errors.Wrapf(ErrInvalidHealthBindAddress, "%s: must be a valid IP address or hostname", addr)
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
		err := ValidateEndpoint(endpoint)
		if err != nil {
			return err
		}
	}

	return nil
}

// ValidateEndpoint checks that a single endpoint string is a valid host:port
// pair with a valid IP or hostname and a port in range 1-65535.
func ValidateEndpoint(endpoint string) error {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" || port == "" {
		return errors.Wrapf(ErrInvalidEndpoint, "endpoint %q", endpoint)
	}

	if !isValidHost(host) {
		return errors.Wrapf(ErrInvalidEndpoint, "endpoint %q: invalid host", endpoint)
	}

	portNum, parseErr := strconv.Atoi(port)
	if parseErr != nil || portNum < minPort || portNum > maxPort {
		return errors.Wrapf(ErrInvalidEndpoint, "endpoint %q: port must be a number between 1 and 65535", endpoint)
	}

	return nil
}

// isValidHost returns true if host is a valid IP address or RFC 1123 hostname.
func isValidHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}

	return isValidHostname(host)
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
