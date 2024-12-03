// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"fmt"
	"strings"
	a"sync/atomic"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	confighttp.ClientConfig   `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	// Region specifies the Sematext region the user is operating in
	// Options:
	// - EU
	// - US
	Region string `mapstructure:"region"`
	// MetricsConfig defines the configuration specific to metrics 
	MetricsConfig `mapstructure:"metrics"`
	// LogsConfig defines the configuration specific to logs
	LogsConfig `mapstructure:"logs"`
}

type MetricsConfig struct {
	// App token is the token of Sematext Monitoring App to which you want to send the metrics.
	AppToken         string                     `mapstructure:"app_token"`
	// MetricsEndpoint specifies the endpoint for receiving metrics in Sematext
	MetricsEndpoint  string                     `mapstructure:"metrics_endpoint"`
	QueueSettings    exporterhelper.QueueConfig `mapstructure:"sending_queue"`
	// MetricsSchema indicates the metrics schema to emit to line protocol.
	// Default: telegraf-prometheus-v2
	MetricsSchema    string                     `mapstructure:"metrics_schema"`
	// PayloadMaxLines is the maximum number of line protocol lines to POST in a single request.
	PayloadMaxLines  int                        `mapstructure:"payload_max_lines"`
	// PayloadMaxBytes is the maximum number of line protocol bytes to POST in a single request.
	PayloadMaxBytes  int                        `mapstructure:"payload_max_bytes"`
}
type LogsConfig struct {
	// App token is the token of Sematext Monitoring App to which you want to send the logs.
	AppToken string `mapstructure:"app_token"`
	// LogsEndpoint specifies the endpoint for receiving logs in Sematext
	LogsEndpoint string  `mapstructure:"logs_endpoint"`
	// LogRequests determines whether request tracking is enabled
	LogRequests bool `mapstructure:"logs_requests"`
	// LogMaxAge is the max number of days to retain old log files
	LogMaxAge int `mapstructure:"logs_max_age"`
	// LogMaxBackups is the maximum number of old log files to retain.
	LogMaxBackups int `mapstructure:"logs_max_backups"`
	// LogMaxSize is the maximum size in megabytes of the log file before it gets rotated
	LogMaxSize int `mapstructure:"logs_max_size"`
	// WriteEvents determines if events are logged
	WriteEvents a.Bool
}


// Validate checks for invalid or missing entries in the configuration.
func (cfg *Config) Validate() error {
	if strings.ToLower(cfg.Region) != "eu" && strings.ToLower(cfg.Region) != "us" && strings.ToLower(cfg.Region) != "custom"{
		return fmt.Errorf("invalid region: %s. please use either 'EU' or 'US'", cfg.Region)
	}
	if len(cfg.MetricsConfig.AppToken) != 36{
		return fmt.Errorf("invalid metrics app_token: %s. app_token should be 36 characters", cfg.MetricsConfig.AppToken)
	}
	if len(cfg.LogsConfig.AppToken) != 36{
		return fmt.Errorf("invalid logs app_token: %s. app_token should be 36 characters", cfg.LogsConfig.AppToken)
	}
	if strings.ToLower(cfg.Region) == "eu" {
		cfg.MetricsEndpoint ="https://spm-receiver.eu.sematext.com"
		cfg.LogsEndpoint ="https://logsene-receiver.eu.sematext.com"
	}
	if strings.ToLower(cfg.Region) == "us"{
		cfg.MetricsEndpoint ="https://spm-receiver.sematext.com"
		cfg.LogsEndpoint = "https://logsene-receiver.sematext.com"
	}

	return nil
}

// Bool provides an atomic boolean type.
type Bool struct{ u Uint32 }
// Uint32 provides an atomic uint32 type.
type Uint32 struct{ value uint32 }
// Load gets the value of atomic boolean.
func (b *Bool) Load() bool { return b.u.Load() == 1 }
// Load get the value of atomic integer.
func (u *Uint32) Load() uint32 { return a.LoadUint32(&u.value) }
