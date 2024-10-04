// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"fmt"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque" // Added to handle sensitive data
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

type Config struct {
	confighttp.ClientConfig   `mapstructure:",squash"`
	QueueSettings             exporterhelper.QueueConfig `mapstructure:"sending_queue"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	// Sensitive data (app_token) should use configopaque.String for security.
	App_token configopaque.String `mapstructure:"app_token"`

	// Region specifies the Sematext region the user is operating in
	// Options:
	// - EU
	// - US
	Region string `mapstructure:"region"`

	// MetricsSchema indicates the metrics schema to emit to line protocol.
	// Options:
	// - otel-v1
	MetricsSchema string `mapstructure:"metrics_schema"`

	// PayloadMaxLines is the maximum number of line protocol lines to POST in a single request.
	PayloadMaxLines int `mapstructure:"payload_max_lines"`
	// PayloadMaxBytes is the maximum number of line protocol bytes to POST in a single request.
	PayloadMaxBytes int `mapstructure:"payload_max_bytes"`
}

// Validate checks for invalid or missing entries in the configuration.
func (cfg *Config) Validate() error {
	if cfg.MetricsSchema != "otel-v1" {
		return fmt.Errorf("invalid metrics schema: %s", cfg.MetricsSchema)
	}
	if cfg.Region != "EU" && cfg.Region != "US" {
		fmt.Println("Error: Invalid region. Please use either 'EU' or 'US'.")
	}
	if cfg.App_token.String() == "" {
		return fmt.Errorf("app_token is required")
	}

	return nil
}
