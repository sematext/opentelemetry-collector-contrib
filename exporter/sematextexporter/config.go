// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"fmt"
	"strings"
	"time"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exporterbatcher"
)

type Config struct {
	confighttp.ClientConfig   `mapstructure:",squash"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`
	// Region specifies the Sematext region the user is operating in
	// Options:
	// - EU
	// - US
	Region string `mapstructure:"region"`
	MetricsConfig `mapstructure:"metrics"`
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
	AppToken string `mapstructure:"app_token"`
	LogsEndpoint string  `mapstructure:"logs_endpoint"`
	LogsMapping LogMapping `mapstructure:"logs_mapping"`
	LogsFlushSettings FlushSettings `mapstructure:"logs_flush_settings"`
	LogstashFormat          LogstashFormatSettings `mapstructure:"logstash_format"`
	// TelemetrySettings contains settings useful for testing/debugging purposes
	// This is experimental and may change at any time.
	TelemetrySettings `mapstructure:"telemetry"`

	// Batcher holds configuration for batching requests based on timeout
	// and size-based thresholds.
	//
	// Batcher is unused by default, in which case Flush will be used.
	// If Batcher.Enabled is non-nil (i.e. batcher::enabled is specified),
	// then the Flush will be ignored even if Batcher.Enabled is false.
	Batcher BatcherConfig `mapstructure:"batcher"`
}
// BatcherConfig holds configuration for exporterbatcher.
//
// This is a slightly modified version of exporterbatcher.Config,
// to enable tri-state Enabled: unset, false, true.
type BatcherConfig struct {
	// Enabled indicates whether to enqueue batches before sending
	// to the exporter. If Enabled is specified (non-nil),
	// then the exporter will not perform any buffering itself.
	Enabled *bool `mapstructure:"enabled"`

	// FlushTimeout sets the time after which a batch will be sent regardless of its size.
	FlushTimeout time.Duration `mapstructure:"flush_timeout"`

	exporterbatcher.MinSizeConfig `mapstructure:",squash"`
	exporterbatcher.MaxSizeConfig `mapstructure:",squash"`
}

type TelemetrySettings struct {
	LogRequestBody  bool `mapstructure:"log_request_body"`
	LogResponseBody bool `mapstructure:"log_response_body"`
}

type LogstashFormatSettings struct {
	Enabled         bool   `mapstructure:"enabled"`
	PrefixSeparator string `mapstructure:"prefix_separator"`
	DateFormat      string `mapstructure:"date_format"`
}
// FlushSettings defines settings for configuring the write buffer flushing
// policy in the Elasticsearch exporter. The exporter sends a bulk request with
// all events already serialized into the send-buffer.
type FlushSettings struct {
	// Bytes sets the send buffer flushing limit.
	Bytes int `mapstructure:"bytes"`

	// Interval configures the max age of a document in the send buffer.
	Interval time.Duration `mapstructure:"interval"`
}
type LogMapping struct {
	//Will refine this comment later but from the research i did there are 4 different modes used in Elastisearch Exporter
	// I believe we need MappingECS but for now i will just leave all the options
    Mode string `mapstructure:"mode"`
}

type MappingMode int
const (
	MappingNone MappingMode = iota
	MappingECS
	MappingOTel
	MappingRaw
) 
func (m MappingMode) String() string {
	switch m {
	case MappingNone:
		return ""
	case MappingECS:
		return "ecs"
	case MappingOTel:
		return "otel"
	case MappingRaw:
		return "raw"
	default:
		return ""
	}
}
var mappingModes = func() map[string]MappingMode {
	table := map[string]MappingMode{}
	for _, m := range []MappingMode{
		MappingNone,
		MappingECS,
		MappingOTel,
		MappingRaw,
	} {
		table[strings.ToLower(m.String())] = m
	}

	// config aliases
	table["no"] = MappingNone
	table["none"] = MappingNone

	return table
}()
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
		cfg.LogsEndpoint ="logsene-receiver.eu.sematext.com"
	}
	if strings.ToLower(cfg.Region) == "us"{
		cfg.MetricsEndpoint ="https://spm-receiver.sematext.com"
		cfg.LogsEndpoint = "logsene-receiver.sematext.com"
	}
	if _, ok := mappingModes[cfg.LogsMapping.Mode]; !ok {
		return fmt.Errorf("unknown mapping mode %q", cfg.LogsMapping.Mode)
	}

	return nil
}
func (cfg *Config) MappingMode() MappingMode {
	return mappingModes[cfg.LogsMapping.Mode]
}