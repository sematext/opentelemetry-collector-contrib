// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter/internal/metadata"
)

const appToken string = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

// NewFactory creates a factory for the Sematext metrics exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	cfg := &Config{
		ClientConfig: confighttp.ClientConfig{
			Timeout: 5 * time.Second,
			Headers: map[string]configopaque.String{
				"User-Agent": "OpenTelemetry -> Sematext",
			},
		},
		MetricsConfig: MetricsConfig{
			MetricsEndpoint: "https://spm-receiver.sematext.com",
			MetricsSchema:   common.MetricsSchemaTelegrafPrometheusV2.String(),
			AppToken:        appToken,
			QueueSettings:   exporterhelper.NewDefaultQueueConfig(),
			PayloadMaxLines: 1_000,
			PayloadMaxBytes: 300_000,
		},
		LogsConfig: LogsConfig{
			LogsEndpoint:  "https://logsene-receiver.sematext.com",
			AppToken:      appToken,
		},
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		Region:        "us",
	}
	return cfg
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	config component.Config,
) (exporter.Metrics, error) {
	cfg := config.(*Config)

	// Initialize the logger for Sematext
	logger := newZapSematextLogger(set.Logger)

	// Create a writer for sending metrics to Sematext
	writer, err := newSematextHTTPWriter(logger, cfg, set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("failed to create Sematext HTTP writer: %w", err)
	}
	schema, found := common.MetricsSchemata[cfg.MetricsSchema]
	if !found {
		return nil, fmt.Errorf("schema '%s' not recognized", cfg.MetricsSchema)
	}

	expConfig := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
	expConfig.Logger = logger
	expConfig.Writer = writer
	expConfig.Schema = schema
	exp, err := otel2influx.NewOtelMetricsToLineProtocol(expConfig)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		exp.WriteMetrics,
		exporterhelper.WithQueue(cfg.MetricsConfig.QueueSettings),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithStart(writer.Start),
		exporterhelper.WithShutdown(writer.Shutdown),
	)
}

// createLogsExporter creates a new logs exporter for Sematext.
func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	cf := cfg.(*Config)

	set.Logger.Info("Creating Sematext Logs Exporter")

	exporter := newExporter(cf, set)

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exporter.pushLogsData, // Function to process and send logs
		exporterhelper.WithRetry(cf.BackOffConfig),
		exporterhelper.WithStart(exporter.Start),
	)
}
