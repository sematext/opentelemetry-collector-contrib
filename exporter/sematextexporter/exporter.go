// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type sematextLogsExporter struct {
	config *Config
	client Client
	logger *zap.Logger
}

// newExporter creates a new instance of the sematextLogsExporter.
func newExporter(cfg *Config, set exporter.Settings) *sematextLogsExporter {
	logger := zap.NewNop()

	// Initialize Sematext client
	client, err := newClient(cfg, logger, FlatWriter{})
	if err != nil {
		set.Logger.Error("Failed to create Sematext client", zap.Error(err))
		return nil
	}

	return &sematextLogsExporter{
		config: cfg,
		client: client,
		logger: logger,
	}
}

// pushLogsData processes and sends log data to Sematext in bulk.
func (e *sematextLogsExporter) pushLogsData(_ context.Context, logs plog.Logs) error {
	// Convert logs to bulk payload
	bulkPayload := convertLogsToBulkPayload(logs)

	// Debug: Print the bulk payload
	for _, payload := range bulkPayload {
		fmt.Printf("Bulk payload: %v\n", payload)
	}

	// Send the bulk payload to Sematext
	if err := e.client.Bulk(bulkPayload, e.config); err != nil {
		e.logger.Error("Failed to send logs to Sematext", zap.Error(err))
		return err
	}

	return nil
}

// convertLogsToBulkPayload converts OpenTelemetry log data into a bulk payload for Sematext.
func convertLogsToBulkPayload(logs plog.Logs) []map[string]any {
	var bulkPayload []map[string]any

	resourceLogs := logs.ResourceLogs()

	// Iterate through logs to prepare the Bulk payload
	for i := 0; i < resourceLogs.Len(); i++ {
		scopeLogs := resourceLogs.At(i).ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			logRecords := scopeLogs.At(j).LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				record := logRecords.At(k)
				// Extract severity and provide a default value if empty
				severity := record.SeverityText()
				if severity == "" {
					severity = "INFO" // Default severity if missing
				}
				// Build the log entry
				logEntry := map[string]any{
					"@timestamp": record.Timestamp().AsTime().Format(time.RFC3339),
					"message":    record.Body().AsString(),
					"severity":   severity,
				}
				bulkPayload = append(bulkPayload, logEntry)
			}
		}
	}

	return bulkPayload
}

// Start initializes the Sematext Logs Exporter.
func (e *sematextLogsExporter) Start(_ context.Context, _ component.Host) error {
	// Create a new logger with a FlatFormatter
	logger := zap.NewNop()

	// Initialize the Sematext client
	client, err := newClient(e.config, logger, FlatWriter{})
	if err != nil {
		e.logger.Error("Failed to initialize Sematext client", zap.Error(err))
		return fmt.Errorf("failed to initialize Sematext client: %w", err)
	}
	if client == nil {
		e.logger.Error("Sematext client is not initialized (nil)")
		return fmt.Errorf("sematext client is not initialized")
	}
	e.client = client
	e.logger = logger

	e.logger.Info("Sematext Logs Exporter successfully started")
	return nil
}
