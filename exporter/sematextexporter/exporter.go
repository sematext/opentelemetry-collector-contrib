// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type sematextLogsExporter struct {
	config *Config
	client Client
	logger *logrus.Logger
}

// newExporter creates a new instance of the sematextLogsExporter.
func newExporter(cfg *Config, set exporter.Settings) *sematextLogsExporter {
	logger := logrus.New()
	logger.SetFormatter(&FlatFormatter{})

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
	bulkPayload, err := convertLogsToBulkPayload(logs, e.config.LogsConfig.AppToken)
	if err != nil {
		e.logger.Errorf("Failed to convert logs: %v", err)
		return err
	}

	// Debug: Print the bulk payload
	for _, payload := range bulkPayload {
		fmt.Printf("Bulk payload: %v\n", payload)
	}

	// Send the bulk payload to Sematext
	if err := e.client.Bulk(bulkPayload, e.config); err != nil {
		e.logger.Errorf("Failed to send logs to Sematext: %v", err)
		return err
	}

	return nil
}

// convertLogsToBulkPayload converts OpenTelemetry log data into a bulk payload for Sematext.
func convertLogsToBulkPayload(logs plog.Logs, appToken string) ([]map[string]interface{}, error) {
	var bulkPayload []map[string]interface{}

	resourceLogs := logs.ResourceLogs()

	// Iterate through logs to prepare the Bulk payload
	for i := 0; i < resourceLogs.Len(); i++ {
		scopeLogs := resourceLogs.At(i).ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			logRecords := scopeLogs.At(j).LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				record := logRecords.At(k)

				// Add metadata for indexing
				meta := map[string]interface{}{
					"index": map[string]interface{}{
						"_index": appToken,
					},
				}
				bulkPayload = append(bulkPayload, meta)

				// Build the log entry
				logEntry := map[string]interface{}{
					"@timestamp": record.Timestamp().AsTime().Format(time.RFC3339),
					"message":    record.Body().AsString(),
					"severity":   record.SeverityText(),
				}
				bulkPayload = append(bulkPayload, logEntry)
			}
		}
	}

	return bulkPayload, nil
}

// Start initializes the Sematext Logs Exporter.
func (e *sematextLogsExporter) Start(_ context.Context, _ component.Host) error {
	if e.client == nil {
		return fmt.Errorf("sematext client is not initialized")
	}
	e.logger.Info("Starting Sematext Logs Exporter...")
	return nil
}

// Shutdown gracefully shuts down the Sematext Logs Exporter.
func (e *sematextLogsExporter) Shutdown(_ context.Context) error {
	if e.logger == nil {
        return fmt.Errorf("logger is not initialized")
    }
	e.logger.Info("Shutting down Sematext Logs Exporter...")
	return nil
}
