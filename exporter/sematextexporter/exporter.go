package sematextexporter

import (
	"context"
	// "fmt"
	"time"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
)

type sematextLogsExporter struct {
	config *Config
	client Client
	logger *logrus.Logger
}

func NewExporter(cfg *Config, set exporter.Settings) *sematextLogsExporter {
	logger := logrus.New()
	logger.SetFormatter(&FlatFormatter{})

	// Initialize Sematext client
	client, err := NewClient(cfg, logger, FlatWriter{})
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


func (e *sematextLogsExporter) pushLogsData(ctx context.Context, logs plog.Logs) error {
	// Convert logs to Bulk API payload
	bulkPayload, err := convertLogsToBulkPayload(logs)
	if err != nil {
		e.logger.Errorf("Failed to convert logs: %v", err)
		return err
	}

	// Send logs using the Sematext client
	if err := e.client.Bulk(bulkPayload, e.config); err != nil {
		e.logger.Errorf("Failed to send logs to Sematext: %v", err)
		return err
	}

	return nil
}
func convertLogsToBulkPayload(logs plog.Logs) ([]map[string]interface{}, error) {
	var bulkPayload []map[string]interface{}

	resourceLogs := logs.ResourceLogs()

	// Iterate through logs to prepare the Bulk payload
	for i := 0; i < resourceLogs.Len(); i++ {
		scopeLogs := resourceLogs.At(i).ScopeLogs()
		for j := 0; j < scopeLogs.Len(); j++ {
			logRecords := scopeLogs.At(j).LogRecords()
			for k := 0; k < logRecords.Len(); k++ {
				record := logRecords.At(k)

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
func (e *sematextLogsExporter) Start(ctx context.Context, host component.Host) error {
	e.logger.Info("Starting Sematext Logs Exporter...")
	return nil
}

func (e *sematextLogsExporter) Shutdown(ctx context.Context) error {
	e.logger.Info("Shutting down Sematext Logs Exporter...")
	return nil
}