// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"
import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

func TestConvertLogsToBulkPayload(t *testing.T) {
	// Create a plog.Logs object
	logs := plog.NewLogs()
	resourceLogs := logs.ResourceLogs().AppendEmpty()
	scopeLogs := resourceLogs.ScopeLogs().AppendEmpty()
	logRecord := scopeLogs.LogRecords().AppendEmpty()

	// Set log record fields
	timestamp := time.Now().UTC() // Ensure timestamp is in UTC
	logRecord.SetTimestamp(pcommon.NewTimestampFromTime(timestamp))
	logRecord.Body().SetStr("This is a test log message")
	logRecord.SetSeverityText("ERROR")

	// Call the function
	result := convertLogsToBulkPayload(logs)

	// Prepare the expected result
	expected := []map[string]any{
		{
			"@timestamp": timestamp.Format(time.RFC3339), // Ensure expected timestamp is in RFC3339 format
			"message":    "This is a test log message",
			"severity":   "ERROR",
		},
	}

	// Assert that the result matches the expected value
	assert.Equal(t, expected, result)
}
func TestNewExporter(t *testing.T) {
	// Mock configuration
	mockConfig := &Config{
		// Add any necessary fields for testing
		Region: "US",
	}
	logger := zap.NewNop()
	// Mock exporter settings with a zap.Logger
	mockSettings := exporter.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
		// Use a no-op logger for testing
	}

	// Call the function
	exporter := newExporter(mockConfig, mockSettings)

	// Ensure the exporter is not nil
	assert.NotNil(t, exporter)

	// Validate the exporter fields
	assert.Equal(t, mockConfig, exporter.config, "Exporter config does not match")
	assert.NotNil(t, exporter.client, "Exporter client should not be nil")
	assert.NotNil(t, exporter.logger, "Exporter logger should not be nil")

	// Ensure the logger has the correct formatter
	flatFormatter, ok := exporter.logger.Formatter.(*FlatFormatter)
	assert.True(t, ok, "Exporter logger should use FlatFormatter")
	assert.NotNil(t, flatFormatter, "FlatFormatter should be properly initialized")
}
