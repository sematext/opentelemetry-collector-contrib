// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"
import (
	"bytes"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

func TestFlatFormatter_Format(t *testing.T) {
	formatter := &FlatFormatter{}

	mockTime := time.Date(2024, 12, 4, 10, 30, 45, 0, time.UTC)
	entry := &logrus.Entry{
		Time:    mockTime,
		Message: "Test log message",
	}

	formatted, err := formatter.Format(entry)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "2024-12-04T10:30:45Z Test log message\n"

	if !bytes.Equal(formatted, []byte(expected)) {
		t.Errorf("unexpected output:\n got: %q\n want: %q", string(formatted), expected)
	}
}
