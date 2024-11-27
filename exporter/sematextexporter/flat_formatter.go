// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"bytes"
	"github.com/sirupsen/logrus"
	"time"
)

const defaultTimestampFormat = time.RFC3339

// FlatFormatter is the formatter for printing log lines in raw format.
type FlatFormatter struct{}

// Format prints the raw log message.
func (f *FlatFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var b bytes.Buffer
	b.WriteString(entry.Time.Format(defaultTimestampFormat) + " " + entry.Message)
	b.WriteByte('\n')
	return b.Bytes(), nil
}