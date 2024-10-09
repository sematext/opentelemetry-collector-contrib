// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"github.com/influxdata/influxdb-observability/common"
	"go.uber.org/zap"
)

type zapInfluxLogger struct {
	*zap.SugaredLogger
}

func newZapSematextLogger(logger *zap.Logger) common.Logger {
	return &common.ErrorLogger{
		Logger: &zapInfluxLogger{
			logger.Sugar(),
		},
	}
}

func (l zapInfluxLogger) Debug(msg string, kv ...any) {
	l.SugaredLogger.Debugw(msg, kv...)
}
