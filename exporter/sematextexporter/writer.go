// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

var _ otel2influx.InfluxWriter = (*sematextHTTPWriter)(nil)

type sematextHTTPWriter struct {
	encoderPool sync.Pool
	httpClient  *http.Client

	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	writeURL           string
	payloadMaxLines    int
	payloadMaxBytes    int

	logger common.Logger
}

func newSematextHTTPWriter(logger common.Logger, config *Config, telemetrySettings component.TelemetrySettings) (*sematextHTTPWriter, error) {
	writeURL, err := composeWriteURL(config)
	if err != nil {
		return nil, err
	}

	return &sematextHTTPWriter{
		encoderPool: sync.Pool{
			New: func() any {
				e := new(lineprotocol.Encoder)
				e.SetLax(false)
				e.SetPrecision(lineprotocol.Nanosecond)
				return e
			},
		},
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  telemetrySettings,
		writeURL:           writeURL,
		payloadMaxLines:    config.PayloadMaxLines,
		payloadMaxBytes:    config.PayloadMaxBytes,
		logger:             logger,
	}, nil
}

func composeWriteURL(config *Config) (string, error) {
	writeURL, err := url.Parse(config.ClientConfig.Endpoint)
	if err != nil {
		return "", err
	}
	if writeURL.Path == "" || writeURL.Path == "/" {
		// Assuming a default path for posting metrics
		writeURL, err = writeURL.Parse("api/v1/metrics")
		if err != nil {
			return "", err
		}
	}

	// Set any query parameters if needed, depending on the structure of the API
	queryValues := writeURL.Query()
	queryValues.Set("precision", "ns")

	// Add App token as authorization in the headers if available
	if config.App_token != "" {
		if config.ClientConfig.Headers == nil {
			config.ClientConfig.Headers = make(map[string]configopaque.String, 1)
		}
		config.ClientConfig.Headers["Authorization"] = "Bearer " + config.App_token
	}

	// Encode any additional query parameters (e.g., metrics schema if needed)
	queryValues.Set("metrics_schema", config.MetricsSchema)

	writeURL.RawQuery = queryValues.Encode()

	return writeURL.String(), nil
}


// Start implements component.StartFunc
func (w *sematextHTTPWriter) Start(ctx context.Context, host component.Host) error {
	httpClient, err := w.httpClientSettings.ToClient(ctx, host, w.telemetrySettings)
	if err != nil {
		return err
	}
	w.httpClient = httpClient
	return nil
}

func (w *sematextHTTPWriter) NewBatch() otel2influx.InfluxWriterBatch {
	return newSematextHTTPWriterBatch(w)
}

var _ otel2influx.InfluxWriterBatch = (*sematextHTTPWriterBatch)(nil)

type sematextHTTPWriterBatch struct {
	*sematextHTTPWriter
	encoder      *lineprotocol.Encoder
	payloadLines int
}

func newSematextHTTPWriterBatch(w *sematextHTTPWriter) *sematextHTTPWriterBatch {
	return &sematextHTTPWriterBatch{
		sematextHTTPWriter: w,
	}
}

// EnqueuePoint emits a set of line protocol attributes (metrics, tags, fields, timestamp)
// to the internal line protocol buffer.
// If the buffer is full, it will be flushed by calling WriteBatch.
func (b *sematextHTTPWriterBatch) EnqueuePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]any, ts time.Time, _ common.InfluxMetricValueType) error {
	if b.encoder == nil {
		b.encoder = b.encoderPool.Get().(*lineprotocol.Encoder)
	}

	b.encoder.StartLine(measurement)
	for _, tag := range b.optimizeTags(tags) {
		b.encoder.AddTag(tag.k, tag.v)
	}
	for k, v := range b.convertFields(fields) {
		b.encoder.AddField(k, v)
	}
	b.encoder.EndLine(ts)

	if err := b.encoder.Err(); err != nil {
		b.encoder.Reset()
		b.encoder.ClearErr()
		b.encoderPool.Put(b.encoder)
		b.encoder = nil
		return consumererror.NewPermanent(fmt.Errorf("failed to encode point: %w", err))
	}

	b.payloadLines++
	if b.payloadLines >= b.payloadMaxLines || len(b.encoder.Bytes()) >= b.payloadMaxBytes {
		if err := b.WriteBatch(ctx); err != nil {
			return err
		}
	}

	return nil
}

// WriteBatch sends the internal line protocol buffer to InfluxDB.
func (b *sematextHTTPWriterBatch) WriteBatch(ctx context.Context) error {
	if b.encoder == nil {
		return nil
	}

	defer func() {
		b.encoder.Reset()
		b.encoder.ClearErr()
		b.encoderPool.Put(b.encoder)
		b.encoder = nil
		b.payloadLines = 0
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.writeURL, bytes.NewReader(b.encoder.Bytes()))
	if err != nil {
		return consumererror.NewPermanent(err)
	}

	res, err := b.httpClient.Do(req)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if err = res.Body.Close(); err != nil {
		return err
	}
	switch res.StatusCode / 100 {
	case 2: // Success
		break
	case 5: // Retryable error
		return fmt.Errorf("line protocol write returned %q %q", res.Status, string(body))
	default: // Terminal error
		return consumererror.NewPermanent(fmt.Errorf("line protocol write returned %q %q", res.Status, string(body)))
	}

	return nil
}

type tag struct {
	k, v string
}

// optimizeTags sorts tags by key and removes tags with empty keys or values
func (b *sematextHTTPWriterBatch) optimizeTags(m map[string]string) []tag {
	tags := make([]tag, 0, len(m))
	for k, v := range m {
		switch {
		case k == "":
			b.logger.Debug("empty tag key")
		case v == "":
			b.logger.Debug("empty tag value", "key", k)
		default:
			tags = append(tags, tag{k, v})
		}
	}
	sort.Slice(tags, func(i, j int) bool {
		return tags[i].k < tags[j].k
	})
	return tags
}

func (b *sematextHTTPWriterBatch) convertFields(m map[string]any) (fields map[string]lineprotocol.Value) {
	fields = make(map[string]lineprotocol.Value, len(m))
	for k, v := range m {
		if k == "" {
			b.logger.Debug("empty field key")
		} else if lpv, ok := lineprotocol.NewValue(v); !ok {
			b.logger.Debug("invalid field value", "key", k, "value", v)
		} else {
			fields[k] = lpv
		}
	}
	return
}
