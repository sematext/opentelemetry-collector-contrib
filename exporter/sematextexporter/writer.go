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
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
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
	hostname string
	token string
	logger common.Logger
}

func newSematextHTTPWriter(logger common.Logger, config *Config, telemetrySettings component.TelemetrySettings) (*sematextHTTPWriter, error) {
	writeURL, err := composeWriteURL(config)
	if err != nil {
		return nil, err
	}
		// Detect the hostname
		hostname, err := os.Hostname()
		if err != nil {
			logger.Debug("could not determine hostname, using 'unknown' as os.host")
			hostname = "unknown"
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
		hostname:           hostname,
		token: config.App_token,
	}, nil
}

func composeWriteURL(config *Config) (string, error) {
	var baseURL string
	if strings.ToLower(config.Region)=="us"{
		baseURL = "https://spm-receiver.sematext.com/write"
	}else if strings.ToLower(config.Region) == "eu"{
		baseURL = "https://spm-receiver.eu.sematext.com/write"
	}else{
		return "", fmt.Errorf("invalid region. Please use either 'eu' or 'us'")
	}
	writeURL, err := url.Parse(baseURL + "?db=metrics")
	if err != nil {
		return "", err
	}
	queryValues := writeURL.Query()

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
	// Add token and os.host tags
	if tags == nil {
		tags = make(map[string]string)
	}
	tags["token"] = b.token   // Add the Sematext token
	tags["os.host"] = b.hostname // You can make this dynamic to detect the hostname

	// Start encoding the measurement
	b.encoder.StartLine(measurement)
	for _, tag := range b.optimizeTags(tags) {
		b.encoder.AddTag(tag.k, tag.v)
	}
	for k, v := range b.convertFields(fields) {
		b.encoder.AddField(k, v)
	}
	b.encoder.EndLine(ts)

	// Log the encoded data after encoding the point
	fmt.Printf("Encoded data: %s\n", b.encoder.Bytes())

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
		fmt.Println("Encoder is nil")
		return nil
	}

	// Log the bytes in the encoder before sending
	fmt.Printf("Sending encoded data: %s\n", b.encoder.Bytes())

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

	// Log the request details before sending
	fmt.Println("Request URL:", req.URL)
	fmt.Printf("Request Headers: %+v\n", req.Header)
	fmt.Println("Request Body (before sending):", string(b.encoder.Bytes()))

	// Send the request
	res, err := b.httpClient.Do(req)

	// Check if the request was successfully sent
	if err != nil {
		fmt.Println("Error sending request:", err)
		return err
	}

	// Log the status of the response
	fmt.Println("Response Status:", res.Status)

	// Read and log the response body
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	fmt.Println("Response Body:", string(body))

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
