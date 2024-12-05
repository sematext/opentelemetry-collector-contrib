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
	"sync"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	fs "github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"gopkg.in/natefinch/lumberjack.v2"
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
	hostname           string
	token              string
	logger             common.Logger
}

func newSematextHTTPWriter(logger common.Logger, config *Config, telemetrySettings component.TelemetrySettings) (*sematextHTTPWriter, error) {
	writeURL, err := composeWriteURL(config)
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("could not detect hostname: %w", err)
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
		telemetrySettings: telemetrySettings,
		writeURL:          writeURL,
		payloadMaxLines:   config.PayloadMaxLines,
		payloadMaxBytes:   config.PayloadMaxBytes,
		logger:            logger,
		hostname:          hostname,
		token:             config.MetricsConfig.AppToken,
	}, nil
}

func composeWriteURL(config *Config) (string, error) {
	writeURL, err := url.Parse(config.MetricsEndpoint)
	if err != nil {
		return "", err
	}
	if writeURL.Path == "" || writeURL.Path == "/" {
		writeURL, err = writeURL.Parse("write?db=metrics")
		if err != nil {
			return "", err
		}
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
	tags["token"] = b.token
	tags["os.host"] = b.hostname

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

// WriteBatch sends the internal line protocol buffer to Sematext.
func (b *sematextHTTPWriterBatch) WriteBatch(ctx context.Context) error {
	if b.encoder == nil {
		fmt.Println("Encoder is nil")
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
		fmt.Println("Error sending request:", err)
		return err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if err = res.Body.Close(); err != nil {
		return err
	}

	switch res.StatusCode {
	case 200, 204:
		break
	case 500:
		return fmt.Errorf("line protocol write returned %q %q", res.Status, string(body))
	default:
		return consumererror.NewPermanent(fmt.Errorf("line protocol write returned %q %q", res.Status, string(body)))
	}

	return nil
}

type tag struct {
	k, v string
}

// optimizeTags sorts tags by key and removes tags with empty keys or values
func (b *sematextHTTPWriterBatch) optimizeTags(m map[string]string) []tag {
	// Ensure token and os.host tags are always present
	m["token"] = b.token
	m["os.host"] = b.hostname

	// Limit to 18 other tags, excluding token and os.host
	if len(m) > 20 {
		count := 0
		for k := range m {
			if k != "token" && k != "os.host" {
				count++
				if count > 18 {
					delete(m, k)
				}
			}
		}
	}

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

// Logs Support

// FlatWriter writes a raw message to log file.
type FlatWriter struct {
	l *logrus.Logger
}

// NewFlatWriter creates a new instance of flat writer.
func NewFlatWriter(f string, c *Config) (*FlatWriter, error) {
	l := logrus.New()
	l.Out = io.Discard

	hook, err := InitRotate(
		f,
		c.LogMaxAge,
		c.LogMaxBackups,
		c.LogMaxSize,
		&FlatFormatter{},
	)
	w := &FlatWriter{
		l: l,
	}
	if err != nil {
		return w, err
	}
	l.AddHook(hook)
	return w, nil
}

// Write dumps a raw message to log file.
func (w *FlatWriter) Write(message string) {
	w.l.Print(message)
}

// InitRotate returns a new fs hook that enables log file rotation with specified pattern,
// maximum size/TTL for existing log files.
func InitRotate(filePath string, maxAge, maxBackups, maxSize int, f logrus.Formatter) (logrus.Hook, error) {
	h, err := NewRotateFile(RotateFileConfig{
		Filename:   filePath,
		MaxAge:     maxAge,
		MaxBackups: maxBackups,
		MaxSize:    maxSize,
		Level:      logrus.DebugLevel,
		Formatter:  f,
	})
	if err != nil {
		// if we can't initialize file log rotation, configure logger
		// without rotation capabilities
		var pathMap fs.PathMap = make(map[logrus.Level]string, 0)
		for _, ll := range logrus.AllLevels {
			pathMap[ll] = filePath
		}
		return fs.NewHook(pathMap, f), fmt.Errorf("unable to initialize log rotate: %w", err)
	}
	return h, nil
}

// RotateFileConfig is the configuration for the rotate file hook.
type RotateFileConfig struct {
	Filename   string
	MaxSize    int
	MaxBackups int
	MaxAge     int
	Level      logrus.Level
	Formatter  logrus.Formatter
}

// RotateFile represents the rotate file hook.
type RotateFile struct {
	Config    RotateFileConfig
	logWriter io.Writer
}

// NewRotateFile builds a new rotate file hook.
func NewRotateFile(config RotateFileConfig) (logrus.Hook, error) {
	hook := RotateFile{
		Config: config,
	}
	hook.logWriter = &lumberjack.Logger{
		Filename:   config.Filename,
		MaxSize:    config.MaxSize,
		MaxBackups: config.MaxBackups,
		MaxAge:     config.MaxAge,
	}
	return &hook, nil
}

// Fire is called by logrus when it is about to write the log entry.
func (hook *RotateFile) Fire(entry *logrus.Entry) error {
	b, err := hook.Config.Formatter.Format(entry)
	if err != nil {
		return err
	}
	if _, err := hook.logWriter.Write(b); err != nil {
		return err
	}
	return nil
}

// Levels determines log levels that for which the logs are written.
func (hook *RotateFile) Levels() []logrus.Level {
	return logrus.AllLevels[:hook.Config.Level+1]
}
