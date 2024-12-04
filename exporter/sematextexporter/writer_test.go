package sematextexporter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"bytes"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"github.com/sirupsen/logrus"
)

func TestSematextHTTPWriterBatchOptimizeTags(t *testing.T) {
	batch := &sematextHTTPWriterBatch{
		sematextHTTPWriter: &sematextHTTPWriter{
			logger: common.NoopLogger{},
		},
	}

	for _, testCase := range []struct {
		name         string
		m            map[string]string
		expectedTags []tag
	}{
		{
			name:         "empty map",
			m:            map[string]string{},
			expectedTags: []tag{},
		},
		{
			name: "one tag",
			m: map[string]string{
				"k": "v",
			},
			expectedTags: []tag{
				{"k", "v"},
			},
		},
		{
			name: "empty tag key",
			m: map[string]string{
				"": "v",
			},
			expectedTags: []tag{},
		},
		{
			name: "empty tag value",
			m: map[string]string{
				"k": "",
			},
			expectedTags: []tag{},
		},
		{
			name: "seventeen tags",
			m: map[string]string{
				"k00": "v00", "k01": "v01", "k02": "v02", "k03": "v03", "k04": "v04", "k05": "v05", "k06": "v06", "k07": "v07", "k08": "v08", "k09": "v09", "k10": "v10", "k11": "v11", "k12": "v12", "k13": "v13", "k14": "v14", "k15": "v15", "k16": "v16",
			},
			expectedTags: []tag{
				{"k00", "v00"}, {"k01", "v01"}, {"k02", "v02"}, {"k03", "v03"}, {"k04", "v04"}, {"k05", "v05"}, {"k06", "v06"}, {"k07", "v07"}, {"k08", "v08"}, {"k09", "v09"}, {"k10", "v10"}, {"k11", "v11"}, {"k12", "v12"}, {"k13", "v13"}, {"k14", "v14"}, {"k15", "v15"}, {"k16", "v16"},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotTags := batch.optimizeTags(testCase.m)
			assert.Equal(t, testCase.expectedTags, gotTags)
		})
	}
}

func TestSematextHTTPWriterBatchMaxPayload(t *testing.T) {
	for _, testCase := range []struct {
		name                   string
		payloadMaxLines        int
		payloadMaxBytes        int
		expectMultipleRequests bool
	}{
		{
			name:                   "default",
			payloadMaxLines:        10_000,
			payloadMaxBytes:        10_000_000,
			expectMultipleRequests: false,
		},
		{
			name:                   "limit-lines",
			payloadMaxLines:        1,
			payloadMaxBytes:        10_000_000,
			expectMultipleRequests: true,
		},
		{
			name:                   "limit-bytes",
			payloadMaxLines:        10_000,
			payloadMaxBytes:        1,
			expectMultipleRequests: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var httpRequests []*http.Request

			mockHTTPService := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				httpRequests = append(httpRequests, r)
			}))
			t.Cleanup(mockHTTPService.Close)

			batch := &sematextHTTPWriterBatch{
				sematextHTTPWriter: &sematextHTTPWriter{
					encoderPool: sync.Pool{
						New: func() any {
							e := new(lineprotocol.Encoder)
							e.SetLax(false)
							e.SetPrecision(lineprotocol.Nanosecond)
							return e
						},
					},
					httpClient:      &http.Client{},
					writeURL:        mockHTTPService.URL,
					payloadMaxLines: testCase.payloadMaxLines,
					payloadMaxBytes: testCase.payloadMaxBytes,
					logger:          common.NoopLogger{},
				},
			}

			err := batch.EnqueuePoint(context.Background(), "m", map[string]string{"k": "v"}, map[string]any{"f": int64(1)}, time.Unix(1, 0), 0)
			require.NoError(t, err)
			err = batch.EnqueuePoint(context.Background(), "m", map[string]string{"k": "v"}, map[string]any{"f": int64(2)}, time.Unix(2, 0), 0)
			require.NoError(t, err)
			err = batch.WriteBatch(context.Background())
			require.NoError(t, err)

			if testCase.expectMultipleRequests {
				assert.Len(t, httpRequests, 2)
			} else {
				assert.Len(t, httpRequests, 1)
			}
		})
	}
}

func TestSematextHTTPWriterBatchEnqueuePointEmptyTagValue(t *testing.T) {
	var recordedRequest *http.Request
	var recordedRequestBody []byte
	noopHTTPServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {

		if assert.Nil(t, recordedRequest) {
			var err error
			recordedRequest = r
			recordedRequestBody, err = io.ReadAll(r.Body)
			if err != nil {
				fmt.Println(err.Error())
			}
		}
	}))
	t.Cleanup(noopHTTPServer.Close)
	nowTime := time.Unix(1628605794, 318000000)

	sematextWriter, err := newSematextHTTPWriter(
		new(common.NoopLogger),
		&Config{
			MetricsConfig: MetricsConfig{
				MetricsEndpoint:noopHTTPServer.URL ,
				AppToken: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			},
			Region:    "US",
		},
		componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	sematextWriter.httpClient = noopHTTPServer.Client()
	sematextWriterBatch := sematextWriter.NewBatch()

	err = sematextWriterBatch.EnqueuePoint(
		context.Background(),
		"m",
		map[string]string{"k": "v", "empty": ""},
		map[string]any{"f": int64(1)},        
		nowTime,                               
		common.InfluxMetricValueTypeUntyped) 
	require.NoError(t, err)

	err = sematextWriterBatch.WriteBatch(context.Background())

	require.NoError(t, err)

	if assert.NotNil(t, recordedRequest) {
		expected:= fmt.Sprintf("m,k=v,os.host=%s,token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx f=1i 1628605794318000000", sematextWriter.hostname)
		assert.Equal(t, expected, strings.TrimSpace(string(recordedRequestBody)))
	}
}

func TestComposeWriteURLDoesNotPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		cfg := &Config{
			Region: "us",
			MetricsConfig: MetricsConfig{
				MetricsEndpoint: "http://localhost:8080",
				MetricsSchema: "telegraf-prometheus-v2",
			},
		}
		_, err := composeWriteURL(cfg)
		assert.NoError(t, err)
	})

	assert.NotPanics(t, func() {
		cfg := &Config{
			Region: "eu",
			MetricsConfig: MetricsConfig{
				MetricsEndpoint: "http://localhost:8080",
				MetricsSchema: "telegraf-prometheus-v2",
			},

		}
		_, err := composeWriteURL(cfg)
		assert.NoError(t, err)
	})
}
func TestNewFlatWriter(t *testing.T) {
	config := &Config{
		LogsConfig: LogsConfig{
		LogMaxAge:     7,
		LogMaxBackups: 5,
		LogMaxSize:    10,
		},
		
	}
	writer, err := NewFlatWriter("test.log", config)
	assert.NoError(t, err)
	assert.NotNil(t, writer)
	assert.NotNil(t, writer.l)
}
func TestFlatWriterWrite(t *testing.T) {
	var buf bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&buf)
	writer := &FlatWriter{l: logger}

	message := "test message"
	writer.Write(message)

	assert.Contains(t, buf.String(), message)
}
func TestInitRotate(t *testing.T) {
	hook, err := InitRotate("test.log", 7, 5, 10, &FlatFormatter{})
	assert.NoError(t, err)
	assert.NotNil(t, hook)
}
func TestNewRotateFile(t *testing.T) {
	config := RotateFileConfig{
		Filename:   "test.log",
		MaxSize:    10,
		MaxBackups: 5,
		MaxAge:     7,
		Level:      logrus.InfoLevel,
		Formatter:  &FlatFormatter{},
	}

	hook, err := NewRotateFile(config)
	assert.NoError(t, err)
	assert.NotNil(t, hook)
}
func TestRotateFileFire(t *testing.T) {
	var buf bytes.Buffer

	hook := &RotateFile{
		Config: RotateFileConfig{
			Filename:   "test.log",
			MaxSize:    10,
			MaxBackups: 5,
			MaxAge:     7,
			Level:      logrus.InfoLevel,
			Formatter:  &logrus.TextFormatter{},
		},
		logWriter: &buf,
	}

	entry := &logrus.Entry{
		Message: "test entry",
		Level:   logrus.InfoLevel,
	}

	err := hook.Fire(entry)
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "test entry")
}
func TestRotateFileLevels(t *testing.T) {
	hook := &RotateFile{
		Config: RotateFileConfig{
			Level: logrus.WarnLevel,
		},
	}

	expectedLevels := logrus.AllLevels[:logrus.WarnLevel+1]
	assert.Equal(t, expectedLevels, hook.Levels())
}