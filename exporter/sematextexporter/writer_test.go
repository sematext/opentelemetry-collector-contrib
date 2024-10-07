package sematextexporter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
)

func Test_sematextHTTPWriterBatch_optimizeTags(t *testing.T) {
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

func Test_sematextHTTPWriterBatch_maxPayload(t *testing.T) {
	for _, testCase := range []struct {
		name            string
		payloadMaxLines int
		payloadMaxBytes int
		expectMultipleRequests bool
	}{
		{
			name:            "default",
			payloadMaxLines: 10_000,
			payloadMaxBytes: 10_000_000,
			expectMultipleRequests: false,
		}, 
		{
			name:            "limit-lines",
			payloadMaxLines: 1,
			payloadMaxBytes: 10_000_000,
			expectMultipleRequests: true,
		}, 
		{
			name:            "limit-bytes",
			payloadMaxLines: 10_000,
			payloadMaxBytes: 1,
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

//This is the test failing:
// Error:      	Received unexpected error:
//         	            	Permanent error: line protocol write returned "400 Bad Request" "ERROR: Can't parse line m,k=v,os.host=ADMINs-MacBook-Pro.local,token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx f=1i 1000000002000 . Invalid timestamp format (must be 19 digit, nanosecond-precision Unix time), found `1000000002000`.; \n"
//         	Test:       	Test_sematextHTTPWriterBatch_EnqueuePoint_emptyTagValue
func Test_sematextHTTPWriterBatch_EnqueuePoint_emptyTagValue(t *testing.T) {
	var recordedRequest *http.Request
	var recordedRequestBody []byte
	noopHTTPServer := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if assert.Nil(t, recordedRequest) {
			recordedRequest = r
			recordedRequestBody, _ = io.ReadAll(r.Body)
		}
	}))
	t.Cleanup(noopHTTPServer.Close)

	nowTime := time.Unix(1000, 2000)

	sematextWriter, err := newSematextHTTPWriter(
		new(common.NoopLogger),
		&Config{
			ClientConfig: confighttp.ClientConfig{
				Endpoint: noopHTTPServer.URL,
			},
			App_token: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			Region: "US",
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
	//What the metrics line should look like
	//appStats,token=00000000-1111-2222-3333-444444444444,
	//os.host=host001,service=registration user.requests.count=10i,user.requests.time=327i 1628605794318000000
	if assert.NotNil(t, recordedRequest) {
		assert.Equal(t, "m,k=v,os.host=ADMINs-MacBook-Pro.local,token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx f=1i 1000000002000", strings.TrimSpace(string(recordedRequestBody)))
	}
}


func Test_composeWriteURL_doesNotPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		cfg := &Config{
			Region: "us",
			ClientConfig: confighttp.ClientConfig{
				Endpoint: "http://localhost:8080",
			},
			MetricsSchema: "otel-v1",
		}
		_, err := composeWriteURL(cfg)
		assert.NoError(t, err)
	})

	assert.NotPanics(t, func() {
		cfg := &Config{
			Region: "eu",
			ClientConfig: confighttp.ClientConfig{
				Endpoint: "http://localhost:8080",
			},
			MetricsSchema: "otel-v1",
		}
		_, err := composeWriteURL(cfg)
		assert.NoError(t, err)
	})
}
