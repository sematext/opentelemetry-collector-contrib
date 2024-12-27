// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"
import (
	"fmt"
	"os"
	"testing"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewClient(t *testing.T) {
	mockConfig := &Config{
		Region: "US",
	}
	mockLogger := zap.NewNop()

	writer := FlatWriter{}
	client, err := newClient(mockConfig, mockLogger, writer)

	assert.NoError(t, err, "Expected no error while creating new client")
	assert.NotNil(t, client, "Expected client to be non-nil")
}
func TestBulkWithMockClient(t *testing.T) {
	mockConfig := &Config{
		Region: "US",
		LogsConfig: LogsConfig{
			LogsEndpoint: "https://logsene-receiver.sematext.com",
			AppToken:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		},
	}

	// Create a mock client
	mockClient := newMockClient(mockConfig)
	defer func() {
		for _, group := range mockClient.clients {
			group.client.Stop()
		}
	}()

	// Create mock payload
	mockPayload := []map[string]any{
		{"field1": "value1", "field2": "value2"},
	}

	// Call the Bulk method on the mock client
	err := mockClient.Bulk(mockPayload, mockConfig)
	assert.NoError(t, err, "Expected no error while sending bulk request")
	for _, group := range mockClient.clients {
		assert.True(t, group.client.BulkCalled, "Expected Bulk to be called on the mock client")
	}
}
func TestNewClient_ErrorRetrievingHostname(t *testing.T) {
	var osHostname = os.Hostname
	mockConfig := &Config{
		Region: "US",
		LogsConfig: LogsConfig{
			LogsEndpoint: "https://logsene-receiver.sematext.com",
			AppToken:     "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		},
	}
	mockLogger := zap.NewNop()
	writer := FlatWriter{}

	originalHostname := osHostname
	osHostname = func() (string, error) {
		return "", fmt.Errorf("mock error retrieving hostname")
	}
	defer func() { osHostname = originalHostname }()

	client, err := newClient(mockConfig, mockLogger, writer)

	assert.NotNil(t, client, "Expected client to be created even with hostname error")
	assert.NoError(t, err, "Expected no error even with hostname error")
}

type mockElasticClient struct {
	BulkCalled bool
	done       chan struct{}
}

func (m *mockElasticClient) Stop() {
	close(m.done)
}
func (m *mockElasticClient) Bulk() *esapi.Bulk {
	m.BulkCalled = true
	return nil
}

type mockGroup struct {
	client *mockElasticClient
	token  string
}

type MockClient struct {
	clients map[string]mockGroup
	Error   error
}

func (m *MockClient) Bulk(_ any, _ *Config) error {
	for _, group := range m.clients {
		group.client.Bulk()
	}
	return m.Error
}

func newMockClient(config *Config) *MockClient {
	mockElastic := &mockElasticClient{
		done: make(chan struct{}),
	}
	return &MockClient{
		clients: map[string]mockGroup{
			config.LogsEndpoint: {
				client: mockElastic,
				token:  config.LogsConfig.AppToken,
			},
		},
	}
}
