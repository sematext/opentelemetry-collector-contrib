// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"
)

// artificialDocType designates a synthetic doc type for ES documents

type group struct {
	client *elasticsearch.Client
	token  string
}

type client struct {
	clients  map[string]group
	config   *Config
	logger   *zap.Logger
	writer   FlatWriter
	hostname string
}

// Client represents a minimal interface client implementation has to satisfy.
type Client interface {
	Bulk(body any, config *Config) error
}

// NewClient creates a new instance of ES client that internally stores a reference
// to both event and log receivers.
func newClient(config *Config, logger *zap.Logger, writer FlatWriter) (Client, error) {
	clients := make(map[string]group)

	// Client for shipping to logsene
	if config.LogsConfig.AppToken != "" {
		c, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: []string{config.LogsEndpoint},
		})
		if err != nil {
			logger.Error("Failed to create Elasticsearch client", zap.Error(err))
			return nil, err
		}
		clients[config.LogsEndpoint] = group{
			client: c,
			token:  config.LogsConfig.AppToken,
		}
	}

	hostname, err := os.Hostname()
	if err != nil {
		logger.Warn("Could not retrieve hostname", zap.Error(err))
	}

	return &client{
		clients:  clients,
		config:   config,
		logger:   logger,
		writer:   writer,
		hostname: hostname,
	}, nil
}

// Bulk processes a batch of documents and sends them to the specified LogsEndpoint.
func (c *client) Bulk(body any, config *Config) error {
	grp, ok := c.clients[config.LogsEndpoint]
	if !ok {
		return fmt.Errorf("no client known for %s endpoint", config.LogsEndpoint)
	}

	var bulkBuffer bytes.Buffer

	if reflect.TypeOf(body).Kind() == reflect.Slice {
		v := reflect.ValueOf(body)
		for i := 0; i < v.Len(); i++ {
			doc := v.Index(i).Interface()
			if docMap, ok := doc.(map[string]any); ok {
				docMap["os.host"] = c.hostname
			}

			meta := map[string]map[string]string{
				"index": {"_index": grp.token},
			}
			metaBytes, _ := json.Marshal(meta)
			docBytes, _ := json.Marshal(doc)

			bulkBuffer.Write(metaBytes)
			bulkBuffer.WriteByte('\n')
			bulkBuffer.Write(docBytes)
			bulkBuffer.WriteByte('\n')
		}
	}

	if bulkBuffer.Len() > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		req := esapi.BulkRequest{
			Body: bytes.NewReader(bulkBuffer.Bytes()),
		}

		res, err := req.Do(ctx, grp.client)
		if err != nil {
			c.logger.Error("Bulk request failed", zap.Error(err))
			return err
		}
		defer res.Body.Close()

		if res.IsError() {
			c.logger.Error("Bulk request returned an error", zap.String("response", res.String()))
			return fmt.Errorf("bulk request error: %s", res.String())
		}

		c.logger.Info("Bulk request successful", zap.String("response", res.String()))
	}

	return nil
}

// writePayload writes a formatted payload along with its status to the configured writer.
func (c *client) writePayload(payload string, status string) {
	if c.config.WriteEvents.Load() {
		c.writer.Write(formatl(payload, status))
	} else {
		c.logger.Debug("WriteEvents disabled", zap.String("payload", payload), zap.String("status", status))
	}
}

// Formatl delimits and formats the response returned by the receiver.
func formatl(payload string, status string) string {
	s := strings.TrimLeft(status, "\n")
	i := strings.Index(s, "\n")
	if i > 0 {
		s = fmt.Sprintf("%s...", s[:i])
	}
	return fmt.Sprintf("%s %s", strings.TrimSpace(payload), s)
}
