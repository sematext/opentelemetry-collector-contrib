// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sematextexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/sematextexporter"
import (
	"fmt"
	"reflect"
	"strings"

	"github.com/olivere/elastic"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"time"

	json "github.com/json-iterator/go"
)

const (
	// artificialDocType designates a syntenic doc type for ES documents
	artificialDocType = "_doc" 
)

type group struct {
	client *elastic.Client
	token  string
}

type client struct {
	clients map[string]group
	config  *Config
	logger  *logrus.Logger
	writer  FlatWriter
}

// Client represents a minimal interface client implementation has to satisfy.
type Client interface {
	Bulk(body interface{},config *Config) error
}

// NewClient creates a new instance of ES client that internally stores a reference
// to both, event and log receivers.
func NewClient(config *Config, logger *logrus.Logger, writer FlatWriter) (Client, error) {
	clients := make(map[string]group)

	// client for shipping to logsene
	if config.LogsConfig.AppToken != "" {
		c, err := elastic.NewClient(elastic.SetURL(config.LogsEndpoint), elastic.SetSniff(false), elastic.SetHealthcheckTimeout(time.Second*2))
		if err != nil {
			return nil, err
		}
		// clients := map[string]group{}
		clients[config.LogsEndpoint] = group{
            client: c,
            token:  config.LogsConfig.AppToken,
        }
		}

	return &client{
		clients: clients,
		config:  config,
		logger:  logger,
		writer:  writer,
	}, nil
}

func (c *client) Bulk(body interface{}, config *Config) error {
    // Lookup for client by endpoint
    if grp, ok := c.clients[config.LogsEndpoint]; ok {
        bulkRequest := grp.client.Bulk()

        // Dynamically process the body as a slice
        if reflect.TypeOf(body).Kind() == reflect.Slice {
            v := reflect.ValueOf(body)
            for i := 0; i < v.Len(); i++ {
                req := elastic.NewBulkIndexRequest().
                    Index(grp.token).
                    Type(artificialDocType).
                    Doc(v.Index(i).Interface())
                bulkRequest.Add(req)
            }
        }

        if bulkRequest.NumberOfActions() > 0 {
            // Serialize the payload for debugging or printing
            payloadBytes, err := json.Marshal(body)
            if err != nil {
                return fmt.Errorf("failed to serialize payload: %w", err)
            }

            // Print or log the payload
            fmt.Printf("Payload being sent to Sematext:\n%s\n", string(payloadBytes))

            if c.config.LogRequests {
                c.logger.Infof("Sending bulk to %s", config.LogsEndpoint)
            }

            ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
            defer cancel()

            // Send the payload
            res, err := bulkRequest.Do(ctx)
            if err != nil {
                c.writePayload(string(payloadBytes), err.Error())
                return err
            }

            // Check for errors in the response
            if res.Errors {
                for _, item := range res.Failed() {
                    if item.Error != nil {
                        c.logger.Errorf("Document %s failed to index: %s - %s", item.Id, item.Error.Type, item.Error.Reason)
                    }
                }
            }

            c.writePayload(string(payloadBytes), "200")
            return nil
        }
    }
    return fmt.Errorf("no client known for %s endpoint", config.LogsEndpoint)
}


func (c *client) writePayload(payload string, status string) {
	if c.config.WriteEvents.Load() {
		c.writer.Write(Formatl(payload, status))
	}
}
// Formatl delimits and formats the response returned by receiver.
func Formatl(payload string, status string) string {
	s := strings.TrimLeft(status, "\n")
	i := strings.Index(s, "\n")
	if i > 0 {
		s = fmt.Sprintf("%s...", s[:i])
	}
	return fmt.Sprintf("%s %s", strings.TrimSpace(payload), s)
}