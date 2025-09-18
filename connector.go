package flink

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/http"
)

type ConnConfig struct {
	GatewayURL string
	Client     *http.Client
	APIVersion string
	Properties map[string]string
}

func WithDefaults() *ConnConfig {
	return &ConnConfig{
		GatewayURL: "http://localhost:8081",
		Client:     http.DefaultClient,
		APIVersion: "v3",
		Properties: make(map[string]string),
	}
}

type ConnOption func(c *ConnConfig)

func WithGatewayURL(gatewayUrl string) ConnOption {
	return func(c *ConnConfig) {
		c.GatewayURL = gatewayUrl
	}
}

func WithClient(client *http.Client) ConnOption {
	return func(c *ConnConfig) {
		c.Client = client
	}
}

func WithAPIVersion(apiVersion string) ConnOption {
	return func(c *ConnConfig) {
		c.APIVersion = apiVersion
	}
}

func WithProperties(properties map[string]string) ConnOption {
	return func(c *ConnConfig) {
		if c.Properties == nil {
			c.Properties = make(map[string]string)
		}
		for k, v := range properties {
			c.Properties[k] = v
		}
	}
}

type connector struct {
	client        GatewayClient
	sessionHandle string
	config        *ConnConfig
	properties    map[string]string
}

// NewConnector creates a connection that can be used with `sql.OpenDB()`.
// This is an easier way to set up the DB instead of having to construct a DSN string.
func NewConnector(options ...ConnOption) (driver.Connector, error) {
	cfg := WithDefaults()
	for _, opt := range options {
		opt(cfg)
	}
	mergedProps, err := mergeProperties(cfg.GatewayURL, cfg.Properties)
	if err != nil {
		return nil, fmt.Errorf("flink: error while merging connector properties: %w", err)
	}
	flinkClient, err := NewClient(cfg.GatewayURL, cfg.Client, cfg.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("flink: error while creating flink client: %w", err)
	}
	return &connector{client: flinkClient, config: cfg, properties: mergedProps}, nil
}

// Connect returns a new Conn bound to this Connector's client and session.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.sessionHandle == "" {
		var err error
		c.sessionHandle, err = c.client.OpenSession(ctx, &OpenSessionRequest{
			Properties: c.properties,
		})
		if err != nil {
			return nil, fmt.Errorf("flink: failed to open session: %w", err)
		}
	}
	return &flinkConn{client: c.client, sessionHandle: c.sessionHandle}, nil
}

// Close closes the gateway session associated with this connector.
func (c *connector) Close() error {
	if c.sessionHandle != "" {
		c.client.CloseSession(context.Background(), c.sessionHandle)
	}
	return nil
}

func (c *connector) Driver() driver.Driver {
	return &sqlDriver{}
}
