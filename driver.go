package flink

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
)

func init() {
	sql.Register("flink", &sqlDriver{})
}

type sqlDriver struct{}

func (d *sqlDriver) OpenConnector(dsn string) (driver.Connector, error) {
	baseDSN := dsn
	var properties map[string]string
	if trimmed, rawQuery, found := strings.Cut(baseDSN, "?"); found {
		baseDSN = trimmed
		if rawQuery != "" {
			parsed, err := parseURIParameters(rawQuery)
			if err != nil {
				return nil, fmt.Errorf("flink: invalid DSN properties: %w", err)
			}
			properties = parsed
		}
	}
	options := []ConnOption{WithGatewayURL(baseDSN)}
	if len(properties) != 0 {
		options = append(options, WithProperties(properties))
	}
	connector, err := NewConnector(options...)
	if err != nil {
		return nil, err
	}
	return connector, nil
}

func (d *sqlDriver) Open(dsn string) (driver.Conn, error) {
	conn, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return conn.Connect(context.Background())
}
