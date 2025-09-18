package flink

import "testing"

func TestSQLDriver_OpenConnector_ParsesProperties(t *testing.T) {
	driver := &sqlDriver{}

	conn, err := driver.OpenConnector("http://example.com:8083?opt1=val1&opt2=val2")
	if err != nil {
		t.Fatalf("OpenConnector() error = %v", err)
	}
	flinkConn, ok := conn.(*connector)
	if !ok {
		t.Fatalf("OpenConnector() returned %T, want *connector", conn)
	}
	if got := flinkConn.config.GatewayURL; got != "http://example.com:8083" {
		t.Fatalf("GatewayURL = %q, want %q", got, "http://example.com:8083")
	}
	wantProps := map[string]string{"opt1": "val1", "opt2": "val2"}
	if len(flinkConn.config.Properties) != len(wantProps) {
		t.Fatalf("Properties length = %d, want %d", len(flinkConn.config.Properties), len(wantProps))
	}
	for k, v := range wantProps {
		if got := flinkConn.config.Properties[k]; got != v {
			t.Fatalf("Properties[%q] = %q, want %q", k, got, v)
		}
	}
}

func TestSQLDriver_OpenConnector_InvalidProperties(t *testing.T) {
	driver := &sqlDriver{}

	if _, err := driver.OpenConnector("http://example.com:8083?opt1"); err == nil {
		t.Fatalf("OpenConnector() error = nil, want error")
	}
}
