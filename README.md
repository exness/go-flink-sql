# go-flink-sql

A lightweight Go driver for the **Apache Flink SQL Gateway**.


## Quick start

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/exness/go-flink-sql"
)

type NullString sql.NullString

type order struct {
	ID        int64
	Item      string
	CreatedAt time.Time
	Customer  customer
	Note      *string
}

type customer struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Age       int    `json:"age"`
}

func (c *customer) Scan(src any) error {
	data, ok := src.([]byte)
	if !ok {
		return fmt.Errorf("flink: unexpected customer scan type %T", src)
	}
	err := json.Unmarshal(data, c)
	return err
}

func ptrToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func main() {
	connector, err := flink.NewConnector(
		flink.WithGatewayURL("http://localhost:8083"),
		flink.WithProperties(map[string]string{
			"execution.runtime-mode": "STREAMING",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ddl := `CREATE TABLE orders (
		id INT NOT NULL,
		item STRING NOT NULL,
		created_at TIMESTAMP(6) NOT NULL,
		customer ROW<
			first_name STRING not null,
			last_name STRING not null,
			age INT not null
		> NOT NULL,
		notes STRING
	) WITH (
		'connector' = 'datagen',
		'rows-per-second' = '5',
		'fields.id.kind' = 'sequence',
		'fields.id.start' = '1',
		'fields.id.end' = '100',
		'fields.item.length' = '12',
		'fields.customer.first_name.length' = '8',
		'fields.customer.last_name.length' = '10',
		'fields.customer.age.min' = '21',
		'fields.customer.age.max' = '65',
		'fields.notes.length' = '12'
	)`
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		log.Fatal(err)
	}

	rows, err := db.QueryContext(ctx, `
		SELECT
			id,
			item,
			created_at,
			customer,
			notes AS note
		FROM orders
	`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var seen int
	for rows.Next() {
		var o order
		var note NullString
		if err := rows.Scan(&o.ID, &o.Item, &o.CreatedAt, &o.Customer, &note); err != nil {
			log.Fatal(err)
		}
		if note.Valid {
			val := note.String
			o.Note = &val
		} else {
			o.Note = nil
		}

		fmt.Printf(
			"%d\t%s\t%s\t%s %s (%d)\t%s\n",
			o.ID,
			o.Item,
			o.CreatedAt.Format(time.RFC3339Nano),
			o.Customer.FirstName,
			o.Customer.LastName,
			o.Customer.Age,
			ptrToString(o.Note),
		)

		seen++
		if seen == 5 {
			break
		}
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
}
```
> Tip: Complex types such as `ROW`, `MAP`, `ARRAY`, and binary data arrive as `[]byte`. Decode the JSON or binary payload to suit your application.

### Low-level REST access

If you need features beyond `database/sql`, reuse the exported client in `gateway.go`:

```go
client, err := flink.NewClient("http://localhost:8083", nil, "v3")
if err != nil {
    log.Fatal(err)
}

status, err := client.GetOperationStatus(ctx, "session-handle", "operation-handle")
if err != nil {
    log.Fatal(err)
}
fmt.Println("current status:", status)
```

## Type mapping (summary)

Non‑nullable columns map to concrete Go types; nullable columns map to `sql.Null*` wrappers:

| Flink Type              | Go (NOT NULL) | Go (NULLABLE)   |
|-------------------------|---------------|-----------------|
| TINYINT / SMALLINT / INT| int64         | sql.NullInt64   |
| BIGINT / INTERVAL       | int64         | sql.NullInt64   |
| FLOAT                   | float64       | sql.NullFloat64 |
| DOUBLE                  | float64       | sql.NullFloat64 |
| BOOLEAN                 | bool          | sql.NullBool    |
| CHAR / VARCHAR / STRING | string        | sql.NullString  |
| DATE / TIME / TIMESTAMP | time.Time     | sql.NullTime    |
| BINARY / VARBINARY      | []byte        | []byte          |
| ROW / MAP / ARRAY       | []byte        | []byte          |

---

## Development & tests
- Requires a reachable Flink SQL Gateway; tests spin up a local cluster with Testcontainers.

---

## License
MIT (see LICENSE)
