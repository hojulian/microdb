// Package client represents a MicroDB client
package client

// MicroDB client is a "database/sql" driver.

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/nats-io/stan.go"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
	"github.com/hojulian/microdb/microdb"
	mquery "github.com/hojulian/microdb/query"
)

var (
	_ driver.Conn           = &Conn{}
	_ driver.QueryerContext = &Conn{}
	_ driver.ExecerContext  = &Conn{}
	_ driver.QueryerContext = &Conn{}
)

// Conn is a connection to MicroDB system. It is not used concurrently by multiple goroutines.
type Conn struct {
	sc     stan.Conn
	sqc    driver.Conn
	tables map[string]stan.Subscription
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepare method not implemented")
}

func (c *Conn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("begin method not implemented")
}

func (c *Conn) Close() error {
	for _, t := range c.tables {
		if err := t.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe table updates: %w", err)
		}
	}

	if err := c.sqc.Close(); err != nil {
		return fmt.Errorf("failed to close local sqlite3 connection: %w", err)
	}

	return nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, err := mquery.Query(query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}
	if q.QueryType != mquery.QueryTypeSelect {
		return nil, fmt.Errorf("unsupported query type: %w", err)
	}

	switch q.DestinationType {
	case mquery.DestinationTypeLocal:
		rs, err := c.sqc.(driver.QueryerContext).QueryContext(ctx, query, args)
		if err != nil {
			return nil, fmt.Errorf("failed to query local sqlite3: %w", err)
		}
		return rs, nil

	case mquery.DestinationTypeOrigin:
		d, err := microdb.GetDataOrigin(q.GetDestinationTable())
		if err != nil {
			return nil, fmt.Errorf("failed to get data origin for table: %w", err)
		}

		db, err := d.GetDB()
		if err != nil {
			return nil, fmt.Errorf("failed to get database connector for data origin: %w", err)
		}

		dbc, err := db.Conn(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create database connection for data origin: %w", err)
		}

		rs, err := dbc.QueryContext(ctx, query, normalizeDriverValues(args)...)
		if err != nil {
			return nil, fmt.Errorf("failed to query data origin: %w", err)
		}

		return &dRows{
			sRows: rs,
		}, nil
	}

	return nil, errors.New("unsupported destination type")
}

func normalizeDriverValues(ds []driver.NamedValue) []interface{} {
	is := make([]interface{}, 0, len(ds))
	for _, d := range ds {
		is = append(is, d.Value)
	}
	return is
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	q, err := mquery.Query(query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}
	if q.QueryType == mquery.QueryTypeSelect {
		return nil, errors.New("for select query, please use QueryContext")
	}

	req := &pb.QueryRequest{
		Query: q.SQL(),
		Args:  pb.MarshalDriverValues(args),
	}

	p, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal write request: %w", err)
	}

	dest := q.GetDestinationTable()
	destTopic := fmt.Sprintf("%s_write", dest)

	// Forward to querier directly, it will figure out the type conversion.
	msg, err := c.sc.NatsConn().RequestWithContext(ctx, destTopic, p)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	var res pb.WriteQueryReply
	if err := proto.Unmarshal(msg.Data, &res); err != nil {
		return nil, fmt.Errorf("failed to parse reply: %w", err)
	}

	if !res.GetOk() {
		return nil, fmt.Errorf("failed to execute query: %s", res.GetMsg())
	}

	return res.GetResult(), nil
}
