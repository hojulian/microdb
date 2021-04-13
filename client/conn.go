package client

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"

	"github.com/nats-io/stan.go"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
)

var (
	_ driver.Conn   = &Conn{}
	_ driver.Rows   = &Rows{}
	_ driver.Result = &Result{}
)

type Conn struct {
	sc     stan.Conn
	db     *sql.DB
	tables map[string]stan.Subscription
}

func newConn(sc stan.Conn, db *sql.DB, tables map[string]stan.Subscription) (*Conn, error) {
	c := &Conn{
		sc:     sc,
		db:     db,
		tables: tables,
	}

	for t, s := range c.tables {
		if s == nil {
			sc, err := sc.Subscribe(t, newNATSSubsriptionHandler(db), stan.DeliverAllAvailable())
			if err != nil {
				return nil, fmt.Errorf("failed to subscribe to table change: %w", err)
			}
			c.tables[t] = sc
		}
	}

	return c, nil
}

func newNATSSubsriptionHandler(db *sql.DB) stan.MsgHandler {
	return func(m *stan.Msg) {
		var ru pb.RowUpdate

		if err := proto.Unmarshal(m.Data, &ru); err != nil {
			log.Printf("failed to parse row update: %v\n", err)
			return
		}
	}
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

	if err := c.db.Close(); err != nil {
		return fmt.Errorf("failed to close local sqlite3 connection: %w", err)
	}

	return nil
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.Value) (driver.Rows, error) {
	rs, err := c.db.QueryContext(ctx, query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to query local sqlite3: %w", err)
	}

	return &Rows{
		rows: rs,
	}, nil
}

type Rows struct {
	rows *sql.Rows
}

func (r *Rows) Columns() []string {
	if rs, err := r.rows.Columns(); err == nil {
		return rs
	}

	return nil
}

func (r *Rows) Next(dest []driver.Value) error {
	return r.rows.Scan(dest)
}

func (r *Rows) Close() error {
	return r.rows.Close()
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.Value) (driver.Result, error) {
	req := &pb.WriteQueryRequest{
		Query: query,
		Args:  normalizeDriverValues(args),
	}

	p, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal write request: %w", err)
	}

	// Forward to egress directly, it will figure out the type conversion.
	msg, err := c.sc.NatsConn().RequestWithContext(ctx, "ip_addresses_write", p)
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

	return nil, nil
}

func normalizeDriverValues(args []driver.Value) []*pb.Value {
	norm := make([]*pb.Value, 0, len(args))
	for _, a := range args {
		v := pb.MarshalValue(interface{}(a))
		if v != nil {
			norm = append(norm, v)
		} else {
			log.Printf("failed to normalize argument: %v", a)
		}
	}
	return norm
}

type Result struct {
	lastInsertId int64
	rowsAffected int64
	err          error
}

func (re *Result) LastInsertId() (int64, error) {
	if re.err != nil {
		return 0, fmt.Errorf("failed to retrieve last insert ID: %w", re.err)
	}

	return re.lastInsertId, nil
}

func (re *Result) RowsAffected() (int64, error) {
	if re.err != nil {
		return 0, fmt.Errorf("failed to retrieve rows affected: %w", re.err)
	}

	return re.rowsAffected, nil
}
