// Package client represents a MicroDB client
package client

import (
	"context"
	"database/sql"
	"fmt"

	// Register local database driver.
	_ "github.com/mattn/go-sqlite3"
	"github.com/nats-io/stan.go"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
	"github.com/hojulian/microdb/microdb"
	mquery "github.com/hojulian/microdb/query"
)

// Client represents a microDB client.
type Client struct {
	sc     stan.Conn
	mdb    *sql.DB
	tables map[string]stan.Subscription
}

// Connect creates a microDB client.
func Connect(natsHost, natsPort, natsClientID, natsClusterID string, tables ...string) (*Client, error) {
	sc, err := microdb.NATSConn(natsHost, natsPort, natsClusterID, natsClientID, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	mdb, err := sql.Open("sqlite3", "file::memory:?cache=shared&mode=memory&_journal=memory&_cache_size=-64000")
	mdb.SetConnMaxLifetime(-1)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to local database: %w", err)
	}

	c := &Client{
		sc:     sc,
		mdb:    mdb,
		tables: make(map[string]stan.Subscription),
	}
	if err := c.subscribe(tables); err != nil {
		return nil, fmt.Errorf("failed to initialize microDB client: %w", err)
	}

	return c, nil
}

func (c *Client) subscribe(tables []string) error {
	for _, t := range tables {
		if err := createTable(c.mdb, t); err != nil {
			return fmt.Errorf("failed to create table locally: %w", err)
		}

		sub, err := subscribeTable(t, c.sc, c.mdb)
		if err != nil {
			return fmt.Errorf("failed to subscribe to table: %w", err)
		}

		c.tables[t] = sub
	}
	return nil
}

func createTable(db *sql.DB, table string) error {
	tq, err := microdb.LocalTableQuery(table)
	if err != nil {
		return fmt.Errorf("failed to get table schema query: %w", err)
	}

	_, err = db.Exec(tq)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	return nil
}

func subscribeTable(table string, sc stan.Conn, db *sql.DB) (stan.Subscription, error) {
	do, err := microdb.GetDataOrigin(table)
	if err != nil {
		return nil, fmt.Errorf("failed to get data origin for table: %w", err)
	}
	handler := tableHandler(db, table)

	sub, err := sc.Subscribe(do.ReadTopic(), handler, stan.DeliverAllAvailable())
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to table updates: %w", err)
	}

	return sub, nil
}

func tableHandler(db *sql.DB, table string) stan.MsgHandler {
	return func(m *stan.Msg) {
		var ru pb.RowUpdate

		if err := proto.Unmarshal(m.Data, &ru); err != nil {
			panic(fmt.Errorf("failed to parse row update: %w", err))
		}

		iq, err := microdb.InsertQuery(table)
		if err != nil {
			panic(fmt.Errorf("failed to get insert query: %w", err))
		}

		tx, err := db.Begin()
		if err != nil {
			panic(fmt.Errorf("failed to create update transaction: %w", err))
		}

		r, err := tx.Exec(iq, pb.UnmarshalValues(ru.GetRow())...)
		if err != nil {
			derr := fmt.Errorf("failed to update local databse for table %s: %w, got: %s",
				table, err, ru.String())
			if rerr := tx.Rollback(); rerr != nil {
				panic(fmt.Errorf("failed to rollback transaction: %w for error: %s",
					rerr, derr.Error()))
			}
		}

		if ra, err := r.RowsAffected(); ra == 0 || err != nil {
			panic(fmt.Errorf("failed to update table: %w or no rows affected", err))
		}

		if err := tx.Commit(); err != nil {
			panic(fmt.Errorf("failed commit update to table: %w", err))
		}
	}
}

// Query executes a query that returns rows, typically a SELECT. The args are for any placeholder
// parameters in the query.
func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	q, err := mquery.Query(query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}
	if q.GetQueryType() != mquery.QueryTypeSelect {
		return nil, fmt.Errorf("unsupported query type: %w", err)
	}

	// Check if it is able to be executed locally
	if !c.containsAllRequiredTable(q.GetRequiredTables()) {
		// If not, force the query to data origin
		q = q.OnOrigin()
	}

	switch q.GetDestinationType() {
	case mquery.DestinationTypeLocal:
		rs, err := c.mdb.QueryContext(ctx, q.SQL(), args...)
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

		rs, err := db.QueryContext(ctx, q.SQL(), args)
		if err != nil {
			return nil, fmt.Errorf("failed to query data origin database: %w", err)
		}

		return rs, nil
	}

	return nil, fmt.Errorf("unsupported destination type")
}

// Execute executes a query without returning any rows. The args are for any placeholder parameters
// in the query.
func (c *Client) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	q, err := mquery.Query(query)
	if err != nil {
		return nil, fmt.Errorf("invalid query: %w", err)
	}
	if q.GetQueryType() == mquery.QueryTypeSelect {
		return nil, fmt.Errorf("for select query, please use Query")
	}

	req := &pb.QueryRequest{
		Query: q.SQL(),
		Args:  pb.MarshalValues(args),
	}

	p, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal write request: %w", err)
	}

	dest := q.GetDestinationTable()
	do, err := microdb.GetDataOrigin(dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get data origin for table: %w", err)
	}

	// Forward to querier directly, it will figure out the type conversion.
	msg, err := c.sc.NatsConn().RequestWithContext(ctx, do.WriteTopic(), p)
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

func (c *Client) containsAllRequiredTable(ts []string) bool {
	for _, t := range ts {
		if _, ok := c.tables[t]; !ok {
			return false
		}
	}
	return true
}

// Close unsubscribes database changes and closes its local database.
func (c *Client) Close() error {
	for _, s := range c.tables {
		if err := s.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe table: %w", err)
		}
	}

	if err := c.sc.Close(); err != nil {
		return fmt.Errorf("failed to close nats connection: %w", err)
	}

	if err := c.mdb.Close(); err != nil {
		return fmt.Errorf("failed to close local database: %w", err)
	}

	return nil
}
