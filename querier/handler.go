// Package querier contains library functions for MicroDB querier
package querier

// Querier handler implementation

import (
	"database/sql"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	_ "github.com/siddontang/go-mysql/driver"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
)

// Handler represents a data origin querier.
type Handler interface {
	Handle() error
	Close() error
}

// MySQLQuerier represents a MySQL-based data origin querier.
type MySQLQuerier struct {
	table string
	sc    stan.Conn
	db    *sql.DB
	sub   []*nats.Subscription
}

// Handle starts the subscriber for handling write and direct read queries.
func (m *MySQLQuerier) Handle() error {
	writeTopic := fmt.Sprintf("%s_write", m.table)
	wSub, err := m.sc.NatsConn().Subscribe(writeTopic, tableWriteHandler(m.sc, m.db))
	if err != nil {
		return fmt.Errorf("failed to subscribe to write query topic: %w", err)
	}
	m.sub = append(m.sub, wSub)

	return nil
}

// Close closes all connections that the handler uses.
func (m *MySQLQuerier) Close() error {
	for _, sub := range m.sub {
		if err := sub.Unsubscribe(); err != nil {
			return fmt.Errorf("failed to unsubscribe topic: %w", err)
		}
	}

	if err := m.sc.Close(); err != nil {
		return fmt.Errorf("failed to close nats connection: %w", err)
	}

	if err := m.db.Close(); err != nil {
		return fmt.Errorf("failed to close database connection: %w", err)
	}

	return nil
}

// MySQLHandler returns a new instance of querier for MySQL-based data origin.
func MySQLHandler(host, port, user, password, database, table string, sc stan.Conn) (Handler, error) {
	dsn := fmt.Sprintf("%s:%s@%s:%s?%s", user, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to data origin: %w", err)
	}

	return &MySQLQuerier{
		table: table,
		sc:    sc,
		db:    db,
	}, nil
}

func tableWriteHandler(sc stan.Conn, db *sql.DB) func(*nats.Msg) {
	return func(m *nats.Msg) {
		var req pb.QueryRequest

		if err := proto.Unmarshal(m.Data, &req); err != nil {
			errMsg := fmt.Errorf("failed to unmarshal write request: %w", err).Error()
			_ = replyError(sc, m, errMsg) // TODO: Handle reply error
			return
		}

		r, err := db.Exec(req.Query, pb.UnmarshalValues(req.Args)...)
		if err != nil {
			errMsg := fmt.Errorf("failed to execute database query: %w", err).Error()
			_ = replyError(sc, m, errMsg)
			return
		}

		if ra, err := r.RowsAffected(); ra == 0 || err != nil {
			errMsg := "no rows affected"
			_ = replyError(sc, m, errMsg)
			return
		}
	}
}

func replyError(sc stan.Conn, originMsg *nats.Msg, errMsg string) error {
	res := &pb.WriteQueryReply{
		Ok:  false,
		Msg: errMsg,
	}

	pm, err := proto.Marshal(res)
	if err != nil {
		return fmt.Errorf("failed to marshal error reply: %w", err)
	}

	if err := sc.Publish(originMsg.Reply, pm); err != nil {
		return fmt.Errorf("failed to publish reply: %w", err)
	}

	return nil
}
