// Package querier contains library functions for MicroDB querier.
package querier

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/go-sql-driver/mysql"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
	"github.com/hojulian/microdb/microdb"
)

// Querier handler implementation.

// Handler represents a data origin querier.
type Handler interface {
	Handle() error
	Close() error
}

// MySQLQuerier represents a MySQL-based data origin querier.
type MySQLQuerier struct {
	topic string
	sc    stan.Conn
	db    *sql.DB
	sub   []*nats.Subscription
}

// Handle starts the subscriber for handling write and direct read queries.
func (m *MySQLQuerier) Handle() error {
	wSub, err := m.sc.NatsConn().Subscribe(m.topic, tableWriteHandler(m.sc, m.db))
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
	var db *sql.DB
	var err error

	dsn := mySQLDSN(host, port, user, password, database)
	rerr := retry(func() error {
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return fmt.Errorf("sql error: %w", err)
		}
		return nil
	})
	if rerr != nil {
		return nil, fmt.Errorf("failed to connect to data origin: %w", rerr)
	}

	do, err := microdb.GetDataOrigin(table)
	if err != nil {
		return nil, fmt.Errorf("failed to get data origin for table: %w", err)
	}

	return &MySQLQuerier{
		topic: do.WriteTopic(),
		sc:    sc,
		db:    db,
	}, nil
}

func mySQLDSN(host, port, user, password, database string) string {
	mCfg := mysql.NewConfig()
	mCfg.Net = "tcp"
	mCfg.Addr = fmt.Sprintf("%s:%s", host, port)
	mCfg.User = user
	mCfg.Passwd = password
	mCfg.DBName = database
	mCfg.ParseTime = true

	return mCfg.FormatDSN()
}

func tableWriteHandler(sc stan.Conn, db *sql.DB) func(*nats.Msg) {
	return func(m *nats.Msg) {
		var req pb.QueryRequest

		if err := proto.Unmarshal(m.Data, &req); err != nil {
			errMsg := fmt.Errorf("failed to unmarshal write request: %w", err).Error()
			if rerr := replyError(sc, m, errMsg); rerr != nil {
				panic(fmt.Errorf("failed to publish error reply: %w: %s", rerr, errMsg))
			}
			return
		}

		r, err := db.Exec(req.Query, pb.UnmarshalValues(req.Args)...)
		if err != nil {
			errMsg := fmt.Errorf("failed to execute database query: %w got: %s", err, &req.Args).Error()
			if rerr := replyError(sc, m, errMsg); rerr != nil {
				panic(fmt.Errorf("failed to publish error reply: %w: %s", rerr, errMsg))
			}
			return
		}

		ra, err := r.RowsAffected()
		if err != nil {
			errMsg := fmt.Errorf("failed to get rows affected: %w", err).Error()
			if rerr := replyError(sc, m, errMsg); rerr != nil {
				panic(fmt.Errorf("failed to publish error reply: %w: %s", rerr, errMsg))
			}
			return
		}

		lid, err := r.LastInsertId()
		if err != nil {
			errMsg := fmt.Errorf("failed to get last insert id: %w", err).Error()
			if rerr := replyError(sc, m, errMsg); rerr != nil {
				panic(fmt.Errorf("failed to publish error reply: %w: %s", rerr, errMsg))
			}
			return
		}

		// Reply to request
		res := &pb.WriteQueryReply{
			Ok: true,
			Result: &pb.DriverResult{
				ResultRowsAffected: ra,
				ResultLastInsertId: lid,
			},
		}

		pm, err := proto.Marshal(res)
		if err != nil {
			errMsg := fmt.Errorf("failed to marshal reply: %w", err).Error()
			if rerr := replyError(sc, m, errMsg); rerr != nil {
				panic(fmt.Errorf("failed to publish error reply: %w: %s", rerr, errMsg))
			}
			return
		}

		if err := sc.NatsConn().Publish(m.Reply, pm); err != nil {
			panic(fmt.Errorf("failed to publish error reply: %w", err))
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

	if err := sc.NatsConn().Publish(originMsg.Reply, pm); err != nil {
		return fmt.Errorf("failed to publish reply: %w", err)
	}

	return nil
}

//nolint // Internal method.
func retry(op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Second * 5
	bo.MaxElapsedTime = time.Minute

	return backoff.Retry(op, bo)
}
