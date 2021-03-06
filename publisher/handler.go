// Package publisher contains library functions for MicroDB publisher.
package publisher

// Publisher handler implementation.

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/nats-io/stan.go"
	"github.com/siddontang/go-mysql/canal"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
	"github.com/hojulian/microdb/microdb"
)

// Handler represents a data origin publisher.
type Handler interface {
	Handle() error
	Close() error
}

// MySQLPublisher represents a MySQL-based data origin publisher.
type MySQLPublisher struct {
	tableMapping map[string]string
	c            *canal.Canal
	sc           stan.Conn

	canal.DummyEventHandler
}

// Handle starts the event handler for handling new row updates from data origin.
func (m *MySQLPublisher) Handle() error {
	// Register a handler to handle RowsEvent
	m.c.SetEventHandler(m)

	err := retry(func() error {
		if err := m.c.Run(); err != nil {
			return fmt.Errorf("canal error: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to start handler: %w", err)
	}

	return nil
}

// Close closes all connections that the handler uses.
func (m *MySQLPublisher) Close() error {
	m.c.Close()

	if err := m.sc.Close(); err != nil {
		return fmt.Errorf("failed to close nats connection: %w", err)
	}

	return nil
}

// OnRow is a callback that get triggered when a new row update is received from the data origin.
func (m *MySQLPublisher) OnRow(e *canal.RowsEvent) error {
	for _, r := range e.Rows {
		update := &pb.RowUpdate{
			Row: pb.MarshalCanalValues(e.Table, r),
		}

		p, err := proto.Marshal(update)
		if err != nil {
			return fmt.Errorf("failed to marshal row update: %w", err)
		}

		if err := m.sc.Publish(m.tableMapping[e.Table.Name], p); err != nil {
			return fmt.Errorf("failed to publish row update: %w", err)
		}
	}

	return nil
}

// MySQLHandler returns a new instance of publisher for MySQL-based data origin.
func MySQLHandler(host, port, user, password, database string,
	id uint32, sc stan.Conn, tables ...string) (Handler, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.User = user
	cfg.Password = password
	cfg.Dump.TableDB = database
	cfg.Dump.Tables = tables
	cfg.ServerID = id

	var c *canal.Canal
	var err error

	rerr := retry(func() error {
		c, err = canal.NewCanal(cfg)
		if err != nil {
			return fmt.Errorf("canal error: %w", err)
		}

		return nil
	})
	if rerr != nil {
		return nil, fmt.Errorf("failed to create canal client: %w", err)
	}

	mapping := make(map[string]string)
	for _, t := range tables {
		do, err := microdb.GetDataOrigin(t)
		if err != nil {
			return nil, fmt.Errorf("failed to get data origin for table: %w", err)
		}
		mapping[t] = do.ReadTopic()
	}

	return &MySQLPublisher{
		tableMapping: mapping,
		c:            c,
		sc:           sc,
	}, nil
}

//nolint // Internal method.
func retry(op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Second * 5
	bo.MaxElapsedTime = time.Minute

	return backoff.Retry(op, bo)
}
