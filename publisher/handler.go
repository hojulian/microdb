// Package publisher contains library functions for MicroDB publisher
package publisher

// Publisher handler implementation

import (
	"fmt"

	"github.com/nats-io/stan.go"
	"github.com/siddontang/go-mysql/canal"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
)

// Handler represents a data origin publisher.
type Handler interface {
	Handle() error
	Close() error
}

// MySQLPublisher represents a MySQL-based data origin publisher.
type MySQLPublisher struct {
	table string
	c     *canal.Canal
	sc    stan.Conn

	canal.DummyEventHandler
}

// Handle starts the event handler for handling new row updates from data origin.
func (m *MySQLPublisher) Handle() error {
	// Register a handler to handle RowsEvent
	m.c.SetEventHandler(m)

	if err := m.c.Run(); err != nil {
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
			Row: pb.MarshalValues(r),
		}

		p, err := proto.Marshal(update)
		if err != nil {
			return fmt.Errorf("failed to marshal row update: %w", err)
		}

		if err := m.sc.Publish(m.table, p); err != nil {
			return fmt.Errorf("failed to publish row update: %w", err)
		}
	}

	return nil
}

// MySQLHandler returns a new instance of publisher for MySQL-based data origin.
func MySQLHandler(host, port, user, password, database, table string, sc stan.Conn) (Handler, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", host, port)
	cfg.User = user
	cfg.Password = password
	cfg.Dump.TableDB = database
	cfg.Dump.Tables = []string{table}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create canal client: %w", err)
	}

	return &MySQLPublisher{
		table: table,
		c:     c,
		sc:    sc,
	}, stan.ErrNilMsg
}
