package publisher

import (
	"fmt"

	"github.com/nats-io/stan.go"
	"github.com/siddontang/go-mysql/canal"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
)

type Handler interface {
	Handle() error
}

type MySQLOrigin struct {
	table string
	c     *canal.Canal
	sc    stan.Conn

	canal.DummyEventHandler
}

func (m *MySQLOrigin) Handle() error {
	// Register a handler to handle RowsEvent
	m.c.SetEventHandler(m)
	defer m.sc.Close()

	return m.c.Run()
}

func (m *MySQLOrigin) OnRow(e *canal.RowsEvent) error {
	for _, r := range e.Rows {
		update := &pb.RowUpdate{
			Row: pb.MarshalValues(r),
		}

		p, err := proto.Marshal(update)
		if err != nil {
			return err
		}

		if err := m.sc.Publish(m.table, p); err != nil {
			return err
		}
	}

	return nil
}

func MySQLHandler(mySQLHost, port, user, password, database, table string, sc stan.Conn) (Handler, error) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%s", mySQLHost, port)
	cfg.User = user
	cfg.Password = password
	cfg.Dump.TableDB = database
	cfg.Dump.Tables = []string{table}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create canal client: %w", err)
	}

	return &MySQLOrigin{
		table: table,
		c:     c,
		sc:    sc,
	}, stan.ErrNilMsg
}
