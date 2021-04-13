package querier

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/siddontang/go-mysql/client"

	pb "github.com/hojulian/microdb/internal/proto"
)

type Handler interface {
	Handle() error
	Close() error
}

type MySQLQuerier struct {
	table string
	sc    stan.Conn
	db    *client.Conn
	sub   *nats.Subscription
}

func (m *MySQLQuerier) Handle() error {
	topic := fmt.Sprintf("%s_write", m.table)
	sub, err := m.sc.NatsConn().Subscribe(topic, tableHandler(m.table, m.sc, m.db))
	if err != nil {
		return fmt.Errorf("failed to subscribe to write query topic: %w", err)
	}
	m.sub = sub

	return nil
}

func (m *MySQLQuerier) Close() error {
	if err := m.sub.Unsubscribe(); err != nil {
		return fmt.Errorf("failed to unsubscribe topic: %w", err)
	}

	if err := m.sc.Close(); err != nil {
		return fmt.Errorf("failed to close nats connection: %w", err)
	}

	if err := m.db.Close(); err != nil {
		return fmt.Errorf("failed to close database connection: %w", err)
	}

	return nil
}

func MYSQLHandler(host, port, user, password, database, table string, sc stan.Conn) (Handler, error) {
	addr := fmt.Sprintf("%s:%s", host, port)
	db, err := client.Connect(addr, user, password, database)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to data origin: %w", err)
	}

	return &MySQLQuerier{
		table: table,
		sc:    sc,
		db:    db,
	}, nil
}

func tableHandler(table string, sc stan.Conn, db *client.Conn) func(*nats.Msg) {
	return func(m *nats.Msg) {
		var req pb.WriteQueryRequest

		if err := proto.Unmarshal(m.Data, &req); err != nil {
			errMsg := fmt.Errorf("failed to unmarshal write request: %w", err).Error()
			_ = replyError(sc, m, errMsg)
			return
		}

		r, err := db.Execute(req.Query, pb.UnmarshalValues(req.Args)...)
		if err != nil {
			errMsg := fmt.Errorf("failed to execute database query: %w", err).Error()
			_ = replyError(sc, m, errMsg)
			return
		}

		if r.AffectedRows == 0 {
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

	return sc.Publish(originMsg.Reply, pm)
}
