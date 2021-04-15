// Package client represents a MicroDB client
package client

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/mattn/go-sqlite3"
	"github.com/nats-io/stan.go"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
	"github.com/hojulian/microdb/microdb"
)

func init() {
	sql.Register("microdb", &Driver{})
}

type Driver struct {
	cfg *DriverCfg

	initialized bool
	drv         *sqlite3.SQLiteDriver
	db          *sql.DB
	sc          stan.Conn
	tables      map[string]stan.Subscription
}

type DriverCfg struct {
	dsn           string
	natsClientID  string
	natsClusterID string
	natsHost      string
	natsPort      string
	tables        []string
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	cfg, err := parseDSN(name)
	if err != nil {
		return nil, fmt.Errorf("invalid dsn: %w", err)
	}
	d.cfg = cfg

	if !d.initialized {
		if ierr := d.init(); ierr != nil {
			return nil, fmt.Errorf("failed to initialize driver: %w", ierr)
		}
	}

	sqc, err := d.drv.Open(d.cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to local sqlite3: %w", err)
	}

	return &Conn{
		sc:  d.sc,
		sqc: sqc,
	}, nil
}

func parseDSN(dsn string) (*DriverCfg, error) {
	// dsn format:
	//    natsClientID=... natsHost=... natsPort=... tables=...,...
	cfg := &DriverCfg{
		dsn: "file::memory:?cache=shared&mode=memory&_journal=memory&_cache_size=-64000",
	}

	opts, err := parseDSNMap(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dsn: %w", err)
	}

	if v, ok := opts["natsClientID"]; ok {
		cfg.natsClientID = v
	} else {
		return nil, fmt.Errorf("missing clientID")
	}

	if v, ok := opts["natsClusterID"]; ok {
		cfg.natsClusterID = v
	} else {
		return nil, fmt.Errorf("missing NATS clusterID")
	}

	if v, ok := opts["natsHost"]; ok {
		cfg.natsHost = v
	} else {
		return nil, fmt.Errorf("missing NATS host")
	}

	if v, ok := opts["natsPort"]; ok {
		cfg.natsPort = v
	} else {
		return nil, fmt.Errorf("missing NATS port")
	}

	if v, ok := opts["tables"]; ok {
		cfg.tables = strings.Split(v, ",")
	} else {
		return nil, fmt.Errorf("missing tables")
	}

	return cfg, nil
}

func parseDSNMap(dsn string) (map[string]string, error) {
	opts := make(map[string]string)
	kvList := strings.Split(dsn, " ")

	for _, kv := range kvList {
		sep := strings.LastIndex(kv, "=")
		if sep == -1 {
			return nil, fmt.Errorf("invalid key-value pair in dsn: %s", kv)
		}

		k, v := kv[:sep], kv[sep+1:]
		opts[k] = v
	}

	return opts, nil
}

func (d *Driver) init() error {
	drv := &sqlite3.SQLiteDriver{}
	db := sql.OpenDB(dsnConnector{dsn: d.cfg.dsn, driver: drv})
	db.SetConnMaxLifetime(-1)

	sc, err := microdb.NATSConn(
		d.cfg.natsHost,
		d.cfg.natsPort,
		d.cfg.natsClusterID,
		d.cfg.natsClientID,
	)
	if err != nil {
		return fmt.Errorf("failed to connect to nats cluster: %w", err)
	}

	d.drv = drv
	d.db = db
	d.sc = sc

	for _, t := range d.cfg.tables {
		if err := createTable(d.db, t); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}

		if err := d.subscribeTable(t); err != nil {
			return fmt.Errorf("failed to subscribe to table: %w", err)
		}
	}

	d.initialized = true

	return nil
}

func (d *Driver) subscribeTable(table string) error {
	sub, err := d.sc.Subscribe(table, tableHandler(d.db, table), stan.DeliverAllAvailable())
	if err != nil {
		return fmt.Errorf("failed to subscribe to nats: %w", err)
	}

	d.tables[table] = sub
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

		r, err := db.Exec(iq, pb.UnmarshalValues(ru.GetRow())...)
		if err != nil {
			panic(fmt.Errorf("failed to execute query: %w", err))
		}

		if ra, err := r.RowsAffected(); ra == 0 || err != nil {
			panic(fmt.Errorf("failed to update table: %w or no rows affected", err))
		}
	}
}
