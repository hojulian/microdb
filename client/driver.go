// Package client represents a MicroDB client
package client

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/mattn/go-sqlite3"
	"github.com/nats-io/stan.go"

	"github.com/hojulian/microdb/microdb"
)

//nolint // Init used for registering this driver on startup
func init() {
	sql.Register("microdb", &Driver{})
}

// Driver is the MicroDB driver that implements database/sql/driver.
type Driver struct {
	cfg *driverCfg

	initialized bool
	drv         *sqlite3.SQLiteDriver
	db          *sql.DB
	sc          stan.Conn
	tables      map[string]stan.Subscription
}

type driverCfg struct {
	dsn           string
	natsClientID  string
	natsClusterID string
	natsHost      string
	natsPort      string
	tables        []string
}

// Open returns a new connection to the database.
//
// The name is a string in a driver-specific format.
// dsn format:
//    natsClientID=... natsHost=... natsPort=... tables=...,...
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// This method blocks until the nats connection is ready.
//
// The returned connection is only used by one goroutine at a
// time.
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

func parseDSN(dsn string) (*driverCfg, error) {
	// dsn format:
	//    natsClientID=... natsHost=... natsPort=... tables=...,...
	cfg := &driverCfg{
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
		nil,
		nil,
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
	do, err := microdb.GetDataOrigin(table)
	if err != nil {
		return fmt.Errorf("failed to get data origin for table: %w", err)
	}

	sub, err := d.sc.Subscribe(do.ReadTopic(), tableHandler(d.db, table), stan.DeliverAllAvailable())
	if err != nil {
		return fmt.Errorf("failed to subscribe to nats: %w", err)
	}

	if d.tables == nil {
		d.tables = make(map[string]stan.Subscription)
	}
	d.tables[table] = sub

	return nil
}
