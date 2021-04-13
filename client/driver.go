package client

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"github.com/nats-io/stan.go"
)

type Driver struct {
	clientID      string
	natsClusterID string
	natsHost      string
	natsPort      string
	tables        map[string]stan.Subscription
}

func (d *Driver) Open(name string) (driver.Conn, error) {
	if err := d.parseDSN(name); err != nil {
		return nil, fmt.Errorf("invalid dsn: %w", err)
	}

	sc, err := newNATSConn(d.clientID, d.natsClusterID, d.natsHost, d.natsPort)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	db, err := sql.Open("sqlite3", "file::memory:?cache=shared&mode=memory&_journal=memory&_cache_size=-64000")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to local sqlite3: %w", err)
	}

	return newConn(sc, db, d.tables)
}

func newNATSConn(clientID, natsClusterID, natsHost, natsPort string) (stan.Conn, error) {
	natsURL := fmt.Sprintf("nats://%s:%s", natsHost, natsPort)

	sc, err := stan.Connect(natsClusterID, clientID, stan.NatsURL(natsURL))
	if err != nil {
		log.Printf("failed to connect to nats: %v\n", err)
		return nil, fmt.Errorf("failed to connect to nats cluster: %w", err)
	}

	return sc, nil
}

func (d *Driver) parseDSN(dsn string) error {
	// dsn format:
	//    clientID=... natsHost=... natsPort=... tables=...,...
	opts, err := parseDSNMap(dsn)
	if err != nil {
		return fmt.Errorf("failed to parse dsn: %w", err)
	}

	if v, ok := opts["clientID"]; ok {
		d.clientID = v
	} else {
		return fmt.Errorf("missing clientID")
	}

	if v, ok := opts["natsClusterID"]; ok {
		d.natsClusterID = v
	} else {
		return fmt.Errorf("missing NATS clusterID")
	}

	if v, ok := opts["natsHost"]; ok {
		d.natsHost = v
	} else {
		return fmt.Errorf("missing NATS host")
	}

	if v, ok := opts["natsPort"]; ok {
		d.natsPort = v
	} else {
		return fmt.Errorf("missing NATS port")
	}

	if v, ok := opts["tables"]; ok {
		d.tables = make(map[string]stan.Subscription)
		for _, t := range strings.Split(v, ",") {
			d.tables[t] = nil
		}
	} else {
		return fmt.Errorf("missing tables")
	}

	return nil
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
