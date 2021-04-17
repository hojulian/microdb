// Package microdb includes all application level components used either with MicroDB client or
// with in MicroDB system.
package microdb

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/huandu/go-sqlbuilder"
	// Register local database driver.
	_ "github.com/mattn/go-sqlite3"

	// Register data origin database drivers.
	_ "github.com/siddontang/go-mysql/driver"
)

const (
	// DataOriginTypeMySQL represents a MySQL-based data origin.
	DataOriginTypeMySQL = "mysql"
	// DataOriginTypeSQLite3 represents a SQLite3-based data origin.
	DataOriginTypeSQLite3 = "sqlite3"
)

//nolint // Used as internal data origin mapping.
var dataOrigins = make(map[string]*DataOrigin)

// DataOriginType represents a data origin database type.
type DataOriginType string

func (d *DataOriginType) toBuilderFlavor() sqlbuilder.Flavor {
	switch *d {
	case DataOriginTypeMySQL:
		return sqlbuilder.MySQL

	case DataOriginTypeSQLite3:
		return sqlbuilder.SQLite

	default:
		panic(fmt.Errorf("unsupported data origin type, got: %s", *d))
	}
}

// DataOrigin represents a table in MicroDB.
// For details, please refers to documentation.
type DataOrigin struct {
	schema *Schema
	cfg    *dataOriginCfg
	db     *sql.DB
}

type dataOriginCfg struct {
	originType DataOriginType
	dsn        string
}

// DataOriginOption represents options for creating a DataOrigin.
// This is used with AddDataOrigin().
type DataOriginOption func() (*DataOrigin, error)

// WithMySQLDataOrigin creates options for using a new MySQL-based data origin.
func WithMySQLDataOrigin(host, port, user, password, database string, opt SchemaOption) DataOriginOption {
	return func() (*DataOrigin, error) {
		cfg, err := mySQLDataOriginCfg(host, port, user, password, database)
		if err != nil {
			return nil, fmt.Errorf("failed to create data origin: %w", err)
		}

		s, err := opt()
		if err != nil {
			return nil, fmt.Errorf("invalid schema: %w", err)
		}

		return &DataOrigin{
			schema: s,
			cfg:    cfg,
		}, nil
	}
}

func mySQLDataOriginCfg(host, port, user, password, database string) (*dataOriginCfg, error) {
	dsn := fmt.Sprintf("%s:%s@%s:%s?%s", user, password, host, port, database)
	if err := validateMySQLDSN(dsn); err != nil {
		return nil, fmt.Errorf("failed to create data origin config: %w", err)
	}

	return &dataOriginCfg{
		originType: DataOriginTypeMySQL,
		dsn:        dsn,
	}, nil
}

// modified from:
// https://github.com/go-mysql-org/go-mysql/blob/8801d838aa3ae1063b4b17827a0d33cf63168853/driver/driver.go#L22
func validateMySQLDSN(dsn string) error {
	lastIndex := strings.LastIndex(dsn, "@")
	seps := []string{dsn[:lastIndex], dsn[lastIndex+1:]}
	if len(seps) != 2 { //nolint // Checker for dsn format.
		return fmt.Errorf("invalid dsn, must user:password@addr[?db], got: %s", dsn)
	}

	if ss := strings.Split(seps[0], ":"); !(len(ss) >= 1) {
		return fmt.Errorf("invalid dsn, must user:password@addr[?db], got: %s", dsn)
	}

	if ss := strings.Split(seps[1], "?"); !(len(ss) >= 1) {
		return fmt.Errorf("invalid dsn, must user:password@addr[?db], got: %s", dsn)
	}

	return nil
}

// AddDataOrigin adds a new data origin.
func AddDataOrigin(table string, opt DataOriginOption) error {
	if _, ok := dataOrigins[table]; ok {
		return nil
	}

	d, err := opt()
	if err != nil {
		return fmt.Errorf("failed to add data origin: %w", err)
	}

	dataOrigins[table] = d
	schemaStore[table] = d.schema
	return nil
}

// GetDataOrigin retreives a DataOrigin given the table name.
func GetDataOrigin(table string) (*DataOrigin, error) {
	d, ok := dataOrigins[table]
	if !ok {
		return nil, fmt.Errorf("no such table, got: %s", table)
	}

	return d, nil
}

// GetDB returns a database connection to a specific data origin.
func (d *DataOrigin) GetDB() (*sql.DB, error) {
	if d.db != nil {
		return d.db, nil
	}

	db, err := sql.Open(string(d.cfg.originType), d.cfg.dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to data origin: %w", err)
	}
	d.db = db

	return db, nil
}
