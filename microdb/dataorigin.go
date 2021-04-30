package microdb //nolint // Package comment located in a different file.

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/huandu/go-sqlbuilder"

	// Register local database driver.
	_ "github.com/mattn/go-sqlite3"
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
	Schema     *Schema        `yaml:"schema"`
	Connection *ConnectionCfg `yaml:"connection"`
	db         *sql.DB        `yaml:"-"`
}

// ConnectionCfg represents all the info for connecting to the data origin.
type ConnectionCfg struct {
	OriginType DataOriginType `yaml:"type"`
	Dsn        string         `yaml:"dsn"`
}

// DataOriginOption represents options for creating a DataOrigin.
// This is used with AddDataOrigin().
type DataOriginOption func() (*DataOrigin, error)

// WithMySQLDataOrigin creates options for using a new MySQL-based data origin.
func WithMySQLDataOrigin(host, port, user, password, database string, opt SchemaOption) DataOriginOption {
	return func() (*DataOrigin, error) {
		s, err := opt()
		if err != nil {
			return nil, fmt.Errorf("invalid schema: %w", err)
		}

		cfg := mySQLConnectionCfg(host, port, user, password, database)

		return &DataOrigin{
			Schema:     s,
			Connection: cfg,
		}, nil
	}
}

func mySQLConnectionCfg(host, port, user, password, database string) *ConnectionCfg {
	mCfg := mysql.NewConfig()
	mCfg.Net = "tcp"
	mCfg.Addr = fmt.Sprintf("%s:%s", host, port)
	mCfg.User = user
	mCfg.Passwd = password
	mCfg.DBName = database
	mCfg.ParseTime = true

	return &ConnectionCfg{
		OriginType: DataOriginTypeMySQL,
		Dsn:        mCfg.FormatDSN(),
	}
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
	schemaStore[table] = d.Schema
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
	var db *sql.DB
	var err error

	if d.db != nil {
		return d.db, nil
	}

	err = retry(func() error {
		db, err = sql.Open(string(d.Connection.OriginType), d.Connection.Dsn)
		if err != nil {
			return fmt.Errorf("sql error: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to data origin: %w", err)
	}

	d.db = db
	return db, nil
}

// ReadTopic returns the NATS topic name for subscribe to a table's updates.
func (d *DataOrigin) ReadTopic() string {
	return fmt.Sprintf("%s_table", d.Schema.Table)
}

// WriteTopic returns the NATS topic name for table writes.
func (d *DataOrigin) WriteTopic() string {
	return fmt.Sprintf("%s_write", d.Schema.Table)
}
