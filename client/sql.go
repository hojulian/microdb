// Package client represents a MicroDB client.
package client

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

// This file contains mostly helper structs for integrating with sql.Driver.

var (
	_ driver.Connector = dsnConnector{}
	_ driver.Rows      = &dRows{}
)

type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (t dsnConnector) Connect(_ context.Context) (driver.Conn, error) {
	//nolint // Low level sql method, no need for error wrapping
	return t.driver.Open(t.dsn)
}

func (t dsnConnector) Driver() driver.Driver {
	return t.driver
}

type dRows struct {
	sRows *sql.Rows
	dbc   *sql.Conn
}

func (d *dRows) Columns() []string {
	c, _ := d.sRows.Columns()
	return c
}

func (d *dRows) Close() error {
	if err := d.sRows.Close(); err != nil {
		return fmt.Errorf("failed to close underlying rows: %w", err)
	}

	if err := d.dbc.Close(); err != nil {
		return fmt.Errorf("failed to close database connection: %w", err)
	}

	return nil
}

func (d *dRows) Next(dest []driver.Value) error {
	if !d.sRows.Next() {
		return errors.New("no more rows")
	}

	if err := d.sRows.Scan(dest); err != nil {
		return fmt.Errorf("failed to scan underlying row: %w", err)
	}

	return nil
}
