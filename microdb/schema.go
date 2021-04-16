// Package microdb includes all application level components used either with MicroDB client or
// with in MicroDB system.
package microdb

import "errors"

//nolint // Used as internal schema mapping.
var schemaStore = make(map[string]*Schema)

// Schema represents the SQL schema for a table.
type Schema struct {
	originTableQuery string
	localTableQuery  string
	insertQuery      string
}

// LocalTableQuery returns the create table query (sqlite3) for a given table.
func LocalTableQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.localTableQuery, nil
}

// OriginTableQuery returns the create table query (origin) for a given table.
func OriginTableQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.originTableQuery, nil
}

// InsertQuery returns the insert query (sqlite3) for a given table.
func InsertQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.insertQuery, nil
}

func (s *Schema) convertOriginTableQuery() error {
	// TODO: Implement me
	return nil
}
