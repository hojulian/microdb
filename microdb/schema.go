// Package microdb includes all application level components used either with MicroDB client or
// with in MicroDB system.
package microdb

import (
	"errors"

	sqlbuilder "github.com/huandu/go-sqlbuilder"
)

//nolint // Used as internal schema mapping.
var schemaStore = make(map[string]*Schema)

// Schema represents the SQL schema for a table.
type Schema struct {
	OriginTableQuery string
	LocalTableQuery  string
	InsertQuery      string
}

// SchemaOption represents options for creating a Schema.
type SchemaOption func() (*Schema, error)

// WithSchemaStrings creates option for a new schema using raw query strings.
func WithSchemaStrings(
	originType DataOriginType, originTableQuery, localTableQuery, localInsertQuery string) SchemaOption {
	return func() (*Schema, error) {
		if originTableQuery == "" {
			return nil, errors.New("missing origin table query")
		}

		if localTableQuery == "" {
			return nil, errors.New("missing local table query")
		}

		if localInsertQuery == "" {
			return nil, errors.New("missing local insert query")
		}

		return &Schema{
			OriginTableQuery: originTableQuery,
			LocalTableQuery:  localTableQuery,
			InsertQuery:      localInsertQuery,
		}, nil
	}
}

// WithSchemaBuilder creates option for a new schema using sqlbuilder.
func WithSchemaBuilder(
	originType DataOriginType,
	table string,
	originTableBuilder *sqlbuilder.CreateTableBuilder,
	originTableStruct *sqlbuilder.Struct,
) SchemaOption {
	return func() (*Schema, error) {
		f := originType.toBuilderFlavor()
		originTableBuilder.SetFlavor(f)
		originTableQuery, _ := originTableBuilder.Build()

		originTableBuilder.SetFlavor(sqlbuilder.SQLite)
		localTableQuery, _ := originTableBuilder.Build()

		insertQueryBuilder := originTableStruct.For(sqlbuilder.SQLite)
		iqb := insertQueryBuilder.ReplaceInto(table)
		iqb.SetFlavor(sqlbuilder.SQLite)
		insertQuery, _ := iqb.Build()

		return &Schema{
			OriginTableQuery: originTableQuery,
			LocalTableQuery:  localTableQuery,
			InsertQuery:      insertQuery,
		}, nil
	}
}

// LocalTableQuery returns the create table query (sqlite3) for a given table.
func LocalTableQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.LocalTableQuery, nil
}

// OriginTableQuery returns the create table query (origin) for a given table.
func OriginTableQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.OriginTableQuery, nil
}

// InsertQuery returns the insert query (sqlite3) for a given table.
func InsertQuery(table string) (string, error) {
	s, ok := schemaStore[table]
	if !ok {
		return "", errors.New("no such table")
	}

	return s.InsertQuery, nil
}
