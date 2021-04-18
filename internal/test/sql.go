// Package test provides library functions for testing MicroDB system.
package test

import "github.com/hojulian/microdb/microdb"

//nolint // Allow globals for easy use.
var (
	tableNameTest    = "test"
	testOGTableQuery = `CREATE TABLE test (
	id INT(11) NOT NULL AUTO_INCREMENT,
	string_type VARCHAR(255) COLLATE utf8_unicode_ci DEFAULT NULL,
	int_type INT(11) DEFAULT NULL,
	float_type DOUBLE DEFAULT NULL,
	bool_type TINYINT(1) DEFAULT NULL,
	timestamp_type DATETIME DEFAULT NULL,
	PRIMARY KEY(id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8 COLLATE = utf8_unicode_ci;`
	testLocalTableQuery = `CREATE TABLE test (
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	string_type VARCHAR(255) DEFAULT NULL,
	int_type INTEGER DEFAULT NULL,
	float_type DOUBLE DEFAULT NULL,
	bool_type INTEGER DEFAULT NULL,
	timestamp_type DATETIME DEFAULT NULL
);`
	testInsertQuery = `REPLACE INTO test VALUES (?, ?, ?, ?, ?, ?);`

	// TestTableName represents the table name for the test table.
	TestTableName = tableNameTest
	// TestSchemaOption represents option used for creating a test table origin.
	TestSchemaOption = microdb.WithSchemaStrings(
		microdb.DataOriginTypeMySQL,
		testOGTableQuery,
		testLocalTableQuery,
		testInsertQuery,
	)
)
