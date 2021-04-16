// Package query provides query building and parsing functionalities used by both MicroDB client
// and external users.
package query

import (
	"fmt"
)

const (
	// QueryTypeSelect represents a SELECT statement.
	QueryTypeSelect = iota
	// QueryTypeInsert represents an INSERT or REPLACE statement.
	QueryTypeInsert
	// QueryTypeUpdate represents an UPDATE statement.
	QueryTypeUpdate
	// DestinationTypeLocal represents using local database as destination.
	DestinationTypeLocal
	// DestinationTypeOrigin represents using data origin as destination.
	DestinationTypeOrigin
)

// QueryType represents the type of query.
// Currently, only SELECT, INSERT/REPLACE, and UPDATE is supported.
//nolint // Silence name suggestion.
type QueryType int

// DestinationType represents the type of destination for the query.
type DestinationType int

// QueryStmt represents a parsed query.
//nolint // Silence name suggestion.
type QueryStmt struct {
	originQuery string

	queryType        QueryType
	destinationType  DestinationType
	destinationTable string
	requiredTables   []string

	query string
}

// Query creates a new query statement.
//
// This is used for chaining to provide access to MicroDB specific API through a conventitional
// builder interface.
func Query(query string) (*QueryStmt, error) {
	qs, err := parseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	return qs, nil
}

// OnLocal forces the query to the local database.
//
// This could be overrided at query-time if the query contains tables that do not exist locally.
func (q *QueryStmt) OnLocal() *QueryStmt {
	q.destinationType = DestinationTypeLocal
	return q
}

// OnOrigin forces the query to the data origin.
func (q *QueryStmt) OnOrigin() *QueryStmt {
	q.destinationType = DestinationTypeOrigin
	return q
}

// SQL converts the query into string format.
func (q *QueryStmt) SQL() string {
	return q.query
}

// GetDestinationTable returns the destination table.
//
// This is only used for INSERT queries.
func (q *QueryStmt) GetDestinationTable() string {
	return q.destinationTable
}

// GetDestinationType returns the destination.
func (q *QueryStmt) GetDestinationType() DestinationType {
	return q.destinationType
}

// GetQueryType returns the query type.
func (q *QueryStmt) GetQueryType() QueryType {
	return q.queryType
}

// GetRequiredTables returns all the tables required by the query.
func (q *QueryStmt) GetRequiredTables() []string {
	return q.requiredTables
}
