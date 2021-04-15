package query

import (
	"fmt"
)

const (
	QueryTypeSelect = iota
	QueryTypeInsert
	QueryTypeUpdate
	DestinationTypeLocal
	DestinationTypeOrigin
)

type QueryType int

type DestinationType int

type QueryStmt struct {
	originQuery string

	QueryType        QueryType
	DestinationType  DestinationType
	destinationTable string
	requiredTables   []string

	query string
}

func Query(query string) (*QueryStmt, error) {
	qs, err := parseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	return qs, nil
}

func (q *QueryStmt) OnLocal() *QueryStmt {
	q.DestinationType = DestinationTypeLocal
	return q
}

func (q *QueryStmt) OnOrigin() *QueryStmt {
	q.DestinationType = DestinationTypeOrigin
	return q
}

func (q *QueryStmt) SQL() string {
	return q.query
}

func (q *QueryStmt) GetDestinationTable() string {
	return q.destinationTable
}
