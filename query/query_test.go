package query_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hojulian/microdb/query"
)

//nolint // Disable linter for test.
func TestQuery(t *testing.T) {
	testCases := []struct {
		desc             string
		q                string
		queryType        query.QueryType
		destinationType  query.DestinationType
		destinationTable string
		requiredTables   []string
		err              error
	}{
		{
			desc:            "select from 1 table",
			q:               "SELECT a.age, a.name FROM anacondas a",
			queryType:       query.QueryTypeSelect,
			destinationType: query.DestinationTypeLocal,
			requiredTables:  []string{"anacondas"},
		},
		{
			desc:            "select from 2 tables left join",
			q:               "SELECT p.name FROM people p LEFT JOIN cities c ON p.city = c.name AND p.favorite_city = c.name",
			queryType:       query.QueryTypeSelect,
			destinationType: query.DestinationTypeLocal,
			requiredTables:  []string{"cities", "people"},
		},
		{
			desc: "select from 1 table subquery",
			q: `
			SELECT p3.name, (SELECT p1.city FROM people p1 WHERE p3.name = 'Kuba' AND p1.name = 'adam') as city
			FROM (Select * from people p4) p3
			WHERE (SELECT p2.age FROM people p2 WHERE p2.name = 'wojtek') > p3.age`,
			queryType:       query.QueryTypeSelect,
			destinationType: query.DestinationTypeLocal,
			requiredTables:  []string{"people"},
		},
		{
			desc:             "insert into 1 table",
			q:                "INSERT INTO a VALUES (1)",
			queryType:        query.QueryTypeInsert,
			destinationType:  query.DestinationTypeOrigin,
			destinationTable: "a",
			requiredTables:   []string{"a"},
		},
		{
			desc: "insert into 1 table on select",
			q: `
			INSERT INTO table4 ( name, age, sex, city, id, number, nationality)
			SELECT name, age, sex, city, p.id, number, n.nationality
			FROM table1 p
			INNER JOIN table2 c ON c.Id = p.Id
			INNER JOIN table3 n ON p.Id = n.Id`,
			queryType:        query.QueryTypeInsert,
			destinationType:  query.DestinationTypeOrigin,
			destinationTable: "table4",
			requiredTables:   []string{"table1", "table2", "table3", "table4"},
		},
		{
			desc:            "update 1 table",
			q:               "UPDATE tt AS aa SET aa.cc = 3",
			queryType:       query.QueryTypeUpdate,
			destinationType: query.DestinationTypeOrigin,
			requiredTables:  []string{"tt"},
		},
		{
			desc:            "update 2 tables",
			q:               "UPDATE foo AS f JOIN bar AS b ON f.name = b.name SET f.id = b.id WHERE b.name = 'test'",
			queryType:       query.QueryTypeUpdate,
			destinationType: query.DestinationTypeOrigin,
			requiredTables:  []string{"foo", "bar"},
		},
		{
			desc: "unsupported operation: delete",
			q:    "DELETE FROM a1, a2 USING t1 AS a1 INNER JOIN t2 AS a2 WHERE a1.id=a2.id",
			err: fmt.Errorf(
				"failed to parse query: %w",
				fmt.Errorf(
					"failed to parse statement: %w",
					errors.New("unsupported query statement type"))),
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			actual, err := query.Query(tC.q)
			if err != nil {
				assert.Equal(t, tC.err, err, "unexpected error")
				return
			}

			assert.Equal(t, tC.destinationTable, actual.GetDestinationTable(), "unequal destination table")
			assert.Equal(t, tC.destinationType, actual.GetDestinationType(), "unequal destination type")
			assert.Equal(t, tC.queryType, actual.GetQueryType(), "unequal query type")
			assert.Equal(t, tC.q, actual.SQL(), "unequal SQL query")
			assert.ElementsMatch(t, tC.requiredTables, actual.GetRequiredTables(), "unequal required tables")
		})
	}
}
