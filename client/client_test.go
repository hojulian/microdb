package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hojulian/microdb/client"
	"github.com/hojulian/microdb/internal/test"
	"github.com/hojulian/microdb/microdb"
)

var (
	requestTimeout = 30 * time.Second
	propagateTime  = 3 * time.Second
)

func TestClient(t *testing.T) {
	testCases := []struct {
		desc string
		id   int
		v1   string
		v2   int
		v3   float32
		v4   bool
		v5   time.Time
	}{
		{
			desc: "simple values",
			id:   333,
			v1:   "test-333",
			v2:   123213,
			v3:   123.2122,
			v4:   true,
			v5:   time.Now(),
		},
	}

	setup(t)

	c, err := client.Connect("127.0.0.1", "4222", "client-unit-test", "nats-cluster", test.TestTableName)
	if err != nil {
		t.Fatalf("failed to create client: %s", err)
	}
	defer c.Close()

	q, err := microdb.InsertQuery(test.TestTableName)
	if err != nil {
		t.Logf("failed to retrieve insert query: %s", err)
		t.Fail()
		return
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			ctx, cFunc := context.WithTimeout(context.Background(), requestTimeout)
			defer cFunc()

			r, err := c.Execute(ctx, q, tC.id, tC.v1, tC.v2, tC.v3, tC.v4, tC.v5)
			if err != nil {
				t.Logf("failed to execute query: %s", err)
				t.Fail()
				return
			}

			if _, err := r.RowsAffected(); err != nil {
				t.Logf("failed to update database: %s", err)
				t.Fail()
				return
			}

			time.Sleep(propagateTime)

			// Select the same row back
			ctx, cFunc = context.WithTimeout(context.Background(), requestTimeout)
			defer cFunc()

			sq := `SELECT id, string_type, int_type, float_type, bool_type, timestamp_type FROM test WHERE id = ?`
			rs, err := c.Query(ctx, sq, tC.id)
			if err != nil {
				t.Logf("failed to query: %s", err)
				t.Fail()
				return
			}
			if err := rs.Err(); err != nil {
				t.Logf("result error: %s", err)
				t.Fail()
				return
			}

			for rs.Next() {
				var (
					id int
					v1 string
					v2 int
					v3 float32
					v4 bool
					v5 time.Time
				)

				if err := rs.Scan(&id, &v1, &v2, &v3, &v4, &v5); err != nil {
					t.Logf("failed to read result from query: %s", err)
					t.Fail()
					return
				}

				assert.Equal(t, tC.id, id)
				assert.Equal(t, tC.v1, v1)
				assert.Equal(t, tC.v2, v2)
				assert.Equal(t, tC.v3, v3)
				assert.Equal(t, tC.v4, v4)
				assert.Equal(t, tC.v5.Format("2021-04-23 09:38:19"), v5.Format("2021-04-23 09:38:19"))
			}
		})
	}
}

func setup(t *testing.T) {
	if err := microdb.AddDataOrigin(
		test.TestTableName,
		microdb.WithMySQLDataOrigin("127.0.0.1", "3306", "root", "test", "test", test.TestSchemaOption),
	); err != nil {
		t.Fatalf("failed to add test data origin: %s", err)
	}
}
