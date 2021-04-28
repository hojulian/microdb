package publisher_test

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	fuzz "github.com/google/gofuzz"
	"github.com/nats-io/stan.go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	pb "github.com/hojulian/microdb/internal/proto"
	"github.com/hojulian/microdb/internal/test"
	"github.com/hojulian/microdb/microdb"
	"github.com/hojulian/microdb/publisher"
)

var (
	sc stan.Conn
	db *sql.DB
)

func TestMain(m *testing.M) {
	var err error

	cid := fmt.Sprintf("test-publisher-client-%s", test.UUID())

	// Connect to nats
	sc, err = microdb.NATSConn("127.0.0.1", "4222", "nats-cluster", cid, nil, nil)
	if err != nil {
		log.Fatalf("failed to connect to NATS cluster: %s", err)
	}

	// Create data origin
	err = microdb.AddDataOrigin(test.TestTableName, microdb.WithMySQLDataOrigin(
		"127.0.0.1",
		"3306",
		"root",
		"test",
		"test",
		test.TestSchemaOption,
	))
	if err != nil {
		log.Fatalf("failed to create data origin: %s", err)
	}

	do, err := microdb.GetDataOrigin(test.TestTableName)
	if err != nil {
		log.Fatalf("failed to verify data origin: %s", err)
	}

	db, err = do.GetDB()
	if err != nil {
		log.Fatalf("failed to retrieve database client: %s", err)
	}

	// Create publisher
	pub, err := publisher.MySQLHandler("127.0.0.1", "3306", "root", "test", "test", 1, sc, "test")
	if err != nil {
		log.Fatalf("failed to create publisher: %s", err)
	}
	go func() {
		if err := pub.Handle(); err != nil {
			log.Fatalf("failed to start publisher: %s", err)
		}
	}()

	code := m.Run()

	if err := pub.Close(); err != nil {
		log.Fatalf("failed to close publisher: %s", err)
	}
	if err := sc.Close(); err != nil {
		log.Fatalf("failed to close connection to nats: %s", err)
	}

	os.Exit(code)
}

func TestHandle(t *testing.T) {
	testCases := []struct {
		desc  string
		count int
	}{
		{
			desc:  "insert 1 row",
			count: 1,
		},
	}

	do, err := microdb.GetDataOrigin(test.TestTableName)
	if err != nil {
		t.Errorf("failed to get data origin for table: %w", err)
	}

	sub, err := sc.NatsConn().SubscribeSync(do.ReadTopic())
	if err != nil {
		t.Errorf("failed to subscribe to test topic: %w", err)
		return
	}
	defer assert.Nil(t, sub.Unsubscribe())

	q, err := microdb.InsertQuery(test.TestTableName)
	if err != nil {
		t.Errorf("failed to get insert query: %w", err)
		return
	}

	f := fuzz.New()

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			for i := 0; i < tC.count; i++ {
				var (
					val0 uint32
					val1 string
					val2 int
					val3 float32
					val4 bool
					val5 time.Time
				)

				f.Fuzz(&val0)
				val0 %= 10000

				f.Fuzz(&val1)
				f.Fuzz(&val2)
				val2 %= math.MaxInt32

				f.Fuzz(&val3)
				f.Fuzz(&val4)
				val5 = time.Now()

				r, err := db.Exec(q, val0, val1, val2, val3, val4, &val5)
				assert.Nil(t, err)

				ra, err := r.RowsAffected()
				assert.Nil(t, err)
				assert.NotEqual(t, ra, 0)

				recMsg, err := sub.NextMsg(10 * time.Second)
				assert.Nil(t, err)

				var ru pb.RowUpdate
				err = proto.Unmarshal(recMsg.Data, &ru)
				assert.Nil(t, err)

				assert.Equal(t, []interface{}{val0, val1, val2, val3, val4, val5}, pb.UnmarshalValues(ru.GetRow()))
			}
		})
	}
}
