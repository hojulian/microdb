package microdb_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/nats-io/stan.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"

	"github.com/hojulian/microdb/internal/test"
	"github.com/hojulian/microdb/microdb"
)

var sc stan.Conn

func TestMain(m *testing.M) {
	p, err := dockertest.NewPool("unix:///var/run/docker.sock")
	if err != nil {
		log.Fatalf("failed to connect to docker: %v", err)
	}

	n, err := p.CreateNetwork("test-nats-conn-network")
	if err != nil {
		log.Fatalf("failed to create docker network: %v", err)
	}

	cid := "test-nats-conn-cluster"
	clid := "test-nats-conn-client"
	c, err := test.NATS(p, n, cid)
	if err != nil {
		log.Fatalf("unexpected error: %v", err)
	}

	err = p.Retry(func() error {
		sc, err = microdb.NATSConn("localhost", c.GetPort("4222/tcp"), cid, clid, nil, nil)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatalf("failed to connect to nats: %v", err)
	}

	code := m.Run()

	if err := sc.Close(); err != nil {
		log.Fatalf("failed to close connection to nats: %v", err)
	}
	if err := c.Purge(); err != nil {
		log.Fatalf("failed to purge nats container: %v", err)
	}
	if err := n.Close(); err != nil {
		log.Fatalf("failed to purge docker network: %v", err)
	}

	os.Exit(code)
}

func TestPublish(t *testing.T) {
	testCases := []struct {
		desc     string
		topic    string
		messages []string
		err      error
	}{
		{
			desc:     "publish one message",
			topic:    "test_publish_topic_1",
			messages: []string{"testing123"},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			sub, err := sc.NatsConn().SubscribeSync(tC.topic)
			if err != nil {
				t.Errorf("failed to subscribe to test topic: %w", err)
				return
			}
			defer assert.Nil(t, sub.Unsubscribe())

			for _, msg := range tC.messages {
				if err := sc.Publish(tC.topic, []byte(msg)); err != nil {
					assert.Equal(t, tC.err, err)
					return
				}

				recMsg, err := sub.NextMsg(10 * time.Second)
				if err != nil {
					t.Errorf("unexpected error from receive: %w", err)
					return
				}

				assert.Equal(t, msg, string(recMsg.Data))
			}
		})
	}
}
