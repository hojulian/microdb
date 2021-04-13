package microdb

import (
	"fmt"
	"os"

	"github.com/nats-io/stan.go"
)

func NATSConn() (stan.Conn, error) {
	natsHost := os.Getenv("NATS_HOST")
	natsURL := fmt.Sprintf("nats://%s:4222", natsHost)

	sc, err := stan.Connect("test-cluster", "ingress", stan.NatsURL(natsURL))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	return sc, nil
}
