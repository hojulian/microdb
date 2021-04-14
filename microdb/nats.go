package microdb

import (
	"fmt"
	"os"

	"github.com/nats-io/stan.go"
)

func NATSConnFromEnv() (stan.Conn, error) {
	natsHost := os.Getenv("NATS_HOST")
	natsPort := os.Getenv("NATS_PORT")
	natsClusterID := os.Getenv("NATS_CLUSTER_ID")
	natsClientID := os.Getenv("NATS_CLIENT_ID")

	return NATSConn(natsHost, natsPort, natsClusterID, natsClientID)
}

func NATSConn(host, port, clusterID, clientID string) (stan.Conn, error) {
	url := fmt.Sprintf("nats://%s:%s", host, port)
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(url))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	return sc, nil
}
