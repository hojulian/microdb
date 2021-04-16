// Package microdb includes all application level components used either with MicroDB client or
// with in MicroDB system.
package microdb

import (
	"fmt"
	"os"

	"github.com/nats-io/stan.go"
)

// NATSConnFromEnv create a NATS connection from environment variables.
func NATSConnFromEnv() (stan.Conn, error) {
	natsHost := os.Getenv("NATS_HOST")
	natsPort := os.Getenv("NATS_PORT")
	natsClusterID := os.Getenv("NATS_CLUSTER_ID")
	natsClientID := os.Getenv("NATS_CLIENT_ID")

	return NATSConn(natsHost, natsPort, natsClusterID, natsClientID)
}

// NATSConn creates a NATS connection.
func NATSConn(host, port, clusterID, clientID string, opt ...stan.Option) (stan.Conn, error) {
	url := fmt.Sprintf("nats://%s:%s", host, port)
	opts := append(opt, stan.NatsURL(url))
	sc, err := stan.Connect(clusterID, clientID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", err)
	}

	return sc, nil
}
