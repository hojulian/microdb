// Package microdb includes all application level components used either with MicroDB client or
// with in MicroDB system.
package microdb

import (
	"fmt"
	"os"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
)

// NATSConnFromEnv create a NATS connection from environment variables.
func NATSConnFromEnv() (stan.Conn, error) {
	var (
		natsHost      = os.Getenv("NATS_HOST")
		natsPort      = os.Getenv("NATS_PORT")
		natsClusterID = os.Getenv("NATS_CLUSTER_ID")
		natsClientID  = os.Getenv("NATS_CLIENT_ID")
	)

	return NATSConn(natsHost, natsPort, natsClusterID, natsClientID, nil, nil)
}

// NATSConn creates a NATS connection.
func NATSConn(host, port, clusterID, clientID string, sOpts []stan.Option, nOpts []nats.Option) (stan.Conn, error) {
	var nc *nats.Conn
	var sc stan.Conn
	var err error

	nOpts = append(nOpts, nats.Name(clientID))
	sOpts = append(sOpts, stan.PubAckWait(time.Minute))

	url := fmt.Sprintf("nats://%s:%s", host, port)
	nerr := retry(func() error {
		nc, err = nats.Connect(url, nOpts...)
		if err != nil {
			return fmt.Errorf("nats error: %w", err)
		}

		return nil
	})
	if nerr != nil {
		return nil, fmt.Errorf("failed to connect to nats: %w", nerr)
	}

	sOpts = append(sOpts, stan.NatsConn(nc))
	serr := retry(func() error {
		sc, err = stan.Connect(clusterID, clientID, sOpts...)
		if err != nil {
			return fmt.Errorf("stan error: %w", err)
		}

		return nil
	})
	if serr != nil {
		return nil, fmt.Errorf("failed to connect to nats using existing connection: %w", serr)
	}

	return sc, nil
}

//nolint // Internal method.
func retry(op func() error) error {
	bo := backoff.NewExponentialBackOff()
	bo.MaxInterval = time.Second * 5
	bo.MaxElapsedTime = time.Minute

	return backoff.Retry(op, bo)
}
