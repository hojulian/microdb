package test //nolint // Package comment located in a different file.

import (
	"fmt"

	"github.com/ory/dockertest/v3"
	dc "github.com/ory/dockertest/v3/docker"
)

// Container represents a container resource.
type Container interface {
	GetPort(id string) string
	Purge() error
}

type dockerContainer struct {
	pool      *dockertest.Pool
	resources []*dockertest.Resource
	ports     map[string]string
}

func (c *dockerContainer) GetPort(id string) string {
	return c.ports[id]
}

func (c *dockerContainer) Purge() error {
	for _, r := range c.resources {
		if err := c.pool.Purge(r); err != nil {
			return fmt.Errorf("failed to purge node: %w", err)
		}
	}
	return nil
}

// TestDB creates a MySQL data origin for testing.
// Initialized with test.sql.
//nolint // This method name is fine.
func TestDB(pool *dockertest.Pool, network *dockertest.Network, password, database string) (Container, error) {
	r, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "microdb/testdb",
		Tag:        "client-test",
		Name:       "testdb-data-origin",
		Env:        []string{fmt.Sprintf("MYSQL_ROOT_PASSWORD=%s", password)},
		Cmd: []string{
			"--default-authentication-plugin=mysql_native_password",
			"--datadir=/var/lib/mysql",
			"--server-id=1",
			"--log-bin=/var/lib/mysql/mysql-bin.log",
			fmt.Sprintf("--binlog_do_db=%s", database),
			"--binlog-format=row",
		},
		Networks: []*dockertest.Network{network},
	}, func(hostConfig *dc.HostConfig) {
		hostConfig.AutoRemove = true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	ports := map[string]string{
		"3306/tcp": r.GetPort("3306/tcp"),
	}

	dc := &dockerContainer{
		pool:      pool,
		ports:     ports,
		resources: []*dockertest.Resource{r},
	}

	return dc, nil
}

// NATS starts a NATS Streaming Server cluster in docker containers.
func NATS(pool *dockertest.Pool, network *dockertest.Network, clusterID string) (Container, error) {
	ports := make(map[string]string)

	n, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "2.2.1",
		Name:       "test-nats-server",
		Cmd: []string{
			"-p=4222",
			"-m=8222",
		},
		Networks: []*dockertest.Network{network},
	}, func(hostConfig *dc.HostConfig) {
		hostConfig.AutoRemove = true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create NATS node: %w", err)
	}

	ns, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats-streaming",
		Tag:        "0.21.2",
		Name:       "test-nats-streaming-node",
		Cmd: []string{
			"-p=4222",
			"-m=8222",
			fmt.Sprintf("-cid=%s", clusterID),
			"-store=file",
			"-dir=store",
			"-nats_server=nats://test-nats-server:4222",
		},
		Networks: []*dockertest.Network{network},
	}, func(hostConfig *dc.HostConfig) {
		hostConfig.AutoRemove = true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create STAN node: %w", err)
	}

	ports["4222/tcp"] = n.GetPort("4222/tcp")

	return &dockerContainer{
		pool:      pool,
		ports:     ports,
		resources: []*dockertest.Resource{n, ns},
	}, nil
}

// Publisher starts a publisher in docker container.
//nolint // Allow duplicate statements with querier.
func Publisher(
	pool *dockertest.Pool,
	network *dockertest.Network,
	natsHost, natsPort, natsClusterID, natsClientID,
	mysqlHost, mysqlPort, mysqlUser, mysqlPassword, mysqlDatabase, mysqlTable string) (Container, error) {
	r, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "microdb/publisher",
		Tag:        "latest",
		Env: []string{
			fmt.Sprintf("MYSQL_HOST=%s", mysqlHost),
			fmt.Sprintf("MYSQL_PORT=%s", mysqlPort),
			fmt.Sprintf("MYSQL_USER=%s", mysqlUser),
			fmt.Sprintf("MYSQL_PASSWORD=%s", mysqlPassword),
			fmt.Sprintf("MYSQL_DATABASE=%s", mysqlDatabase),
			fmt.Sprintf("MYSQL_TABLE=%s", mysqlTable),
			fmt.Sprintf("NATS_HOST=%s", natsHost),
			fmt.Sprintf("NATS_PORT=%s", natsPort),
			fmt.Sprintf("NATS_CLUSTER_ID=%s", natsClusterID),
			fmt.Sprintf("NATS_CLIENT_ID=publisher-%s", natsClientID),
		},
		Networks: []*dockertest.Network{network},
	}, func(hostConfig *dc.HostConfig) {
		hostConfig.AutoRemove = true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	return &dockerContainer{
		pool:      pool,
		resources: []*dockertest.Resource{r},
		ports:     make(map[string]string),
	}, nil
}

// Querier starts a querier in docker container.
//nolint // Allow duplicate statements with publisher.
func Querier(
	pool *dockertest.Pool,
	network *dockertest.Network,
	natsHost, natsPort, natsClusterID, natsClientID,
	mysqlHost, mysqlPort, mysqlUser, mysqlPassword, mysqlDatabase, mysqlTable string) (Container, error) {
	r, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "microdb/querier",
		Tag:        "latest",
		Env: []string{
			fmt.Sprintf("MYSQL_HOST=%s", mysqlHost),
			fmt.Sprintf("MYSQL_PORT=%s", mysqlPort),
			fmt.Sprintf("MYSQL_USER=%s", mysqlUser),
			fmt.Sprintf("MYSQL_PASSWORD=%s", mysqlPassword),
			fmt.Sprintf("MYSQL_DATABASE=%s", mysqlDatabase),
			fmt.Sprintf("MYSQL_TABLE=%s", mysqlTable),
			fmt.Sprintf("NATS_HOST=%s", natsHost),
			fmt.Sprintf("NATS_PORT=%s", natsPort),
			fmt.Sprintf("NATS_CLUSTER_ID=%s", natsClusterID),
			fmt.Sprintf("NATS_CLIENT_ID=querier-%s", natsClientID),
		},
		Networks: []*dockertest.Network{network},
	}, func(hostConfig *dc.HostConfig) {
		hostConfig.AutoRemove = true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create querier: %w", err)
	}

	return &dockerContainer{
		pool:      pool,
		resources: []*dockertest.Resource{r},
		ports:     make(map[string]string),
	}, nil
}
