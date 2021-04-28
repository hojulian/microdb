// Package main contains a publisher server.
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hojulian/microdb/internal/logger"
	"github.com/hojulian/microdb/microdb"
	"github.com/hojulian/microdb/publisher"
)

func main() {
	var (
		log            = logger.Logger("publisher")
		natsHost       = os.Getenv("NATS_HOST")
		natsPort       = os.Getenv("NATS_PORT")
		natsClusterID  = os.Getenv("NATS_CLUSTER_ID")
		mysqlHost      = os.Getenv("MYSQL_HOST")
		mysqlPort      = os.Getenv("MYSQL_PORT")
		mysqlUser      = os.Getenv("MYSQL_USER")
		mysqlPassword  = os.Getenv("MYSQL_PASSWORD")
		mysqlDatabase  = os.Getenv("MYSQL_DATABASE")
		mysqlTables    = os.Getenv("MYSQL_TABLES")
		dataOriginPath = os.Getenv("DATAORIGIN_CFG")
		id             = os.Getenv("PUBLISHER_ID")
	)

	if id == "" {
		log.Fatalf("empty publisher ID")
	}
	pid, err := strconv.ParseInt(id, 10, 32)
	if err != nil {
		log.Fatalf("publisher ID must be an integer")
	}
	tables := parseTablesString(mysqlTables)

	if err = microdb.AddDataOriginFromCfg(dataOriginPath); err != nil {
		log.Fatalf("failed to parse data origin configs: %v", err)
	}

	sc, err := microdb.NATSConn(
		natsHost,
		natsPort,
		natsClusterID,
		fmt.Sprintf("publisher-%s-%d", strings.Join(tables, "-"), pid),
		nil,
		nil,
	)
	if err != nil {
		log.Fatalf("failed to create nats connection: %v", err)
	}

	h, err := publisher.MySQLHandler(
		mysqlHost,
		mysqlPort,
		mysqlUser,
		mysqlPassword,
		mysqlDatabase,
		uint32(pid),
		sc,
		tables...,
	)
	if err != nil {
		log.Fatalf("failed to create mysql handler: %v", err)
	}

	if err := h.Handle(); err != nil {
		log.Fatalf("failed to publish to tables %s: %v", mysqlTables, err)
	}
	if err := h.Close(); err != nil {
		log.Fatalf("failed to close connections: %v", err)
	}
}

func parseTablesString(mysqlTables string) []string {
	tables := []string{}
	for _, t := range strings.Split(mysqlTables, ",") {
		tables = append(tables, strings.TrimSpace(t))
	}

	return tables
}
