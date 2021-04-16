// Package main contains a publisher server.
package main

import (
	"os"

	"github.com/hojulian/microdb/internal/logger"
	"github.com/hojulian/microdb/microdb"
	"github.com/hojulian/microdb/publisher"
)

func main() {
	log := logger.Logger("publisher")

	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlPort := os.Getenv("MYSQL_PORT")
	mysqlUser := os.Getenv("MYSQL_USER")
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	mysqlDatabase := os.Getenv("MYSQL_DATABASE")
	mysqlTable := os.Getenv("MYSQL_TABLE")

	sc, err := microdb.NATSConnFromEnv()
	if err != nil {
		log.Fatalf("failed to create nats connection: %v", err)
	}

	h, err := publisher.MySQLHandler(
		mysqlHost,
		mysqlPort,
		mysqlUser,
		mysqlPassword,
		mysqlDatabase,
		mysqlTable,
		sc,
	)
	if err != nil {
		log.Fatalf("failed to create mysql handler: %v", err)
	}

	if err := h.Handle(); err != nil {
		log.Fatalf("failed to publish to table %s: %v", mysqlTable, err)
	}

	if err := h.Close(); err != nil {
		log.Fatalf("failed to close connections: %v", err)
	}
}
