// Package main contains a querier server.
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/hojulian/microdb/internal/logger"
	"github.com/hojulian/microdb/microdb"
	"github.com/hojulian/microdb/querier"
)

func main() {
	log := logger.Logger("querier")

	mysqlHost := os.Getenv("MYSQL_HOST")
	mysqlPort := os.Getenv("MYSQL_PORT")
	mysqlUser := os.Getenv("MYSQL_USER")
	mysqlPassword := os.Getenv("MYSQL_PASSWORD")
	mysqlDatabase := os.Getenv("MYSQL_DATABASE")
	mysqlTable := os.Getenv("MYSQL_TABLE")
	dataOriginPath := os.Getenv("DATAORIGIN_CFG")

	if err := microdb.AddDataOriginFromCfg(dataOriginPath); err != nil {
		log.Fatalf("failed to parse data origin configs: %v", err)
	}

	sc, err := microdb.NATSConnFromEnv()
	if err != nil {
		log.Fatalf("failed to create nats connection: %v", err)
	}

	q, err := querier.MySQLHandler(
		mysqlHost,
		mysqlPort,
		mysqlUser,
		mysqlPassword,
		mysqlDatabase,
		mysqlTable,
		sc,
	)
	if err != nil {
		log.Fatalf("failed to create mysql querier: %v", err)
	}

	if err := q.Handle(); err != nil {
		log.Fatalf("failed to publish to table %s: %v", mysqlTable, err)
	}

	log.Printf("Querier for %s is ready.", mysqlTable)

	s := make(chan os.Signal, 1)
	signal.Notify(s,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-s

	if err := q.Close(); err != nil {
		log.Fatalf("failed to close connections: %v", err)
	}
}
