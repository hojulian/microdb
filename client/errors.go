package client //nolint // Package comment located in a different file.

import "errors"

// MicroDB errors represents all error values returned by MicroDB client.

var (
	// ErrNATSError represents the client lost connection to NATS.
	ErrNATSError = errors.New("NATS connection error")

	// ErrLocalDBError represents the client lost connection to local database or it is not ready
	// for operations yet.
	ErrLocalDBError = errors.New("local sqlite3 database connection error")
)
