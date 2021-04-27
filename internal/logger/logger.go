// Package logger provides a standardized logger used for MicroDB system components.
package logger

import (
	"log"
	"os"
)

// Logger returns a new logger.
func Logger(name string) *log.Logger {
	return log.New(os.Stdout, name+" ", log.Ltime|log.Lmicroseconds|log.Llongfile)
}
