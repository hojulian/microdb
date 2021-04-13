package microdb

import (
	"log"
	"os"
)

func Logger(name string) *log.Logger {
	return log.New(os.Stdout, name, log.Ltime|log.Lmicroseconds|log.Llongfile)
}
