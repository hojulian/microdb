// Package test provides library functions for testing the MicroDB system.
package test

import (
	uuid "github.com/satori/go.uuid"
)

// UUID generates a unique UUID for the duration of a test.
func UUID() string {
	return uuid.NewV4().String()
}
