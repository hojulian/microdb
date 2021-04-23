// Package microdb includes all application level components used either with MicroDB client or
// with in MicroDB system.
package microdb

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// DataOriginCfg represents a config file with all the data origins.
type DataOriginCfg struct {
	Origins map[string]*DataOrigin `yaml:",inline"`
}

// AddDataOriginFromCfg parses a config file and registers the data origins.
func AddDataOriginFromCfg(name string) error {
	path := filepath.Clean(name)
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to open config file")
	}

	var cfg DataOriginCfg
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	for t, do := range cfg.Origins {
		dataOrigins[t] = do
		schemaStore[t] = do.Schema
	}

	return nil
}
