package storage

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/febrd/maungdb/internal/config"
)

func CreateDatabase(name string) error {
	dbPath := filepath.Join(config.DataDir, "db_"+name)
	if _, err := os.Stat(dbPath); err == nil {
		return errors.New("database geus aya")
	}
	if err := os.MkdirAll(filepath.Join(dbPath, config.SchemaDir), 0755); err != nil {
		return err
	}
	return nil
}

func DatabasePath(name string) string {
	return filepath.Join(config.DataDir, "db_"+name)
}
