package view

import (
	"os"
	"path/filepath"
	"github.com/febrd/maungdb/internal/config"
)

func IsView(dbName, viewName string) bool {
	path := getViewPath(dbName, viewName)
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

func SaveView(dbName, viewName, query string) error {
	path := getViewPath(dbName, viewName)
	return os.WriteFile(path, []byte(query), 0644)
}

func LoadView(dbName, viewName string) (string, error) {
	path := getViewPath(dbName, viewName)
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func DeleteView(dbName, viewName string) error {
	path := getViewPath(dbName, viewName)
	return os.Remove(path)
}

func getViewPath(dbName, viewName string) string {
	return filepath.Join(config.DataDir, "db_"+dbName, viewName+".view")
}