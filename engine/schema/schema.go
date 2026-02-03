package schema

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/febrd/maungdb/internal/config"
)

type Schema struct {
	Table       string              `json:"table"`
	Fields      []string            `json:"fields"`
	Permissions map[string][]string `json:"permissions"`
}

func Load(table string) (*Schema, error) {
	path := filepath.Join(
		config.DataDir,
		config.SchemaDir,
		table+".tpk",
	)

	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("schema teu kapanggih")
	}
	defer file.Close()

	var s Schema
	if err := json.NewDecoder(file).Decode(&s); err != nil {
		return nil, err
	}

	return &s, nil
}

func Create(table string, fields []string, perms map[string][]string) error {
	path := filepath.Join(
		config.DataDir,
		config.SchemaDir,
		table+".tpk",
	)

	if _, err := os.Stat(path); err == nil {
		return errors.New("schema geus aya")
	}

	s := Schema{
		Table:       table,
		Fields:      fields,
		Permissions: perms,
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(s)
}

func (s *Schema) ValidateRow(row string) error {
	values := strings.Split(row, "|")
	if len(values) != len(s.Fields) {
		return errors.New("jumlah kolom teu saluyu jeung schema")
	}
	return nil
}

func (s *Schema) Can(role, action string) bool {
	roles, ok := s.Permissions[action]
	if !ok {
		return false
	}

	for _, r := range roles {
		if r == role {
			return true
		}
	}
	return false
}
