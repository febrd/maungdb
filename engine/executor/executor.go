package executor

import (
	"errors"
	"strings"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
)

func Execute(cmd *parser.Command) ([]string, error) {
	switch cmd.Type {
	case parser.CmdInsert:
		return execInsert(cmd)
	case parser.CmdSelect:
		return execSelect(cmd)
	default:
		return nil, errors.New("command teu didukung")
	}
}

func execInsert(cmd *parser.Command) ([]string, error) {
	user, _ := auth.CurrentUser()

	s, err := schema.Load(user.Database, cmd.Table)
	if err != nil {
		return nil, err
	}

	if !s.Can(user.Role, "write") {
		return nil, errors.New("teu boga hak nulis")
	}

	if err := s.ValidateRow(cmd.Data); err != nil {
		return nil, err
	}

	if err := storage.Append(cmd.Table, cmd.Data); err != nil {
		return nil, err
	}

	return []string{"OK"}, nil
}

func execSelect(cmd *parser.Command) ([]string, error) {
	user, _ := auth.CurrentUser()

	s, err := schema.Load(user.Database, cmd.Table)
	if err != nil {
		return nil, err
	}

	if !s.Can(user.Role, "read") {
		return nil, errors.New("teu boga hak maca")
	}

	rows, err := storage.ReadAll(cmd.Table)
	if err != nil {
		return nil, err
	}

	if cmd.Where == nil {
		return rows, nil
	}

	var result []string
	colIndex := indexOf(cmd.Where.Field, s.Fields)
	if colIndex < 0 {
		return nil, errors.New("kolom teu kapanggih")
	}

	for _, row := range rows {
		values := strings.Split(row, "|")
		if match(values[colIndex], cmd.Where.Operator, cmd.Where.Value) {
			result = append(result, row)
		}
	}

	return result, nil
}

func indexOf(field string, fields []string) int {
	for i, f := range fields {
		if f == field {
			return i
		}
	}
	return -1
}

func match(a, op, b string) bool {
	switch op {
	case "=":
		return a == b
	case "!=":
		return a != b
	case ">":
		return a > b
	case "<":
		return a < b
	case ">=":
		return a >= b
	case "<=":
		return a <= b
	default:
		return false
	}
}
