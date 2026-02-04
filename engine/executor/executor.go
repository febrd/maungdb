package executor

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
)

type ExecutionResult struct {
	Columns []string
	Rows    [][]string
	Message string
}

func Execute(cmd *parser.Command) (*ExecutionResult, error) {
	switch cmd.Type {
	case parser.CmdCreate:
		return execCreate(cmd)
	case parser.CmdInsert:
		return execInsert(cmd)
	case parser.CmdSelect:
		return execSelect(cmd)
	case parser.CmdUpdate:
		return execUpdate(cmd)
	case parser.CmdDelete:
		return execDelete(cmd)
	default:
		return nil, errors.New("command teu didukung")
	}
}

func execCreate(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()
	fields := splitColumns(cmd.Data)

	perms := map[string][]string{
		"read":  {"user", "admin", "supermaung"},
		"write": {"admin", "supermaung"},
	}

	if err := schema.Create(user.Database, cmd.Table, fields, perms); err != nil {
		return nil, err
	}

	return &ExecutionResult{Message: fmt.Sprintf("✅ Tabel '%s' parantos didamel!", cmd.Table)}, nil
}

func splitColumns(input string) []string {
	var fields []string
	var currentField strings.Builder
	parenCount := 0 

	for _, char := range input {
		switch char {
		case '(':
			parenCount++
			currentField.WriteRune(char)
		case ')':
			parenCount--
			currentField.WriteRune(char)
		case ',':
			if parenCount == 0 {
				fields = append(fields, strings.TrimSpace(currentField.String()))
				currentField.Reset()
			} else {
				currentField.WriteRune(char)
			}
		default:
			currentField.WriteRune(char)
		}
	}
	
	if currentField.Len() > 0 {
		fields = append(fields, strings.TrimSpace(currentField.String()))
	}

	return fields
}

func execInsert(cmd *parser.Command) (*ExecutionResult, error) {
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

	return &ExecutionResult{
		Message: fmt.Sprintf("✅ Data asup ka table '%s'", cmd.Table),
	}, nil
}

func execSelect(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()
	s, err := schema.Load(user.Database, cmd.Table)
	if err != nil { 
		return nil, err 
	}
	if !s.Can(user.Role, "read") { 
		return nil, errors.New("teu boga hak maca") 
	}

	rawRows, err := storage.ReadAll(cmd.Table)
	if err != nil { 
		return nil, err 
	}

	var parsedRows [][]string
	fieldNames := s.GetFieldNames()

	for _, raw := range rawRows {
		if raw == "" { continue }
		cols := strings.Split(raw, "|")

		if len(cmd.Where) > 0 {
			matchAll := true
			currentMatch := evaluateOne(cols, s.Columns, cmd.Where[0])
			for i := 0; i < len(cmd.Where); i++ {
				cond := cmd.Where[i]
				if cond.LogicOp == "" { 
					matchAll = currentMatch; break 
				}
				nextResult := evaluateOne(cols, s.Columns, cmd.Where[i+1])
				if cond.LogicOp == "SARENG" || cond.LogicOp == "sareng"  { 
					currentMatch = currentMatch && nextResult 
				} else if cond.LogicOp == "ATAWA" || cond.LogicOp == "atawa" { 
					currentMatch = currentMatch || nextResult 
				}
			}
			if !matchAll { continue }
		}
		parsedRows = append(parsedRows, cols)
	}

	if cmd.OrderBy != "" {
		colIdx := indexOf(cmd.OrderBy, fieldNames)
		if colIdx == -1 { 
			return nil, fmt.Errorf("kolom '%s' teu kapanggih", cmd.OrderBy) 
		}
		
		colType := s.Columns[colIdx].Type

		sort.Slice(parsedRows, func(i, j int) bool {
			valA := parsedRows[i][colIdx]
			valB := parsedRows[j][colIdx]
			isLess := false
			
			switch colType {
			case "INT":
				a, _ := strconv.Atoi(valA)
				b, _ := strconv.Atoi(valB)
				isLess = a < b
			case "FLOAT":
				a, _ := strconv.ParseFloat(valA, 64)
				b, _ := strconv.ParseFloat(valB, 64)
				isLess = a < b
			default: 
				isLess = valA < valB
			}

			if cmd.OrderDesc {
				return !isLess 
			}
			return isLess 
		})
	}

	totalRows := len(parsedRows)
	start := 0
	end := totalRows

	if cmd.Offset > 0 {
		start = cmd.Offset
		if start > totalRows { start = totalRows }
	}

	if cmd.Limit > 0 {
		end = start + cmd.Limit
		if end > totalRows { end = totalRows }
	}

	finalRows := parsedRows[start:end]

	return &ExecutionResult{
		Columns: fieldNames,
		Rows:    finalRows,
	}, nil
}


func execUpdate(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()
	s, err := schema.Load(user.Database, cmd.Table)
	if err != nil { return nil, err }
	if !s.Can(user.Role, "write") { return nil, errors.New("teu boga hak nulis (omean)") }

	rawRows, err := storage.ReadAll(cmd.Table)
	if err != nil { return nil, err }

	var newRows []string
	updatedCount := 0

	for _, raw := range rawRows {
		if raw == "" { continue }
		cols := strings.Split(raw, "|")

		shouldUpdate := false
		if len(cmd.Where) == 0 {
			shouldUpdate = true 
		} else {
			currentMatch := evaluateOne(cols, s.Columns, cmd.Where[0])
			shouldUpdate = currentMatch
		}

		if shouldUpdate {
			for colName, newVal := range cmd.Updates {
				idx := indexOf(colName, s.GetFieldNames())
				if idx != -1 {
					cols[idx] = newVal 
				}
			}
			updatedCount++
		}
		
		newRows = append(newRows, strings.Join(cols, "|"))
	}

	if err := storage.Rewrite(cmd.Table, newRows); err != nil {
		return nil, err
	}

	return &ExecutionResult{Message: fmt.Sprintf("✅ %d data geus diomean", updatedCount)}, nil
}

func execDelete(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()
	s, err := schema.Load(user.Database, cmd.Table)
	if err != nil { return nil, err }
	if !s.Can(user.Role, "write") { return nil, errors.New("teu boga hak nulis (miceun)") }

	rawRows, err := storage.ReadAll(cmd.Table)
	if err != nil { return nil, err }

	var newRows []string
	deletedCount := 0

	for _, raw := range rawRows {
		if raw == "" { continue }
		cols := strings.Split(raw, "|")

		shouldDelete := false
		if len(cmd.Where) > 0 {
			shouldDelete = evaluateOne(cols, s.Columns, cmd.Where[0])
		}

		if shouldDelete {
			deletedCount++
			continue 
		}
		
		newRows = append(newRows, raw)
	}

	if err := storage.Rewrite(cmd.Table, newRows); err != nil {
		return nil, err
	}

	return &ExecutionResult{Message: fmt.Sprintf("✅ %d data geus dipiceun", deletedCount)}, nil
}

func evaluateOne(row []string, cols []schema.Column, cond parser.Condition) bool {
	idx := -1
	var colType string

	for i, c := range cols {
		if c.Name == cond.Field {
			idx = i
			colType = c.Type
			break
		}
	}

	if idx < 0 || idx >= len(row) {
		return false
	}

	return match(row[idx], cond.Operator, cond.Value, colType)
}

func indexOf(field string, fields []string) int {
	for i, f := range fields {
		if f == field {
			return i
		}
	}
	return -1
}
func match(a, op, b, colType string) bool {
	if strings.ToUpper(op) == "JIGA" {
		return strings.Contains(strings.ToLower(a), strings.ToLower(b))
	}

	switch colType {
	case "INT":
		numA, errA := strconv.Atoi(a)
		numB, errB := strconv.Atoi(b)
		
		if errA != nil || errB != nil {
			return false
		}

		switch op {
		case "=":  return numA == numB
		case "!=": return numA != numB
		case ">":  return numA > numB
		case "<":  return numA < numB
		case ">=": return numA >= numB
		case "<=": return numA <= numB
		}

	case "FLOAT":
		fA, errA := strconv.ParseFloat(a, 64)
		fB, errB := strconv.ParseFloat(b, 64)
		
		if errA != nil || errB != nil {
			return false
		}

		switch op {
		case "=":  return fA == fB
		case "!=": return fA != fB
		case ">":  return fA > fB
		case "<":  return fA < fB
		case ">=": return fA >= fB
		case "<=": return fA <= fB
		}

	case "BOOL":
		if op == "=" { return a == b }
		if op == "!=" { return a != b }
		return false


	case "STRING", "TEXT", "CHAR", "ENUM", "DATE":
		switch op {
		case "=":  return a == b
		case "!=": return a != b
		case ">":  return a > b
		case "<":  return a < b
		case ">=": return a >= b
		case "<=": return a <= b
		}

	default:
		return false
	}
	
	return false
}