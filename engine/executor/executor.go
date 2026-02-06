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
	"github.com/febrd/maungdb/engine/transaction"


)

type ExecutionResult struct {
	Columns []string
	Rows    [][]string
	Message string
}

func Execute(cmd *parser.Command) (*ExecutionResult, error) {
	switch cmd.Type {
	case parser.CmdTransaction:
		return execTransaction(cmd)
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

func execTransaction(cmd *parser.Command) (*ExecutionResult, error) {
	tm := transaction.GetManager()
	
	switch strings.ToUpper(cmd.Arg1) {
	case "MIMITIAN", "BEGIN":
		txID := tm.Begin()
		auth.SetSessionTxID(txID)
		return &ExecutionResult{Message: fmt.Sprintf("🏁 Transaksi dimimitian (ID: %s)", txID)}, nil
		
	case "JADIKEUN", "COMMIT":
		txID := auth.GetSessionTxID()
		if txID == "" { return nil, errors.New("teu aya transaksi nu jalan") }
		
		if err := tm.Commit(txID); err != nil {
			return nil, err
		}
		auth.SetSessionTxID("")
		return &ExecutionResult{Message: "💾 Transaksi sukses disimpen (Committed)"}, nil

	case "BATALKEUN", "ROLLBACK":
		txID := auth.GetSessionTxID()
		if txID == "" { return nil, errors.New("teu aya transaksi nu jalan") }
		
		if err := tm.Rollback(txID); err != nil {
			return nil, err
		}
		auth.SetSessionTxID("")
		return &ExecutionResult{Message: "🔙 Transaksi dibatalkeun (Rolled Back)"}, nil
		
	default:
		return nil, errors.New("parentah transaksi teu dikenal")
	}
}

func execCreate(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()

	columns := ParseColumnDefinitions(cmd.Data)
	if len(columns) == 0 {
		return nil, errors.New("gagal membuat tabel: tidak ada definisi kolom")
	}

	perms := map[string][]string{
		"read":  {"user", "admin", "supermaung"},
		"write": {"admin", "supermaung"},
	}

	if err := schema.CreateComplex(user.Database, cmd.Table, columns, perms); err != nil {
		return nil, err
	}

	if err := storage.InitTableFile(user.Database, cmd.Table); err != nil {
		return nil, fmt.Errorf("gagal inisialisasi storage: %v", err)
	}

	return &ExecutionResult{
		Message: fmt.Sprintf("✅ Tabel '%s' parantos didamel (Schema + Constraint Siap)", cmd.Table),
	}, nil
}

func ParseColumnDefinitions(input string) []schema.Column {
	var columns []schema.Column
	
	rawDefs := splitColumns(input)

	for _, def := range rawDefs {
		parts := strings.Split(def, ":")
		
		if len(parts) < 2 { continue }

		colName := strings.TrimSpace(parts[0])
		fullType := strings.ToUpper(strings.TrimSpace(parts[1]))
		baseType, args := parseTypeAndArgsExecutor(fullType)

		col := schema.Column{
			Name: colName,
			Type: baseType,
			Args: args,
		}

		if len(parts) > 2 {
			for _, constraintRaw := range parts[2:] {
				c := strings.ToUpper(strings.TrimSpace(constraintRaw))
				switch {
				case c == "PRIMARY" || c == "PK" || c == "PRIMARY KEY":
					col.IsPrimary = true; col.IsNotNull = true; col.IsUnique = true
				case c == "UNIQUE":
					col.IsUnique = true
				case c == "NOT NULL" || c == "NOTNULL":
					col.IsNotNull = true
				case strings.HasPrefix(c, "FK(") && strings.HasSuffix(c, ")"):
					inner := c[3 : len(c)-1]
					col.ForeignKey = inner
				}
			}
		}
		columns = append(columns, col)
	}
	return columns
}

func parseTypeAndArgsExecutor(fullType string) (string, []string) {
	idxStart := strings.Index(fullType, "(")
	idxEnd := strings.LastIndex(fullType, ")")
	if idxStart == -1 || idxEnd == -1 { return fullType, nil }

	base := fullType[:idxStart]
	content := fullType[idxStart+1 : idxEnd]
	rawArgs := strings.Split(content, ",")
	var args []string
	for _, a := range rawArgs { args = append(args, strings.TrimSpace(a)) }
	return base, args
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
	if err != nil { return nil, err }
	if !s.Can(user.Role, "write") { return nil, errors.New("teu boga hak nulis") }
	if err := s.ValidateRow(cmd.Data); err != nil { return nil, err }
	if err := ValidateConstraints(s, cmd.Table, cmd.Data); err != nil {
		return nil, fmt.Errorf("❌ Gagal Simpen: %v", err)
	}


	activeTxID := auth.GetSessionTxID() 

	if activeTxID != "" {
		tm := transaction.GetManager()
		err := tm.AddChange(activeTxID, transaction.OpInsert, cmd.Table, cmd.Data, "")
		if err != nil {
			return nil, fmt.Errorf("gagal nambah ke transaksi: %v", err)
		}
		return &ExecutionResult{Message: "✅ Data disimpen samentawis (nunggu JADIKEUN)"}, nil
	}

	if err := storage.Append(cmd.Table, cmd.Data); err != nil { return nil, err }
	return &ExecutionResult{Message: fmt.Sprintf("✅ Data asup ka table '%s'", cmd.Table)}, nil
}


func execSelect(cmd *parser.Command) (*ExecutionResult, error) {

	user, _ := auth.CurrentUser()

	sMain, err := schema.Load(user.Database, cmd.Table)
	if err != nil {
		return nil, fmt.Errorf("tabel '%s' teu kapanggih (pastikeun schema parantos didamel): %v", cmd.Table, err)
	}

	if !sMain.Can(user.Role, "read") {
		return nil, errors.New("akses ditolak: anjeun teu boga hak maca tabel ieu")
	}

	mainRaw, err := storage.ReadAll(cmd.Table)
	if err != nil {
		return nil, err
	}

	var currentHeader []string
	mainCols := sMain.GetFieldNames()
	for _, col := range mainCols {
		currentHeader = append(currentHeader, cmd.Table+"."+col)
	}

	var currentRows [][]string
	for _, row := range mainRaw {
		if strings.TrimSpace(row) == "" { continue }
		currentRows = append(currentRows, strings.Split(row, "|"))
	}

	if len(currentRows) == 0 && len(cmd.Joins) == 0 && !isAggregateCheck(cmd.Fields) {
		return &ExecutionResult{Columns: sMain.GetFieldNames(), Rows: [][]string{}, Message: "Data kosong"}, nil
	}

	for _, join := range cmd.Joins {
		targetSchema, err := schema.Load(user.Database, join.Table)
		if err != nil { return nil, fmt.Errorf("tabel join '%s' teu kapanggih", join.Table) }

		targetRaw, err := storage.ReadAll(join.Table)
		if err != nil { return nil, err }

		var targetHeaderFull []string
		targetCols := targetSchema.GetFieldNames()
		for _, h := range targetCols {
			targetHeaderFull = append(targetHeaderFull, join.Table+"."+h)
		}

		targetRows := [][]string{}
		for _, r := range targetRaw {
			if strings.TrimSpace(r) != "" { targetRows = append(targetRows, strings.Split(r, "|")) }
		}

		var nextRows [][]string
		matchedRightIndices := make(map[int]bool)

		for _, leftRow := range currentRows {
			matchedLeft := false

			for tIdx, rightRow := range targetRows {
				isMatch := evaluateJoinCondition(
					leftRow, rightRow,
					currentHeader, targetHeaderFull,
					cmd.Table, join.Table,
					join.Condition,
				)

				if isMatch {
					merged := append([]string{}, leftRow...)
					merged = append(merged, rightRow...)
					nextRows = append(nextRows, merged)
					
					matchedLeft = true
					matchedRightIndices[tIdx] = true
				}
			}

			if !matchedLeft && (join.Type == "LEFT" || join.Type == "KENCA") {
				merged := append([]string{}, leftRow...)
				for range targetHeaderFull { merged = append(merged, "NULL") }
				nextRows = append(nextRows, merged)
			}
		}

		if join.Type == "RIGHT" || join.Type == "KATUHU" {
			for tIdx, rightRow := range targetRows {
				if !matchedRightIndices[tIdx] {
					merged := []string{}
					for range currentHeader { merged = append(merged, "NULL") }
					merged = append(merged, rightRow...)
					nextRows = append(nextRows, merged)
				}
			}
		}

		currentRows = nextRows
		currentHeader = append(currentHeader, targetHeaderFull...)
	}

	var filteredMaps []map[string]string
	var filteredSlices [][]string

	for _, cols := range currentRows {
		rowMap := make(map[string]string)
		for i, val := range cols {
			if i < len(currentHeader) {
				fullKey := currentHeader[i]
				rowMap[fullKey] = val
				parts := strings.Split(fullKey, ".")
				if len(parts) > 1 { rowMap[parts[1]] = val }
			}
		}

		matches := true
		if len(cmd.Where) > 0 {
			matches = evaluateMapCondition(rowMap, cmd.Where)
		}

		if matches {
			filteredSlices = append(filteredSlices, cols)
			filteredMaps = append(filteredMaps, rowMap)
		}
	}

	selectedFields := cmd.Fields
	if len(selectedFields) == 0 || selectedFields[0] == "*" {
		selectedFields = currentHeader
	}

	isAggregateQuery := false
	var parsedCols []ParsedColumn
	for _, f := range selectedFields {
		pc := ParseColumnSelection(f)
		parsedCols = append(parsedCols, pc)
		if pc.IsAggregate { isAggregateQuery = true }
	}

	var finalResult [][]string
	var finalHeader []string

	if isAggregateQuery {
		var resultRow []string
		for _, pc := range parsedCols {
			finalHeader = append(finalHeader, pc.OriginalText)
			val, _ := CalculateAggregate(filteredMaps, pc)
			resultRow = append(resultRow, val)
		}
		finalResult = append(finalResult, resultRow)

	} else {

		if cmd.OrderBy != "" {
			colIdx := indexOf(cmd.OrderBy, currentHeader)
			if colIdx == -1 {
				for i, h := range currentHeader {
					if parts := strings.Split(h, "."); len(parts) > 1 && parts[1] == cmd.OrderBy {
						colIdx = i; break
					}
				}
			}

			if colIdx != -1 {
				sort.Slice(filteredSlices, func(i, j int) bool {
					valA := filteredSlices[i][colIdx]
					valB := filteredSlices[j][colIdx]
					fA, errA := strconv.ParseFloat(valA, 64)
					fB, errB := strconv.ParseFloat(valB, 64)

					isLess := false
					if errA == nil && errB == nil { isLess = fA < fB } else { isLess = valA < valB }
					
					if cmd.OrderDesc { return !isLess }
					return isLess
				})
			}
		}

		totalRows := len(filteredSlices)
		start, end := 0, totalRows
		if cmd.Offset > 0 { start = cmd.Offset; if start > totalRows { start = totalRows } }
		if cmd.Limit > 0 { end = start + cmd.Limit; if end > totalRows { end = totalRows } }
		
		slicedRows := filteredSlices[start:end]

		for _, pc := range parsedCols {
			displayText := pc.TargetCol
			if parts := strings.Split(displayText, "."); len(parts) > 1 {
				displayText = parts[1]
			}
			finalHeader = append(finalHeader, displayText)
		}

		for _, rowSlice := range slicedRows {
			tempMap := make(map[string]string)
			for i, val := range rowSlice {
				if i < len(currentHeader) {
					tempMap[currentHeader[i]] = val
					if p := strings.Split(currentHeader[i], "."); len(p) > 1 { tempMap[p[1]] = val }
				}
			}

			var rowData []string
			for _, pc := range parsedCols {
				if val, ok := tempMap[pc.TargetCol]; ok {
					rowData = append(rowData, val)
				} else {
					found := false
					if !strings.Contains(pc.TargetCol, ".") {
						for k, v := range tempMap {
							if strings.HasSuffix(k, "."+pc.TargetCol) {
								rowData = append(rowData, v)
								found = true
								break
							}
						}
					}
					if !found { rowData = append(rowData, "NULL") }
				}
			}
			finalResult = append(finalResult, rowData)
		}
	}

	return &ExecutionResult{
		Columns: finalHeader,
		Rows:    finalResult,
		Message: fmt.Sprintf("%d baris kapendak", len(finalResult)),
	}, nil
}

func isAggregateCheck(fields []string) bool {
	for _, f := range fields {
		if strings.Contains(f, "(") && strings.Contains(f, ")") { return true }
	}
	return false
}
func cleanHeaders(headers []string) []string {
	seen := map[string]bool{}
	out := []string{}

	for _, h := range headers {
		parts := strings.Split(h, ".")
		col := parts[len(parts)-1]  
		if seen[col] {
			out = append(out, h)
		} else {
			out = append(out, col)
			seen[col] = true
		}
	}
	return out
}

func evaluateJoinCondition(rowA, rowB []string, headA, headB []string, tblA, tblB string, cond parser.Condition) bool {
	valA := ""
	fieldA := cond.Field
	idxA := indexOf(fieldA, headA)
	if idxA == -1 {
		idxA = indexOf(tblA+"."+fieldA, headA)
	}
	if idxA != -1 { valA = rowA[idxA] }
	valB := ""
	fieldB := cond.Value
	idxB := indexOf(fieldB, headB)
	if idxB == -1 {
		idxB = indexOf(tblB+"."+fieldB, headB)
	}
	
	if idxB != -1 { 
		valB = rowB[idxB] 
	} else {
		valB = cond.Value
	}

	return valA == valB
}

func evaluateConditions(cols []string, schemaCols []schema.Column, conditions []parser.Condition) bool {
	if len(conditions) == 0 { return true }

	result := evaluateOne(cols, schemaCols, conditions[0])
	
	for i := 0; i < len(conditions)-1; i++ {
		cond := conditions[i]
		if cond.LogicOp == "" { break }
		
		nextResult := evaluateOne(cols, schemaCols, conditions[i+1])
		op := strings.ToUpper(cond.LogicOp)
		
		if op == "SARENG" || op == "AND" {
			result = result && nextResult
		} else if op == "ATAWA" || op == "OR" {
			result = result || nextResult
		}
	}
	return result
}

func evaluateMapCondition(rowMap map[string]string, conditions []parser.Condition) bool {
	if len(conditions) == 0 { return true }

	check := func(c parser.Condition) bool {
		valData, ok := rowMap[c.Field]
		if !ok { return false } 
		return match(valData, c.Operator, c.Value, "STRING") 
	}

	result := check(conditions[0])
	for i := 0; i < len(conditions)-1; i++ {
		cond := conditions[i]
		if cond.LogicOp == "" { break }
		nextRes := check(conditions[i+1])
		op := strings.ToUpper(cond.LogicOp)
		if op == "SARENG" || op == "AND" {
			result = result && nextRes
		} else if op == "ATAWA" || op == "OR" {
			result = result || nextRes
		}
	}
	return result
}

func execUpdate(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()
	s, err := schema.Load(user.Database, cmd.Table)
	if err != nil {
		return nil, err
	}
	if !s.Can(user.Role, "write") {
		return nil, errors.New("teu boga hak nulis (omean)")
	}

	rawRows, err := storage.ReadAll(cmd.Table)
	if err != nil {
		return nil, err
	}

	activeTxID := auth.GetSessionTxID()
	var tm *transaction.TxManager
	if activeTxID != "" {
		tm = transaction.GetManager()
	}

	var newRows []string 
	updatedCount := 0

	for _, raw := range rawRows {
		if raw == "" { continue }
		cols := strings.Split(raw, "|")

		shouldUpdate := true
		if len(cmd.Where) > 0 {
			shouldUpdate = evaluateOne(cols, s.Columns, cmd.Where[0])
			for i := 0; i < len(cmd.Where)-1; i++ {
				cond := cmd.Where[i]
				if cond.LogicOp == "" { break }
				nextResult := evaluateOne(cols, s.Columns, cmd.Where[i+1])
				op := strings.ToUpper(cond.LogicOp)
				if op == "SARENG" || op == "AND" {
					shouldUpdate = shouldUpdate && nextResult
				} else if op == "ATAWA" || op == "OR" {
					shouldUpdate = shouldUpdate || nextResult
				}
			}
		}

		if shouldUpdate {
			newCols := make([]string, len(cols))
			copy(newCols, cols)

			for colName, newVal := range cmd.Updates {
				idx := indexOf(colName, s.GetFieldNames())
				if idx != -1 {
					newCols[idx] = newVal
				}
			}
			
			newData := strings.Join(newCols, "|")

			if activeTxID != "" {
				err := tm.AddChange(activeTxID, transaction.OpUpdate, cmd.Table, newData, raw)
				if err != nil {
					return nil, fmt.Errorf("gagal nambah ke transaksi: %v", err)
				}
			} else {
	
				cols = newCols 
			}
			updatedCount++
		}

		if activeTxID == "" {
			newRows = append(newRows, strings.Join(cols, "|"))
		}
	}

	if activeTxID != "" {
		return &ExecutionResult{
			Message: fmt.Sprintf("✅ %d data diomean (nunggu JADIKEUN/COMMIT)", updatedCount),
		}, nil
	}

	if err := storage.Rewrite(cmd.Table, newRows); err != nil {
		return nil, err
	}

	return &ExecutionResult{
		Message: fmt.Sprintf("✅ %d data geus diomean", updatedCount),
	}, nil
}

func execDelete(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()
	s, err := schema.Load(user.Database, cmd.Table)
	if err != nil {
		return nil, err
	}
	if !s.Can(user.Role, "write") {
		return nil, errors.New("teu boga hak nulis (miceun)")
	}

	rawRows, err := storage.ReadAll(cmd.Table)
	if err != nil {
		return nil, err
	}

	var newRows []string
	deletedCount := 0

	for _, raw := range rawRows {
		if raw == "" { continue }
		cols := strings.Split(raw, "|")

		shouldDelete := true
		if len(cmd.Where) > 0 {
			shouldDelete = evaluateOne(cols, s.Columns, cmd.Where[0])
			for i := 0; i < len(cmd.Where)-1; i++ {
				cond := cmd.Where[i]
				if cond.LogicOp == "" { break }
				nextResult := evaluateOne(cols, s.Columns, cmd.Where[i+1])
				op := strings.ToUpper(cond.LogicOp)
				if op == "SARENG" || op == "AND" {
					shouldDelete = shouldDelete && nextResult
				} else if op == "ATAWA" || op == "OR" {
					shouldDelete = shouldDelete || nextResult
				}
			}
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

func indexOf(field string, fields []string) int {
	for i, f := range fields {
		if f == field {
			return i
		}
	}
	return -1
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
		case "=": return numA == numB
		case "!=": return numA != numB
		case ">": return numA > numB
		case "<": return numA < numB
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
		case "=": return fA == fB
		case "!=": return fA != fB
		case ">": return fA > fB
		case "<": return fA < fB
		case ">=": return fA >= fB
		case "<=": return fA <= fB
		}

	case "BOOL":
		if op == "=" { return a == b }
		if op == "!=" { return a != b }
		return false

	case "STRING", "TEXT", "CHAR", "ENUM", "DATE":
		switch op {
		case "=": return a == b
		case "!=": return a != b
		case ">": return a > b
		case "<": return a < b
		case ">=": return a >= b
		case "<=": return a <= b
		}

	default:
		return false
	}

	return false
}