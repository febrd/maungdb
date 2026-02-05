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

// ==========================================
// CREATE & INSERT
// ==========================================

func execCreate(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()

	fields := splitColumns(cmd.Data)

	perms := map[string][]string{
		"read":  {"user", "admin", "supermaung"},
		"write": {"admin", "supermaung"},
	}

	// 1️⃣ Create schema
	if err := schema.Create(user.Database, cmd.Table, fields, perms); err != nil {
		return nil, err
	}

	// 2️⃣ Create empty .mg file (NO DATA)
	if err := storage.InitTableFile(user.Database, cmd.Table); err != nil {
		return nil, err
	}

	return &ExecutionResult{
		Message: fmt.Sprintf("✅ Tabel '%s' didamel (schema + data siap)", cmd.Table),
	}, nil
}


// Helper untuk memisahkan kolom CREATE TABLE, menangani koma dalam kurung ENUM(A,B)
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

// ==========================================
// UPDATE: execSelect (Support JOIN, FILTER, SORT, AGGREGATE)
// ==========================================
func execSelect(cmd *parser.Command) (*ExecutionResult, error) {

	// 1. SETUP & LOAD TABEL UTAMA
	// ---------------------------
	user, _ := auth.CurrentUser()

	// Ambil kolom dari schema (SUMBER HEADER YANG BENAR)
	columns, err := schema.GetColumns(user.Database, cmd.Table)
	if err != nil {
		return nil, err
	}

	// Cek Schema & Izin
	sMain, err := schema.Load(user.Database, cmd.Table)
	if err != nil {
		return nil, fmt.Errorf("gagal maca schema tabel '%s': %v", cmd.Table, err)
	}
	if !sMain.Can(user.Role, "read") {
		return nil, errors.New("akses ditolak: anjeun teu boga hak maca tabel ieu")
	}

	// Baca semua data (TANPA HEADER)
	mainRaw, err := storage.ReadAll(cmd.Table)
	if err != nil {
		return nil, err
	}
	if len(mainRaw) == 0 {
		return &ExecutionResult{Message: "Data kosong"}, nil
	}

	// === HEADER DARI SCHEMA (BUKAN .mg)
	var currentHeader []string
	for _, col := range columns {
		currentHeader = append(currentHeader, cmd.Table+"."+col)
	}

	// === DATA: SEMUA BARIS .mg
	var currentRows [][]string
	for _, row := range mainRaw {
		if row == "" {
			continue
		}
		currentRows = append(currentRows, strings.Split(row, "|"))
	}

	// 2. PROSES JOIN
	// ---------------------------
	for _, join := range cmd.Joins {

		targetRaw, err := storage.ReadAll(join.Table)
		if err != nil {
			return nil, fmt.Errorf("tabel join '%s' teu kapanggih", join.Table)
		}
		if len(targetRaw) == 0 {
			continue
		}

		// HEADER JOIN DARI SCHEMA
		targetCols, err := schema.GetColumns(user.Database, join.Table)
		if err != nil {
			return nil, err
		}

		var targetHeaderFull []string
		for _, h := range targetCols {
			targetHeaderFull = append(targetHeaderFull, join.Table+"."+h)
		}

		// DATA JOIN
		targetRows := [][]string{}
		for _, r := range targetRaw {
			if r != "" {
				targetRows = append(targetRows, strings.Split(r, "|"))
			}
		}

		var nextRows [][]string
		matchedRightIndices := make(map[int]bool)

		for _, leftRow := range currentRows {
			matchedLeft := false

			for tIdx, rightRow := range targetRows {
				isMatch := evaluateJoinCondition(
					leftRow,
					rightRow,
					currentHeader,
					targetHeaderFull,
					cmd.Table,
					join.Table,
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

			// LEFT JOIN
			if !matchedLeft && (join.Type == "LEFT" || join.Type == "KENCA") {
				merged := append([]string{}, leftRow...)
				for range targetHeaderFull {
					merged = append(merged, "NULL")
				}
				nextRows = append(nextRows, merged)
			}
		}

		// RIGHT JOIN
		if join.Type == "RIGHT" || join.Type == "KATUHU" {
			for tIdx, rightRow := range targetRows {
				if !matchedRightIndices[tIdx] {
					merged := []string{}
					for range currentHeader {
						merged = append(merged, "NULL")
					}
					merged = append(merged, rightRow...)
					nextRows = append(nextRows, merged)
				}
			}
		}

		currentRows = nextRows
		currentHeader = append(currentHeader, targetHeaderFull...)
	}

	// 3. FILTERING & MAPPING (DIMANA)
	// ---------------------------
	var filteredMaps []map[string]string
	var filteredSlices [][]string

	for _, cols := range currentRows {
		rowMap := make(map[string]string)
		for i, val := range cols {
			if i < len(currentHeader) {
				fullKey := currentHeader[i]
				rowMap[fullKey] = val

				parts := strings.Split(fullKey, ".")
				if len(parts) > 1 {
					rowMap[parts[1]] = val
				}
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

	// 4. PROSES AGREGASI ATAU QUERY BIASA
	// ---------------------------
	selectedFields := cmd.Fields
	if len(selectedFields) == 0 || selectedFields[0] == "*" {
		selectedFields = currentHeader
	}

	isAggregateQuery := false
	var parsedCols []ParsedColumn
	for _, f := range selectedFields {
		pc := ParseColumnSelection(f)
		parsedCols = append(parsedCols, pc)
		if pc.IsAggregate {
			isAggregateQuery = true
		}
	}

	var finalResult [][]string
	var finalHeader []string

	// === CABANG A: QUERY AGREGASI ===
	if isAggregateQuery {

		var resultRow []string
		for _, pc := range parsedCols {
			finalHeader = append(finalHeader, pc.OriginalText)
			val, _ := CalculateAggregate(filteredMaps, pc)
			resultRow = append(resultRow, val)
		}
		finalResult = append(finalResult, resultRow)

	} else {
		// === CABANG B: QUERY BIASA (SORT, LIMIT, OFFSET) ===

		// B1. Sorting
		if cmd.OrderBy != "" {
			colIdx := indexOf(cmd.OrderBy, currentHeader)
			if colIdx == -1 {
				for i, h := range currentHeader {
					if parts := strings.Split(h, "."); len(parts) > 1 && parts[1] == cmd.OrderBy {
						colIdx = i
						break
					}
				}
			}

			if colIdx != -1 {
				sort.Slice(filteredSlices, func(i, j int) bool {
					a, b := filteredSlices[i][colIdx], filteredSlices[j][colIdx]
					fa, ea := strconv.ParseFloat(a, 64)
					fb, eb := strconv.ParseFloat(b, 64)

					less := false
					if ea == nil && eb == nil {
						less = fa < fb
					} else {
						less = a < b
					}
					if cmd.OrderDesc {
						return !less
					}
					return less
				})
			}
		}

		// B2. Pagination
		total := len(filteredSlices)
		start, end := 0, total
		if cmd.Offset > 0 && cmd.Offset < total {
			start = cmd.Offset
		}
		if cmd.Limit > 0 && start+cmd.Limit < total {
			end = start + cmd.Limit
		}

		slicedRows := filteredSlices[start:end]

		// B3. Proyeksi
		for _, pc := range parsedCols {
			finalHeader = append(finalHeader, pc.TargetCol)
		}

		for _, rowSlice := range slicedRows {
			tempMap := make(map[string]string)
			for i, val := range rowSlice {
				if i < len(currentHeader) {
					tempMap[currentHeader[i]] = val
					if p := strings.Split(currentHeader[i], "."); len(p) > 1 {
						tempMap[p[1]] = val
					}
				}
			}

			var rowData []string
			for _, pc := range parsedCols {
				if val, ok := tempMap[pc.TargetCol]; ok {
					rowData = append(rowData, val)
				} else {
					rowData = append(rowData, "NULL")
				}
			}
			finalResult = append(finalResult, rowData)
		}
	}

	return &ExecutionResult{
		Columns: cleanHeaders(finalHeader),
		Rows:    finalResult,
		Message: fmt.Sprintf("%d baris kapanggih", len(finalResult)),
	}, nil
}

func cleanHeaders(headers []string) []string {
	seen := map[string]bool{}
	out := []string{}

	for _, h := range headers {
		parts := strings.Split(h, ".")
		col := parts[len(parts)-1] // ambil nama kolom saja

		// Jika duplikat (JOIN tabrakan), fallback ke full name
		if seen[col] {
			out = append(out, h)
		} else {
			out = append(out, col)
			seen[col] = true
		}
	}
	return out
}

// ==========================================
// HELPERS (Taruh di bawah file executor.go)
// ==========================================

// Helper: Cek kondisi ON saat Join
func evaluateJoinCondition(rowA, rowB []string, headA, headB []string, tblA, tblB string, cond parser.Condition) bool {
	// Ambil nilai Kiri
	valA := ""
	fieldA := cond.Field
	// Coba cari full match (tabel.kolom)
	idxA := indexOf(fieldA, headA)
	// Kalau gak ketemu, coba cari short match (kolom) di tabel A
	if idxA == -1 {
		idxA = indexOf(tblA+"."+fieldA, headA)
	}
	if idxA != -1 { valA = rowA[idxA] }

	// Ambil nilai Kanan (cond.Value biasanya nama kolom di tabel B)
	valB := ""
	fieldB := cond.Value
	idxB := indexOf(fieldB, headB)
	if idxB == -1 {
		idxB = indexOf(tblB+"."+fieldB, headB)
	}
	
	if idxB != -1 { 
		valB = rowB[idxB] 
	} else {
		// Jika tidak ketemu di header B, anggap string literal (misal: ON a.id = "1")
		valB = cond.Value
	}

	return valA == valB
}

// Helper: Evaluasi Filter WHERE pada Map
func evaluateMapCondition(rowMap map[string]string, conditions []parser.Condition) bool {
	if len(conditions) == 0 { return true }

	check := func(c parser.Condition) bool {
		valData, ok := rowMap[c.Field]
		if !ok { return false } // Kolom tidak ditemukan
		return match(valData, c.Operator, c.Value, "STRING") // Auto detect string/number inside match
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

// ==========================================
// UPDATE (OMEAN) - Fixed Logic Filter
// ==========================================
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

	var newRows []string
	updatedCount := 0

	for _, raw := range rawRows {
		if raw == "" { continue }
		cols := strings.Split(raw, "|")

		// --- LOGIKA FILTER (Sama dengan execSelect) ---
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

// ==========================================
// DELETE (MICEUN) - Fixed Logic Filter
// ==========================================
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

		// --- LOGIKA FILTER (Sama dengan execSelect) ---
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

// ==========================================
// HELPERS
// ==========================================

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