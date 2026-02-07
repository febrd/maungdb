package executor

import (
	"os"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/febrd/maungdb/internal/config"

	"github.com/febrd/maungdb/engine/auth"
	"github.com/febrd/maungdb/engine/parser"
	"github.com/febrd/maungdb/engine/schema"
	"github.com/febrd/maungdb/engine/storage"
	"github.com/febrd/maungdb/engine/transaction"
	"github.com/febrd/maungdb/engine/indexing"
	"github.com/febrd/maungdb/engine/view"
	"github.com/febrd/maungdb/engine/trigger"

	



)

type ExecutionResult struct {
	Columns   []string   `json:"columns"`
	Rows      [][]string `json:"rows"`
	Message   string     `json:"message"`
	TimeTaken string     `json:"time_taken"` 
}

func Execute(cmd *parser.Command) (*ExecutionResult, error) {
    start := time.Now() 
    res, err := executeInternal(cmd)

    elapsed := time.Since(start)

    if res != nil {
        res.TimeTaken = fmt.Sprintf("%v", elapsed)
        
        if res.Message != "" {
            res.Message += fmt.Sprintf(" (%v ms)", float64(elapsed.Microseconds())/1000.0)
        }
    }
    return res, err
}

func executeInternal(cmd *parser.Command) (*ExecutionResult, error) {

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
    case parser.CmdShowDB:
        return execShowDB()
    case parser.CmdCreateView:
        return execCreateView(cmd)
	case parser.CmdCreateTrigger:
		return execCreateTrigger(cmd)
    case parser.CmdIndex:
        return execIndex(cmd)
    }

    return nil, fmt.Errorf("paréntah teu dikenal: %s", cmd.Type)
}

func runTriggers(dbName, table, event string) {
    triggers, err := trigger.GlobalTriggerManager.GetTriggers(dbName, table, event)
    if err != nil || len(triggers) == 0 {
        return
    }

    fmt.Printf("⚡ [JARAMBAH] Ngajalankeun %d trigger keur %s di %s...\n", len(triggers), event, table)

    for _, t := range triggers {
        cmd, err := parser.Parse(t.ActionQL)
        if err != nil {
            fmt.Printf("❌ Trigger '%s' gagal parse: %v\n", t.Name, err)
            continue
        }

        _, err = Execute(cmd) 
        if err != nil {
            fmt.Printf("❌ Trigger '%s' gagal eksekusi: %v\n", t.Name, err)
        } else {
             fmt.Printf("✅ Trigger '%s' suksés!\n", t.Name)
        }
    }
}


func execCreateTrigger(cmd *parser.Command) (*ExecutionResult, error) {
    user, _ := auth.CurrentUser()
    
    def := cmd.TriggerDef

	evt := strings.ToUpper(def.Event)
    if evt == "SIMPEN" { evt = "INSERT" }
    if evt == "OMEAN" { evt = "UPDATE" }
    if evt == "MICEUN" { evt = "DELETE" }

    t := trigger.TriggerAction{
        Name:     def.Name,
        Event:    evt,
        Table:    def.Table,
        ActionQL: def.ActionQL,
        CreatedAt: time.Now().Format(time.RFC3339),
    }

	err := trigger.GlobalTriggerManager.SaveTrigger(user.Database, t)
    if err != nil {
        return nil, fmt.Errorf("gagal nyimpen jarambah: %v", err)
    }

    return &ExecutionResult{
        Message: fmt.Sprintf("✅ Jarambah '%s' parantos dijieun keur tabel '%s'", def.Name, def.Table),
    }, nil
}

func execShowDB() (*ExecutionResult, error) {
    user, err := auth.CurrentUser()
    if err != nil {
        return nil, fmt.Errorf("gagal maca user: %v", err)
    }
    
    files, err := os.ReadDir(config.DataDir)
    if err != nil {
        return nil, fmt.Errorf("gagal maca data directory: %v", err)
    }

    var rows [][]string

    for _, f := range files {
        if f.IsDir() && strings.HasPrefix(f.Name(), "db_") {
            dbName := strings.TrimPrefix(f.Name(), "db_")
            permission := ""

            if user.Role == "supermaung" {
                permission = "FULL (Supermaung)"
            
            } else if user.Role == "admin" {
                if isDBAllowed(user, dbName) {
                    permission = "READ/WRITE (Admin)"
                }
            
            } else {
                if isDBAllowed(user, dbName) {
                    permission = "READ ONLY"
                }
            }

            if permission != "" {
                rows = append(rows, []string{dbName, permission})
            }
        }
    }

    return &ExecutionResult{
        Columns: []string{"Database", "Status Akses"},
        Rows:    rows,
        Message: fmt.Sprintf("%d PANGKAL (database) kapendak", len(rows)),
    }, nil
}

func isDBAllowed(user *auth.User, dbName string) bool {
    for _, db := range user.Databases {
        if db == "*" || db == dbName {
            return true
        }
    }
    return false
}

func execIndex(cmd *parser.Command) (*ExecutionResult, error) {
    user, _ := auth.CurrentUser()
    
    s, err := schema.Load(user.Database, cmd.Table)
    if err != nil {
        return nil, fmt.Errorf("tabel teu kapanggih: %v", err)
    }

    colName := cmd.Fields[0]
    
    err = indexing.GlobalIndexManager.BuildIndex(cmd.Table, colName, s.GetFieldNames())
    if err != nil {
        return nil, fmt.Errorf("gagal nyieun index: %v", err)
    }

    return &ExecutionResult{
        Message: fmt.Sprintf("✅ Index '%s' dina tabel '%s' parantos didamel (B-Tree Optimized)", colName, cmd.Table),
    }, nil
}

func execCreateView(cmd *parser.Command) (*ExecutionResult, error) {
	user, _ := auth.CurrentUser()
	if user.Role != "admin" && user.Role != "supermaung" {
		return nil, errors.New("hanya admin nu tiasa damel KACA")
	}

	_, err := parser.Parse(cmd.ViewQuery)
	if err != nil {
		return nil, fmt.Errorf("query view teu valid: %v", err)
	}

	err = view.SaveView(user.Database, cmd.Table, cmd.ViewQuery)
	if err != nil {
		return nil, err
	}

	return &ExecutionResult{
		Message: fmt.Sprintf("✅ Kaca (View) '%s' parantos didamel.", cmd.Table),
	}, nil
}

func execTransaction(cmd *parser.Command) (*ExecutionResult, error) {
    user, err := auth.CurrentUser()
    if err != nil {
        return nil, fmt.Errorf("kedah login heula kanggo transaksi: %v", err)
    }

    tm := transaction.GetManager()
    switch strings.ToUpper(cmd.Arg1) {
	case "MIMITIAN", "BEGIN":
        if user == nil { return nil, errors.New("kedah login heula") }
        txID, err := tm.Begin(user.Username)
        if err != nil { return nil, err }
        return &ExecutionResult{Message: fmt.Sprintf("🏁 Transaksi dimimitian (ID: %s)", txID)}, nil

    case "JADIKEUN", "COMMIT":
        if user == nil { return nil, errors.New("kedah login heula") }
        err := tm.Commit(user.Username)
        if err != nil { return nil, err }
        return &ExecutionResult{Message: "✅ Transaksi SUKSES disimpen (Committed)"}, nil

    case "BATALKEUN", "ROLLBACK":
        if user == nil { return nil, errors.New("kedah login heula") }
        err := tm.Rollback(user.Username)
        if err != nil { return nil, err }
        return &ExecutionResult{Message: "✅ Transaksi dibatalkeun (Rolled Back)"}, nil

	
	default:
        return nil, errors.New("parentah transaksi teu dikenal (pastikeun MIMITIAN, JADIKEUN, atanapi BATALKEUN)")
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
    user, err := auth.CurrentUser()
    if err != nil {
        return nil, fmt.Errorf("gagal maca user: %v", err)
    }

    s, err := schema.Load(user.Database, cmd.Table)
    if err != nil { return nil, err }
    if !s.Can(user.Role, "write") { return nil, errors.New("akses ditolak: anjeun teu boga hak nulis ka tabel ieu") }

    if err := s.ValidateRow(cmd.Data); err != nil { return nil, err }
    if err := ValidateConstraints(s, cmd.Table, cmd.Data); err != nil {
        return nil, fmt.Errorf("gagal validasi data: %v", err)
    }

    tm := transaction.GetManager()
    if tm.IsActive(user.Username) {
        err := tm.AddOperation(user.Username, transaction.OpInsert, cmd.Table, cmd.Data, "")
        if err != nil {
            return nil, fmt.Errorf("gagal nambah ke transaksi: %v", err)
        }

        return &ExecutionResult{
            Message: "✅ Data disimpen samentawis (nunggu JADIKEUN)",
        }, nil
    }

    if err := storage.Append(cmd.Table, cmd.Data); err != nil { 
        return nil, fmt.Errorf("gagal nulis ka disk: %v", err) 
    }

    go func() {
        indexing.GlobalIndexManager.UpdateIndexOnInsert(cmd.Table, cmd.Data, s.GetFieldNames())
    }()
    
	go runTriggers(user.Database, cmd.Table, "INSERT")

    return &ExecutionResult{
        Message: fmt.Sprintf("✅ Data asup ka table '%s'", cmd.Table),
    }, nil
}

func execSelect(cmd *parser.Command) (*ExecutionResult, error) {
    user, _ := auth.CurrentUser()
    var mainRaw []string
    var sMain *schema.Definition
    
    isView := view.IsView(user.Database, cmd.Table)

    if isView {

        viewQueryStr, err := view.LoadView(user.Database, cmd.Table)
        if err != nil { return nil, fmt.Errorf("gagal maca kaca '%s': %v", cmd.Table, err) }
        viewCmd, err := parser.Parse(viewQueryStr)
        if err != nil { return nil, fmt.Errorf("definisi kaca ruksak: %v", err) }
        viewRes, err := execSelect(viewCmd)
        if err != nil { return nil, fmt.Errorf("error nalika muka kaca: %v", err) }

        for _, row := range viewRes.Rows {
            mainRaw = append(mainRaw, strings.Join(row, "|"))
        }

        virtualCols := []schema.Column{}
        for _, colName := range viewRes.Columns {
            cleanName := colName
            if parts := strings.Split(colName, "."); len(parts) > 1 {
                cleanName = parts[1]
            }
            virtualCols = append(virtualCols, schema.Column{Name: cleanName, Type: "STRING"})
        }
        sMain = &schema.Definition{Columns: virtualCols}

    } else {

        s, err := schema.Load(user.Database, cmd.Table)
        if err != nil {
            return nil, fmt.Errorf("tabel '%s' teu kapanggih: %v", cmd.Table, err)
        }
        if !s.Can(user.Role, "read") {
            return nil, errors.New("akses ditolak: anjeun teu boga hak maca tabel ieu")
        }
        sMain = s

        mainRaw, err = storage.ReadAll(cmd.Table)
        if err != nil { return nil, err }
    }

    var indexedPKs map[string]bool = nil 
    
    if !isView && len(cmd.Where) == 1 && cmd.Where[0].Operator == "=" {
        cond := cmd.Where[0]
        pks, err := indexing.GlobalIndexManager.Lookup(cmd.Table, cond.Field, cond.Value)
        if err == nil {
            indexedPKs = make(map[string]bool)
            for _, pk := range pks { indexedPKs[pk] = true }
             fmt.Printf("⚡ [OPTIMIZER] Index Scan on table '%s'\n", cmd.Table)
        }
    }

    var currentHeader []string
    mainCols := sMain.GetFieldNames()
    for _, col := range mainCols {
        currentHeader = append(currentHeader, cmd.Table+"."+col)
    }

    var currentRows [][]string
    for _, row := range mainRaw {
        if strings.TrimSpace(row) == "" { continue }
        parts := strings.Split(row, "|")
        
        if indexedPKs != nil {
            if len(parts) > 0 {
                if !indexedPKs[parts[0]] { continue }
            }
        }
        currentRows = append(currentRows, parts)
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
            filteredMaps = append(filteredMaps, rowMap)
        }
    }

    selectedFields := cmd.Fields
    if len(selectedFields) == 0 || selectedFields[0] == "*" {
        selectedFields = currentHeader
    }

    var parsedCols []ParsedColumn
    isAggregateQuery := false
    for _, f := range selectedFields {
        pc := ParseColumnSelection(f)
        parsedCols = append(parsedCols, pc)
        if pc.IsAggregate { isAggregateQuery = true }
    }

    var finalResult [][]string
    var finalHeader []string

    for _, pc := range parsedCols {
        displayText := pc.TargetCol
        if pc.IsAggregate {
            displayText = pc.OriginalText 
        } else if parts := strings.Split(displayText, "."); len(parts) > 1 {
            displayText = parts[1] 
        }
        finalHeader = append(finalHeader, displayText)
    }

    if cmd.GroupBy != "" {
        groups := make(map[string][]map[string]string)
        for _, row := range filteredMaps {
            groupVal, ok := row[cmd.GroupBy]
            if !ok { 
                 groupVal = row[cmd.Table+"."+cmd.GroupBy]
            }
            if groupVal == "" { groupVal = "NULL" }
            
            groups[groupVal] = append(groups[groupVal], row)
        }

        for _, groupRows := range groups {
            var resultRow []string
            
            calculatedValues := make(map[string]string)
            if len(groupRows) > 0 {
                for k, v := range groupRows[0] { calculatedValues[k] = v }
            }

            for _, pc := range parsedCols {
                val := ""
                if pc.IsAggregate {
                    val, _ = CalculateAggregate(groupRows, pc)
                    calculatedValues[pc.OriginalText] = val
                } else {
                    if len(groupRows) > 0 {
                        v, ok := groupRows[0][pc.TargetCol]
                        if !ok { v = groupRows[0][cmd.Table+"."+pc.TargetCol] }
                        val = v
                    }
                }
                resultRow = append(resultRow, val)
            }

            matchesHaving := true
            if len(cmd.Having) > 0 {
                matchesHaving = evaluateMapCondition(calculatedValues, cmd.Having)
            }

            if matchesHaving {
                finalResult = append(finalResult, resultRow)
            }
        }

    } else if isAggregateQuery {
        var resultRow []string       
        for _, pc := range parsedCols {
            val, _ := CalculateAggregate(filteredMaps, pc)
            resultRow = append(resultRow, val)
        }
        finalResult = append(finalResult, resultRow)

    } else {
        for _, rowMap := range filteredMaps {
            var rowData []string
            for _, pc := range parsedCols {
                if val, ok := rowMap[pc.TargetCol]; ok {
                    rowData = append(rowData, val)
                } else {
                    found := false
                    for k, v := range rowMap {
                        if strings.HasSuffix(k, "."+pc.TargetCol) {
                            rowData = append(rowData, v)
                            found = true; break
                        }
                    }
                    if !found { rowData = append(rowData, "NULL") }
                }
            }
            finalResult = append(finalResult, rowData)
        }
    }

    if cmd.OrderBy != "" && len(finalResult) > 0 {
        colIdx := indexOf(cmd.OrderBy, finalHeader)
        if colIdx == -1 {
            for i, h := range finalHeader {
                if h == cmd.OrderBy { colIdx = i; break }
            }
        }

        if colIdx != -1 {
            sort.Slice(finalResult, func(i, j int) bool {
                valA := finalResult[i][colIdx]
                valB := finalResult[j][colIdx]
                fA, errA := strconv.ParseFloat(valA, 64)
                fB, errB := strconv.ParseFloat(valB, 64)

                isLess := false
                if errA == nil && errB == nil { isLess = fA < fB } else { isLess = valA < valB }
                
                if cmd.OrderDesc { return !isLess }
                return isLess
            })
        }
    }

    totalRows := len(finalResult)
    start, end := 0, totalRows
    if cmd.Offset > 0 { start = cmd.Offset; if start > totalRows { start = totalRows } }
    if cmd.Limit > 0 { end = start + cmd.Limit; if end > totalRows { end = totalRows } }
    
    return &ExecutionResult{
        Columns: finalHeader,
        Rows:    finalResult[start:end],
        Message: fmt.Sprintf("%d baris kapendak", len(finalResult[start:end])),
    }, nil
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
				err := tm.AddOperation(activeTxID, transaction.OpUpdate, cmd.Table, newData, raw)
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

	if updatedCount > 0 {
		go runTriggers(user.Database, cmd.Table, "UPDATE")
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
        return nil, errors.New("teu boga hak nulis (miceun) di tabel ieu")
    }

    rawRows, err := storage.ReadAll(cmd.Table)
    if err != nil {
        return nil, err
    }

    tm := transaction.GetManager()
    isActiveTx := tm.IsActive(user.Username)
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
            rowID := cols[0] 
            if isActiveTx {
                err := tm.AddOperation(user.Username, transaction.OpDelete, cmd.Table, raw, raw)
                if err != nil {
                    return nil, fmt.Errorf("gagal nambah operasi delete ke transaksi: %v", err)
                }
            } else {
                if err := storage.CommitDelete(cmd.Table, rowID); err != nil {
                    return nil, fmt.Errorf("gagal ngahapus data fisik ID %s: %v", rowID, err)
                }
                
                go func(tbl, id string) {
                    indexing.GlobalIndexManager.RemoveIndex(tbl, id) 
                }(cmd.Table, rowID)
            }
            deletedCount++
        }
    }

	if deletedCount > 0 {
		go runTriggers(user.Database, cmd.Table, "DELETE")
	}

    return &ExecutionResult{
        Message: fmt.Sprintf("✅ %d data geus dipiceun", deletedCount),
    }, nil
}

func isAggregateCheck(fields []string) bool {
    for _, f := range fields {
        if strings.Contains(f, "(") && strings.Contains(f, ")") { return true }
    }
    return false
}

func indexOf(field string, fields []string) int {
    for i, f := range fields {
        if f == field {
            return i
        }
    }
    return -1
}


func evaluateMapCondition(rowMap map[string]string, conditions []parser.Condition) bool {
    if len(conditions) == 0 { return true }

    check := func(c parser.Condition) bool {
        valData, ok := rowMap[c.Field]
        
        if !ok { 
            found := false
            suffix := "." + c.Field
            for k, v := range rowMap {
                if strings.HasSuffix(k, suffix) {
                    valData = v
                    found = true
                    break
                }
            }
            if !found { return false } 
        } 
        
        return match(valData, c.Operator, c.Value, "") 
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

    return match(valA, cond.Operator, valB, "")
}


func match(a, op, b, colType string) bool {
    op = strings.TrimSpace(op)

    if strings.ToUpper(op) == "JIGA" || strings.ToUpper(op) == "LIKE" {
        return strings.Contains(strings.ToLower(a), strings.ToLower(b))
    }

    isNumeric := false
    var fA, fB float64
    var errA, errB error

    if colType == "" || colType == "INT" || colType == "FLOAT" {
        fA, errA = strconv.ParseFloat(a, 64)
        fB, errB = strconv.ParseFloat(b, 64)
        if errA == nil && errB == nil {
            isNumeric = true
        }
    }

    if isNumeric {
        switch op {
        case "=": return fA == fB
        case "!=": return fA != fB
        case ">": return fA > fB
        case "<": return fA < fB
        case ">=": return fA >= fB
        case "<=": return fA <= fB
        }
    }


    switch op {
    case "=": return a == b
    case "!=": return a != b
    case ">": return a > b
    case "<": return a < b
    case ">=": return a >= b
    case "<=": return a <= b
    }

    return false
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