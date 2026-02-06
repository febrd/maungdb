package parser

import (
	"errors"
	"strconv"
	"strings"
)


func Parse(query string) (*Command, error) {
    query = strings.TrimSpace(query)
    query = strings.TrimSuffix(query, ";")

      query = normalizeQuery(query)

    tokens := strings.Fields(query)
    if len(tokens) == 0 {
        return nil, errors.New("query kosong")
    }

    verb := strings.ToUpper(tokens[0])

    switch verb {
    case "MIMITIAN", "BEGIN", "JADIKEUN", "COMMIT", "BATALKEUN", "ROLLBACK":
        return &Command{
            Type: CmdTransaction,
            Arg1: verb,
        }, nil

    case "DAMEL", "BIKIN", "NYIEUN", "SCHEMA", "LAHAN":
        if len(tokens) > 1 && strings.ToUpper(tokens[1]) == "CREATE" {
            return parseCreate(tokens[2:])
        }
        return parseCreate(tokens[1:])
        
    case "SIMPEN", "TENDEUN", "INSERT":
        return parseInsert(tokens)
        
    case "TINGALI", "TENJO", "SELECT":
        return parseSelect(tokens)
        
    case "OMEAN", "ROBIH", "UPDATE":
        return parseUpdate(tokens)
        
    case "MICEUN", "PICEUN", "DELETE":
        return parseDelete(tokens)
        
    default:
        return nil, errors.New("paréntah teu dikenal: " + verb)
    }
}

func normalizeQuery(query string) string {

    query = strings.ReplaceAll(query, ">=", " __GTE__ ")
    query = strings.ReplaceAll(query, "<=", " __LTE__ ")
    query = strings.ReplaceAll(query, "!=", " __NEQ__ ")

    query = strings.ReplaceAll(query, "=", " = ")
    query = strings.ReplaceAll(query, ">", " > ")
    query = strings.ReplaceAll(query, "<", " < ")
    
    query = strings.ReplaceAll(query, "__GTE__", ">=")
    query = strings.ReplaceAll(query, "__LTE__", "<=")
    query = strings.ReplaceAll(query, "__NEQ__", "!=")

    return query
}
func parseCreate(tokens []string) (*Command, error) {
	if len(tokens) < 2 {
		return nil, errors.New("format: DAMEL <tabel> <definisi_kolom>")
	}

	table := strings.TrimSpace(tokens[0])

	raw := strings.Join(tokens[1:], " ")
	raw = strings.ReplaceAll(raw, " ,", ",")
	raw = strings.ReplaceAll(raw, ", ", ",")
	raw = strings.TrimSpace(raw)

	if raw == "" {
		return nil, errors.New("definisi kolom teu meunang kosong")
	}

	return &Command{
		Type:  CmdCreate,
		Table: table,
		Data:  raw,
	}, nil
}

func parseInsert(tokens []string) (*Command, error) {
	if len(tokens) < 3 {
		return nil, errors.New("format simpen salah: SIMPEN <table> <data>")
	}

	dataPart := strings.Join(tokens[2:], " ")

	return &Command{
		Type:  CmdInsert,
		Table: tokens[1],
		Data:  dataPart,
	}, nil
}

func parseSelect(tokens []string) (*Command, error) {
	cmd := &Command{
		Type:  CmdSelect,
		Limit: -1, 
		Joins: []JoinClause{},
	}

	tiIndex := -1
	for i, t := range tokens {
		if strings.ToUpper(t) == "TI" || strings.ToUpper(t) == "FROM" {
			tiIndex = i
			break
		}
	}

	idx := 0 
	if tiIndex != -1 {
		if tiIndex < 1 {
			return nil, errors.New("kolom teu disebutkeun samemeh TI")
		}
		
		colsPart := strings.Join(tokens[1:tiIndex], " ")
		rawFields := strings.Split(colsPart, ",")
		for _, f := range rawFields {
			cmd.Fields = append(cmd.Fields, strings.TrimSpace(f))
		}

		if tiIndex+1 >= len(tokens) {
			return nil, errors.New("tabel teu disebutkeun sanggeus TI")
		}
		cmd.Table = tokens[tiIndex+1]
		
		idx = tiIndex + 2 

	} else {
		if len(tokens) < 2 {
			return nil, errors.New("format TINGALI salah, minimal: TINGALI <tabel>")
		}
		cmd.Table = tokens[1]
		cmd.Fields = []string{"*"} 
		idx = 2
	}

	for idx < len(tokens) {
		token := strings.ToUpper(tokens[idx])

		
		if isJoinKeyword(token) {
			
			joinType := "INNER"
			
			if token == "LEFT" || token == "KENCA" {
				joinType = "LEFT"
				idx++ 
			} else if token == "RIGHT" || token == "KATUHU" {
				joinType = "RIGHT"
				idx++
			} else if token == "INNER" || token == "HIJIKEUN" {
				joinType = "INNER"
				idx++
			} else if token == "FULL" || token == "PINUH" {
				joinType = "FULL" 
				idx++
			}

			if idx >= len(tokens) {
				return nil, errors.New("paréntah JOIN teu lengkep")
			}
			
			currToken := strings.ToUpper(tokens[idx])
			if currToken == "GABUNG" || currToken == "JOIN" || currToken == "HIJIKEUN"  {
				idx++ 
			} else if token == "GABUNG" || token == "JOIN" || currToken == "HIJIKEUN" {
				idx++
			} else {
				return nil, errors.New("sanggeus tipe join kedah aya HIJIKEUN/GABUNG/JOIN")
			}

			if idx >= len(tokens) {
				return nil, errors.New("tabel join teu disebutkeun")
			}
			joinTable := tokens[idx]
			idx++

			if idx >= len(tokens) {
				return nil, errors.New("join butuh kondisi DINA / ON")
			}
			onKeyword := strings.ToUpper(tokens[idx])
			if onKeyword != "DINA" && onKeyword != "ON" {
				return nil, errors.New("saenggeus tabel join kedah nganggo DINA / ON")
			}
			idx++

			if idx+2 >= len(tokens) {
				return nil, errors.New("kondisi join teu lengkep (col1 = col2)")
			}

			joinCond := Condition{
				Field:    tokens[idx],
				Operator: tokens[idx+1],
				Value:    tokens[idx+2],
			}
			idx += 3
			cmd.Joins = append(cmd.Joins, JoinClause{
				Type:      joinType,
				Table:     joinTable,
				Condition: joinCond,
			})
			
			continue 
		}

		switch token {
		case "DIMANA", "WHERE":
			endIdx := len(tokens)
			for i := idx + 1; i < len(tokens); i++ {
				kw := strings.ToUpper(tokens[i])
				if kw == "RUNTUYKEUN" || kw == "ORDER" || kw == "SAKADAR" || kw == "LIMIT" || kw == "LIWATAN" || kw == "OFFSET" || isJoinKeyword(kw) {
					endIdx = i
					break
				}
			}
			condTokens := tokens[idx+1 : endIdx]
			conds, err := parseConditionsList(condTokens)
			if err != nil { return nil, err }
			cmd.Where = conds
			idx = endIdx

		case "RUNTUYKEUN", "ORDER":
			if idx+1 >= len(tokens) { return nil, errors.New("RUNTUYKEUN butuh ngaran kolom") }
			targetIdx := idx + 1
			if strings.ToUpper(tokens[targetIdx]) == "BY" { targetIdx++ }
			if targetIdx >= len(tokens) { return nil, errors.New("RUNTUYKEUN butuh ngaran kolom") }
			
			cmd.OrderBy = tokens[targetIdx]
			idx = targetIdx + 1
			if idx < len(tokens) {
				mode := strings.ToUpper(tokens[idx])
				if mode == "TI_LUHUR" || mode == "TURUN" || mode == "DESC" { 
					cmd.OrderDesc = true; idx++ 
				} else if mode == "TI_HANDAP" || mode == "NAEK" || mode == "ASC" {
					cmd.OrderDesc = false; idx++
				}
			}

		case "SAKADAR", "LIMIT":
			if idx+1 >= len(tokens) { return nil, errors.New("SAKADAR butuh angka") }
			limit, err := strconv.Atoi(tokens[idx+1])
			if err != nil { return nil, errors.New("SAKADAR kudu angka") }
			cmd.Limit = limit
			idx += 2

		case "LIWATAN", "OFFSET":
			if idx+1 >= len(tokens) { return nil, errors.New("LIWATAN butuh angka") }
			offset, err := strconv.Atoi(tokens[idx+1])
			if err != nil { return nil, errors.New("LIWATAN kudu angka") }
			cmd.Offset = offset
			idx += 2

		default:
			idx++ 
		}
	}

	return cmd, nil
}

func isJoinKeyword(t string) bool {
	return t == "GABUNG" || t == "JOIN" || 
	       t == "INNER" ||  t == "HIJIKEUN" || 
	       t == "LEFT" || t == "KENCA" || 
	       t == "RIGHT" || t == "KATUHU"
}

func parseUpdate(tokens []string) (*Command, error) {
	
	if len(tokens) < 4 {
		return nil, errors.New("format OMEAN salah: OMEAN <table> JADI <col>=<val> DIMANA ...")
	}

	keyword := strings.ToUpper(tokens[2])
	if keyword != "JADI" && keyword != "JANTEN" && keyword != "SET" {
		return nil, errors.New("kedah nganggo JADI / JANTEN")
	}

	cmd := &Command{
		Type:    CmdUpdate,
		Table:   tokens[1],
		Updates: make(map[string]string),
		Where:   []Condition{},
	}

	whereIdx := -1
	for i := 3; i < len(tokens); i++ {
		if strings.ToUpper(tokens[i]) == "DIMANA" || strings.ToUpper(tokens[i]) == "WHERE" {
			whereIdx = i
			break
		}
	}

	updateEnd := len(tokens)
	if whereIdx != -1 {
		updateEnd = whereIdx
	}

	updatePart := strings.Join(tokens[3:updateEnd], " ")
	pairs := strings.Split(updatePart, ",")
	
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) == 2 {
			cmd.Updates[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	if whereIdx != -1 {
		conds, err := parseConditionsList(tokens[whereIdx+1:])
		if err != nil {
			return nil, err
		}
		cmd.Where = conds
	}

	return cmd, nil
}

func parseDelete(tokens []string) (*Command, error) {
	if len(tokens) < 3 {
		return nil, errors.New("format MICEUN salah: MICEUN TI <table> DIMANA ...")
	}

	if strings.ToUpper(tokens[1]) != "TI" && strings.ToUpper(tokens[1]) != "FROM" {
		return nil, errors.New("kedah nganggo TI")
	}

	cmd := &Command{
		Type:  CmdDelete,
		Table: tokens[2],
		Where: []Condition{},
	}

	if len(tokens) > 3 {
		if strings.ToUpper(tokens[3]) == "DIMANA" || strings.ToUpper(tokens[3]) == "WHERE" {
			conds, err := parseConditionsList(tokens[4:])
			if err != nil {
				return nil, err
			}
			cmd.Where = conds
		} else {
			return nil, errors.New("kedah nganggo DIMANA")
		}
	}

	return cmd, nil
}

func parseConditionsList(tokens []string) ([]Condition, error) {
	var conditions []Condition
	i := 0
	for i < len(tokens) {
		if i+2 >= len(tokens) {
			break 
		}
		field := tokens[i]
		op := tokens[i+1]
		val := tokens[i+2]
		val = strings.Trim(val, "'\"")
		cond := Condition{
			Field:    field,
			Operator: op,
			Value:    val,
		}
		if i+3 < len(tokens) {
			logic := strings.ToUpper(tokens[i+3])
			if logic == "SARENG" || logic == "AND" || logic == "ATAWA" || logic == "OR" {
				cond.LogicOp = logic
				i++
			}
		}
		
		conditions = append(conditions, cond)
		i += 3 
	}
	return conditions, nil
}