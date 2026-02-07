package parser

import (
	"errors"
	"strconv"
	"strings"
	"regexp"
)

var (
  
    ReIndung = regexp.MustCompile(`(?i)^JADI\s+INDUNG`)
    ReAnak   = regexp.MustCompile(`(?i)^JADI\s+ANAK\s+NGINTIL\s+(.+)`)

    ReCreateFTS = regexp.MustCompile(`(?i)^DAMEL\s+INDEKS_TEKS\s+(\w+)\s+DINA\s+(\w+)`)
    ReFTS = regexp.MustCompile(`(?i)^KOREHAN\s+(\w+)\s+DINA\s+(\w+)\s+MILARI\s+"(.+)"`)
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

    case "DAMEL", "BIKIN", "NYIEUN", "SCHEMA":

		if len(tokens) > 1 && (strings.ToUpper(tokens[1]) == "KACA" || strings.ToUpper(tokens[1]) == "VIEW") {
			return parseCreateView(tokens)
		}
		if len(tokens) > 1 && (strings.ToUpper(tokens[1]) == "JARAMBAH" || strings.ToUpper(tokens[1]) == "TRIGGER") {
			return parseCreateTrigger(tokens)
		}

		if len(tokens) > 1 && strings.ToUpper(tokens[1]) == "INDEKS_TEKS" {
            if matches := ReCreateFTS.FindStringSubmatch(query); len(matches) > 2 {
                return &Command{Type: "CREATE_FTS", Table: matches[1], Column: matches[2]}, nil
            }
            return nil, errors.New("format DAMEL INDEKS_TEKS salah. Conto: DAMEL INDEKS_TEKS buku DINA judul")
        }

        if len(tokens) > 1 && strings.ToUpper(tokens[1]) == "CREATE" {
            return parseCreate(tokens[2:])
        }
        return parseCreate(tokens[1:])
        
    case "SIMPEN", "TENDEUN", "INSERT":
        return parseInsert(tokens)
        
    case "TINGALI", "TENJO", "SELECT":
		if len(tokens) > 1 {
			token2 := strings.ToUpper(tokens[1])
			if token2 == "PANGKAL" || token2 == "DATABASES" {
				return &Command{Type: CmdShowDB}, nil
			}
		}
        return parseSelect(tokens)
        
    case "OMEAN", "ROBIH", "UPDATE":
        return parseUpdate(tokens)
        
    case "MICEUN", "PICEUN", "DELETE":
        return parseDelete(tokens)
	case "TANDAIN", "TANDAAN", "TAWISAN":
		return parseIndex(tokens)

	case "KOREHAN", "JADI", "JANTEN":
		return ParseCommand(query)
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

func ParseCommand(input string) (*Command, error) {
    input = strings.TrimSpace(input)

    if ReIndung.MatchString(input) {
        return &Command{Type: "JADI_INDUNG"}, nil
    }

    if matches := ReAnak.FindStringSubmatch(input); len(matches) > 1 {
        return &Command{Type: "JADI_ANAK", Arg1: matches[1]}, nil 
    }

    if matches := ReCreateFTS.FindStringSubmatch(input); len(matches) > 2 {
        return &Command{Type: "CREATE_FTS", Table: matches[1], Column: matches[2]}, nil
    }

    if matches := ReFTS.FindStringSubmatch(input); len(matches) > 3 {
        return &Command{
            Type: "KOREHAN", 
            Table: matches[1], 
            Column: matches[2], 
            Arg1: matches[3],
        }, nil
    }

    return nil, nil
}
func parseCreateTrigger(tokens []string) (*Command, error) {
    if len(tokens) < 8 {
        return nil, errors.New("syntax salah. Gunakeun: DAMEL JARAMBAH <nama> WAKTU <event> PADA <tabel> LAKUKAN <query>")
    }

    name := tokens[2]    
    if strings.ToUpper(tokens[3]) != "WAKTU" && strings.ToUpper(tokens[3]) != "WHEN" {
        return nil, errors.New("kedah nganggo kecap WAKTU sateuacan event")
    }
    event := strings.ToUpper(tokens[4]) 
    if strings.ToUpper(tokens[5]) != "PADA" && strings.ToUpper(tokens[5]) != "ON" {
        return nil, errors.New("kedah nganggo kecap PADA sateuacan nama tabel")
    }
    table := tokens[6]

    if strings.ToUpper(tokens[7]) != "LAKUKAN" && strings.ToUpper(tokens[7]) != "DO" {
        return nil, errors.New("kedah nganggo kecap LAKUKAN sateuacan query aksi")
    }

    actionParts := tokens[8:]
    actionQL := strings.Join(actionParts, " ")

    return &Command{
        Type: CmdCreateTrigger,
        TriggerDef: TriggerDefinition{
            Name:     name,
            Event:    event,
            Table:    table,
            ActionQL: actionQL,
        },
    }, nil
}

func parseCreateView(tokens []string) (*Command, error) {
	if len(tokens) < 5 {
		return nil, errors.New("format salah: DAMEL KACA <nama> TINA <query>")
	}

	viewName := tokens[2]
	tinaIdx := -1
	if strings.ToUpper(tokens[3]) == "TINA" || strings.ToUpper(tokens[3]) == "AS" {
		tinaIdx = 3
	}

	if tinaIdx == -1 {
		return nil, errors.New("kedah nganggo kecap TINA atanapi AS")
	}

	queryParts := tokens[tinaIdx+1:]
	viewQuery := strings.Join(queryParts, " ")

	return &Command{
		Type:      CmdCreateView,
		Table:     viewName,
		ViewQuery: viewQuery,
	}, nil
}

func parseIndex(tokens []string) (*Command, error) {
    if len(tokens) < 4 {
        return nil, errors.New("format salah: TANDAIN <table> DINA <kolom>")
    }

    if strings.ToUpper(tokens[2]) != "DINA" && strings.ToUpper(tokens[2]) != "ON" {
        return nil, errors.New("kedah nganggo kecap DINA")
    }

    return &Command{
        Type:   CmdIndex,
        Table:  tokens[1],
        Fields: []string{tokens[3]},
    }, nil
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
		Type:   CmdSelect,
		Limit:  -1,
		Joins:  []JoinClause{},
		Where:  []Condition{},
		Having: []Condition{}, 
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
			if currToken == "GABUNG" || currToken == "JOIN" || currToken == "HIJIKEUN" {
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
			endIdx := findNextKeyword(tokens, idx+1)
			
			condTokens := tokens[idx+1 : endIdx]
			conds, err := parseConditionsList(condTokens)
			if err != nil {
				return nil, err
			}
			cmd.Where = conds
			idx = endIdx

		case "KUMPULKEUN", "GROUP":
			targetIdx := idx + 1
			if targetIdx < len(tokens) {
				next := strings.ToUpper(tokens[targetIdx])
				if next == "DUMASAR" || next == "BY" {
					targetIdx++
				}
			}

			if targetIdx >= len(tokens) {
				return nil, errors.New("KUMPULKEUN/GROUP butuh ngaran kolom")
			}

			cmd.GroupBy = tokens[targetIdx]
			idx = targetIdx + 1

		case "MUN", "HAVING":
			if idx+1 < len(tokens) && strings.ToUpper(tokens[idx+1]) == "SYARATNA" {
				idx++
			}

			endIdx := findNextKeyword(tokens, idx+1)
			condTokens := tokens[idx+1 : endIdx]
			conds, err := parseConditionsList(condTokens)
			if err != nil {
				return nil, err
			}
			cmd.Having = conds
			idx = endIdx

		case "RUNTUYKEUN", "ORDER":
			targetIdx := idx + 1
			if targetIdx < len(tokens) && strings.ToUpper(tokens[targetIdx]) == "BY" {
				targetIdx++
			}
			if targetIdx >= len(tokens) {
				return nil, errors.New("RUNTUYKEUN butuh ngaran kolom")
			}

			cmd.OrderBy = tokens[targetIdx]
			idx = targetIdx + 1
			
			if idx < len(tokens) {
				mode := strings.ToUpper(tokens[idx])
				if mode == "TI_LUHUR" || mode == "TURUN" || mode == "DESC" {
					cmd.OrderDesc = true
					idx++
				} else if mode == "TI_HANDAP" || mode == "NAEK" || mode == "ASC" {
					cmd.OrderDesc = false
					idx++
				}
			}

		case "SAKADAR", "LIMIT":
			if idx+1 >= len(tokens) {
				return nil, errors.New("SAKADAR butuh angka")
			}
			limit, err := strconv.Atoi(tokens[idx+1])
			if err != nil {
				return nil, errors.New("SAKADAR kudu angka")
			}
			cmd.Limit = limit
			idx += 2

		case "LIWATAN", "OFFSET":
			if idx+1 >= len(tokens) {
				return nil, errors.New("LIWATAN butuh angka")
			}
			offset, err := strconv.Atoi(tokens[idx+1])
			if err != nil {
				return nil, errors.New("LIWATAN kudu angka")
			}
			cmd.Offset = offset
			idx += 2

		default:
			idx++
		}
	}

	return cmd, nil
}

func findNextKeyword(tokens []string, start int) int {
    for i := start; i < len(tokens); i++ {
        t := strings.ToUpper(tokens[i])
        
        if t == "RUNTUYKEUN" || t == "ORDER" || 
           t == "SAKADAR" || t == "LIMIT" || 
           t == "LIWATAN" || t == "OFFSET" || 
           t == "KUMPULKEUN" || t == "GROUP" || 
           t == "MUN" || t == "HAVING" ||     
           isJoinKeyword(t) {
            return i
        }
    }
    return len(tokens)
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