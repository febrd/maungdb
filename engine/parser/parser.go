package parser

import (
	"errors"
	"strconv"
	"strings"
)

func Parse(input string) (*Command, error) {
	tokens := strings.Fields(input)
	if len(tokens) < 2 {
		return nil, errors.New("query teu valid")
	}

	switch strings.ToUpper(tokens[0]) {
	case "DAMEL":
  		return parseCreate(tokens)
	case "SIMPEN":
		return parseInsert(tokens)
	case "TINGALI":
		return parseSelect(tokens)
	case "OMEAN":
		return parseUpdate(tokens)
	case "MICEUN":
		return parseDelete(tokens)
	default:
		return nil, errors.New("paréntah teu dikenal")
	}
}

func parseCreate(tokens []string) (*Command, error) {
    if len(tokens) < 3 {
        return nil, errors.New("format: DAMEL <tabel> <definisi_kolom>")
    }
    return &Command{
        Type: CmdCreate,
        Table: tokens[1],
        Data: tokens[2], 
    }, nil
}

func parseUpdate(tokens []string) (*Command, error) {
	if len(tokens) < 4 || strings.ToUpper(tokens[2]) != "JADI" {
		return nil, errors.New("format OMEAN salah: OMEAN <table> JADI <col>=<val> DIMANA ...")
	}

	pairs := strings.Split(tokens[3], "=")
	if len(pairs) != 2 {
		return nil, errors.New("format update salah, gunakeun col=val")
	}

	cmd := &Command{
		Type:    CmdUpdate,
		Table:   tokens[1],
		Updates: map[string]string{pairs[0]: pairs[1]},
		Where:   []Condition{},
	}

	// Parse WHERE (dimana)
	if len(tokens) > 4 {
		if strings.ToUpper(tokens[4]) != "DIMANA" {
			return nil, errors.New("kedah nganggo DIMANA")
		}
		whereCmd, err := parseWhere(tokens[5:]) // Reuse logic WHERE
		if err != nil {
			return nil, err
		}
		cmd.Where = whereCmd.Where
	}

	return cmd, nil
}

// Sintaks: MICEUN TI <table_name> DIMANA ...
func parseDelete(tokens []string) (*Command, error) {
	if len(tokens) < 3 || strings.ToUpper(tokens[1]) != "TI" {
		return nil, errors.New("format MICEUN salah: MICEUN TI <table> DIMANA ...")
	}

	cmd := &Command{
		Type:  CmdDelete,
		Table: tokens[2],
		Where: []Condition{},
	}

	if len(tokens) > 3 {
		if strings.ToUpper(tokens[3]) != "DIMANA" {
			return nil, errors.New("kedah nganggo DIMANA")
		}
		whereCmd, err := parseWhere(tokens[4:])
		if err != nil {
			return nil, err
		}
		cmd.Where = whereCmd.Where
	}

	return cmd, nil
}


func parseInsert(tokens []string) (*Command, error) {
	if len(tokens) < 3 {
		return nil, errors.New("format simpen salah: simpen <table> <data>")
	}
	return &Command{
		Type:  CmdInsert,
		Table: tokens[1],
		Data:  tokens[2],
	}, nil
}


// ==========================================
// PARSE SELECT (TINGALI) - VERSI LENGKAP
// ==========================================
func parseSelect(tokens []string) (*Command, error) {
	// Minimal: TINGALI <table_name>
	if len(tokens) < 2 {
		return nil, errors.New("format TINGALI salah, minimal: TINGALI <tabel>")
	}

	cmd := &Command{
		Type:  CmdSelect,
		Table: tokens[1],
		Where: []Condition{},
		Limit: -1, // Default -1 hartina euweuh limit
	}

	// Pointer 'idx' pikeun nyusud token ti kenca ka katuhu
	idx := 2

	// 1. Parsing DIMANA (WHERE)
	// Cek naha aya token saterusna jeung naha éta "DIMANA"
	if idx < len(tokens) && strings.ToUpper(tokens[idx]) == "DIMANA" {
		// Urang kudu nyaho DIMANA ieu nepi mana. 
		// Batasna nyaeta lamun panggih keyword lain (RUNTUYKEUN/SAKADAR/LIWATAN) atawa beak token.
		endIdx := len(tokens)
		
		for i := idx + 1; i < len(tokens); i++ {
			kw := strings.ToUpper(tokens[i])
			if kw == "RUNTUYKEUN" || kw == "SAKADAR" || kw == "LIWATAN" {
				endIdx = i
				break
			}
		}

		// Parse bagian WHERE wungkul
		whereCmd, err := parseWhere(tokens[idx+1 : endIdx])
		if err != nil {
			return nil, err
		}
		cmd.Where = whereCmd.Where
		
		// Geser pointer idx ka batas akhir tadi
		idx = endIdx
	}

	// 2. Parsing RUNTUYKEUN (ORDER BY)
	if idx < len(tokens) && strings.ToUpper(tokens[idx]) == "RUNTUYKEUN" {
		// Pastikeun aya ngaran kolom sanggeus keyword
		if idx+1 >= len(tokens) {
			return nil, errors.New("RUNTUYKEUN butuh ngaran kolom")
		}
		
		cmd.OrderBy = tokens[idx+1]
		idx += 2 // Luncat 2 lengkah (keyword + kolom)

		// Cek naha aya modifier TI_LUHUR (DESC) / TI_HANDAP (ASC)
		if idx < len(tokens) {
			mode := strings.ToUpper(tokens[idx])
			if mode == "TI_LUHUR" || mode == "TURUN" { // Support alias
				cmd.OrderDesc = true
				idx++
			} else if mode == "TI_HANDAP" || mode == "NAEK" {
				cmd.OrderDesc = false
				idx++
			}
		}
	}

	// 3. Parsing SAKADAR (LIMIT)
	if idx < len(tokens) && strings.ToUpper(tokens[idx]) == "SAKADAR" {
		if idx+1 >= len(tokens) {
			return nil, errors.New("SAKADAR butuh angka")
		}
		
		limit, err := strconv.Atoi(tokens[idx+1])
		if err != nil {
			return nil, errors.New("SAKADAR kudu angka")
		}
		
		cmd.Limit = limit
		idx += 2
	}

	// 4. Parsing LIWATAN (OFFSET)
	if idx < len(tokens) && strings.ToUpper(tokens[idx]) == "LIWATAN" {
		if idx+1 >= len(tokens) {
			return nil, errors.New("LIWATAN butuh angka")
		}
		
		offset, err := strconv.Atoi(tokens[idx+1])
		if err != nil {
			return nil, errors.New("LIWATAN kudu angka")
		}
		
		cmd.Offset = offset
		idx += 2
	}

	return cmd, nil
}

// ==========================================
// PARSE WHERE (DIMANA) - SUPPORT JIGA & MIX LOGIC
// ==========================================
func parseWhere(tokens []string) (*Command, error) {
	cmd := &Command{Where: []Condition{}}
	remaining := tokens
	
	// Loop salila masih aya minimal 3 token (Col Op Val)
	for len(remaining) >= 3 {
		cond := Condition{
			Field:    remaining[0],
			Operator: remaining[1],
			Value:    remaining[2],
			LogicOp:  "",
		}

		// Khusus Operator JIGA (LIKE), bersihan tanda kutip
		if strings.ToUpper(cond.Operator) == "JIGA" {
			cond.Value = strings.Trim(cond.Value, "'\"")
		}

		// Cek Logika Saterusna (DAN/ATAU/SARENG/ATAWA)
		if len(remaining) > 3 {
			rawLogic := strings.ToUpper(remaining[3])
			
			// Normalisasi Logic Op jadi standar "DAN" / "ATAU"
			if rawLogic == "sareng" || rawLogic == "SARENG" {
				cond.LogicOp = "SARENG"
				remaining = remaining[4:] // Geser 4 lengkah (Col Op Val Logic)
			} else if rawLogic == "atawa" || rawLogic == "ATAWA" {
				cond.LogicOp = "ATAWA"
				remaining = remaining[4:]
			} else {
				// Mun kapanggih token ka-4 tapi lain logika, berarti error atawa beres
				// Tapi di dieu urang anggap beres wae (break)
				remaining = nil
			}
		} else {
			// Geus teu aya sisa token
			remaining = nil
		}

		cmd.Where = append(cmd.Where, cond)
	}

	return cmd, nil
}