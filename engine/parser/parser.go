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


func parseSelect(tokens []string) (*Command, error) {
	if len(tokens) < 2 {
		return nil, errors.New("format TINGALI salah, minimal: TINGALI <tabel>")
	}

	cmd := &Command{
		Type:  CmdSelect,
		Table: tokens[1],
		Where: []Condition{},
		Limit: -1, 
	}

	idx := 2


	if idx < len(tokens) && strings.ToUpper(tokens[idx]) == "DIMANA" {
		endIdx := len(tokens)
		
		for i := idx + 1; i < len(tokens); i++ {
			kw := strings.ToUpper(tokens[i])
			if kw == "RUNTUYKEUN" || kw == "SAKADAR" || kw == "LIWATAN" {
				endIdx = i
				break
			}
		}

		whereCmd, err := parseWhere(tokens[idx+1 : endIdx])
		if err != nil {
			return nil, err
		}
		cmd.Where = whereCmd.Where
		
		idx = endIdx
	}

	if idx < len(tokens) && strings.ToUpper(tokens[idx]) == "RUNTUYKEUN" {
		if idx+1 >= len(tokens) {
			return nil, errors.New("RUNTUYKEUN butuh ngaran kolom")
		}
		
		cmd.OrderBy = tokens[idx+1]
		idx += 2 

		if idx < len(tokens) {
			mode := strings.ToUpper(tokens[idx])
			if mode == "TI_LUHUR" || mode == "TURUN" { 
				cmd.OrderDesc = true
				idx++
			} else if mode == "TI_HANDAP" || mode == "NAEK" {
				cmd.OrderDesc = false
				idx++
			}
		}
	}

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


func parseWhere(tokens []string) (*Command, error) {
	cmd := &Command{Where: []Condition{}}
	remaining := tokens
	
	for len(remaining) >= 3 {
		cond := Condition{
			Field:    remaining[0],
			Operator: remaining[1],
			Value:    remaining[2],
			LogicOp:  "",
		}

		if strings.ToUpper(cond.Operator) == "JIGA" {
			cond.Value = strings.Trim(cond.Value, "'\"")
		}

		if len(remaining) > 3 {
			rawLogic := strings.ToUpper(remaining[3])
			
			if rawLogic == "sareng" || rawLogic == "SARENG" {
				cond.LogicOp = "SARENG"
				remaining = remaining[4:] 
			} else if rawLogic == "atawa" || rawLogic == "ATAWA" {
				cond.LogicOp = "ATAWA"
				remaining = remaining[4:]
			} else {
				remaining = nil
			}
		} else {
			remaining = nil
		}

		cmd.Where = append(cmd.Where, cond)
	}

	return cmd, nil
}