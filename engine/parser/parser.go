package parser

import (
	"errors"
	"strconv"
	"strings"
)

// Parse adalah gerbang utama untuk memproses query string menjadi Command struct
func Parse(query string) (*Command, error) {
	query = strings.TrimSpace(query)
	// Hapus ; di akhir jika ada
	query = strings.TrimSuffix(query, ";")

	tokens := strings.Fields(query)
	if len(tokens) == 0 {
		return nil, errors.New("query kosong")
	}

	verb := strings.ToUpper(tokens[0])

	switch verb {
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


// ==========================================
// 1. CREATE (DAMEL)
// ==========================================
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

// ==========================================
// 2. INSERT (SIMPEN)
// ==========================================
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


// ==========================================
// UPDATE: parseSelect (Support Full JOIN: Inner, Left, Right)
// ==========================================
func parseSelect(tokens []string) (*Command, error) {
	cmd := &Command{
		Type:  CmdSelect,
		Limit: -1, // Default -1 artinya euweuh limit
		Joins: []JoinClause{}, // Inisialisasi slice Joins
	}

	// 1. DETEKSI FORMAT: "TINGALI cols TI table" ATAU "TINGALI table"
	tiIndex := -1
	for i, t := range tokens {
		if strings.ToUpper(t) == "TI" || strings.ToUpper(t) == "FROM" {
			tiIndex = i
			break
		}
	}

	idx := 0 // Index pointer pikeun neruskeun parsing clause

	if tiIndex != -1 {
		// === FORMAT BARU: TINGALI nama,gaji TI pegawai ===
		if tiIndex < 1 {
			return nil, errors.New("kolom teu disebutkeun samemeh TI")
		}
		
		// Gabungkeun tokens samemeh TI (misal: "nama" "," "gaji")
		colsPart := strings.Join(tokens[1:tiIndex], " ")
		rawFields := strings.Split(colsPart, ",")
		
		// Bersihkeun spasi
		for _, f := range rawFields {
			cmd.Fields = append(cmd.Fields, strings.TrimSpace(f))
		}

		if tiIndex+1 >= len(tokens) {
			return nil, errors.New("tabel teu disebutkeun sanggeus TI")
		}
		cmd.Table = tokens[tiIndex+1]
		
		// Lanjut parsing sanggeus nama tabel
		idx = tiIndex + 2 

	} else {
		// === FORMAT LAMA: TINGALI pegawai (Implisit SELECT *) ===
		if len(tokens) < 2 {
			return nil, errors.New("format TINGALI salah, minimal: TINGALI <tabel>")
		}
		cmd.Table = tokens[1]
		cmd.Fields = []string{"*"} // Default ambil semua
		
		// Lanjut parsing sanggeus nama tabel
		idx = 2
	}

	// 2. PARSING CLAUSES (JOIN, DIMANA, RUNTUYKEUN, SAKADAR, LIWATAN)
	for idx < len(tokens) {
		token := strings.ToUpper(tokens[idx])

		// --- LOGIKA JOIN BARU ---
		// Deteksi keyword awal: GABUNG, JOIN, LEFT, RIGHT, INNER, KENCAB, KATUHU
		if isJoinKeyword(token) {
			
			// 1. Tentukan Tipe Join (Default: INNER)
			joinType := "INNER"
			
			// Cek apakah tokennya LEFT/RIGHT/INNER/KENCAB/KATUHU
			if token == "LEFT" || token == "KENCA" {
				joinType = "LEFT"
				idx++ // Skip token tipe, lanjut cari "GABUNG/JOIN"
			} else if token == "RIGHT" || token == "KATUHU" {
				joinType = "RIGHT"
				idx++
			} else if token == "INNER" || token == "HIJIKEUN" {
				joinType = "INNER"
				idx++
			} else if token == "FULL" || token == "PINUH" {
				joinType = "FULL" // Optional mun rek support full outer
				idx++
			}

			// Pastikan setelah tipe ada kata GABUNG/JOIN (kecuali user nulis langsung GABUNG tanpa tipe)
			if idx >= len(tokens) {
				return nil, errors.New("paréntah JOIN teu lengkep")
			}
			
			currToken := strings.ToUpper(tokens[idx])
			if currToken == "GABUNG" || currToken == "JOIN" || currToken == "HIJIKEUN"  {
				idx++ // Skip kata GABUNG/JOIN
			} else if token == "GABUNG" || token == "JOIN" || currToken == "HIJIKEUN" {
				// Kasus: User langsung nulis GABUNG (Implicit Inner)
				// Ulah di skip deui, idx geus bener di awal loop
				idx++
			} else {
				return nil, errors.New("sanggeus tipe join kedah aya HIJIKEUN/GABUNG/JOIN")
			}

			// 2. Ambil Nama Tabel Join
			if idx >= len(tokens) {
				return nil, errors.New("tabel join teu disebutkeun")
			}
			joinTable := tokens[idx]
			idx++

			// 3. Cek Keyword ON / DINA
			if idx >= len(tokens) {
				return nil, errors.New("join butuh kondisi DINA / ON")
			}
			onKeyword := strings.ToUpper(tokens[idx])
			if onKeyword != "DINA" && onKeyword != "ON" {
				return nil, errors.New("saenggeus tabel join kedah nganggo DINA / ON")
			}
			idx++

			// 4. Ambil Kondisi (a = b) -> 3 token
			if idx+2 >= len(tokens) {
				return nil, errors.New("kondisi join teu lengkep (col1 = col2)")
			}

			joinCond := Condition{
				Field:    tokens[idx],
				Operator: tokens[idx+1],
				Value:    tokens[idx+2],
			}
			idx += 3 // Maju 3 langkah

			// Simpan ke struct Command
			cmd.Joins = append(cmd.Joins, JoinClause{
				Type:      joinType,
				Table:     joinTable,
				Condition: joinCond,
			})
			
			continue // Lanjut loop (bisi aya join deui atawa dimana)
		}

		// --- CLAUSE LAINNYA ---
		switch token {
		case "DIMANA", "WHERE":
			endIdx := len(tokens)
			for i := idx + 1; i < len(tokens); i++ {
				kw := strings.ToUpper(tokens[i])
				// Tambahkan keyword JOIN ke daftar "stop words"
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
			idx++ // Skip token aneh
		}
	}

	return cmd, nil
}

// Helper untuk cek apakah token adalah awal dari JOIN syntax
func isJoinKeyword(t string) bool {
	return t == "GABUNG" || t == "JOIN" || 
	       t == "INNER" ||  t == "HIJIKEUN" || 
	       t == "LEFT" || t == "KENCA" || 
	       t == "RIGHT" || t == "KATUHU"
}

// ==========================================
// 4. UPDATE (OMEAN)
// ==========================================
func parseUpdate(tokens []string) (*Command, error) {
	// Format: OMEAN <table> JADI/JANTEN col=val,col=val DIMANA ...
	
	if len(tokens) < 4 {
		return nil, errors.New("format OMEAN salah: OMEAN <table> JADI <col>=<val> DIMANA ...")
	}

	// Cek Keyword JADI / JANTEN / SET
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

	// Cari batas DIMANA
	whereIdx := -1
	for i := 3; i < len(tokens); i++ {
		if strings.ToUpper(tokens[i]) == "DIMANA" || strings.ToUpper(tokens[i]) == "WHERE" {
			whereIdx = i
			break
		}
	}

	// Parse Updates (col=val)
	updateEnd := len(tokens)
	if whereIdx != -1 {
		updateEnd = whereIdx
	}

	// Gabung token update heula bisi aya spasi, terus split koma
	updatePart := strings.Join(tokens[3:updateEnd], " ")
	pairs := strings.Split(updatePart, ",")
	
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) == 2 {
			cmd.Updates[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}

	// Parse DIMANA (Lamun aya)
	if whereIdx != -1 {
		conds, err := parseConditionsList(tokens[whereIdx+1:])
		if err != nil {
			return nil, err
		}
		cmd.Where = conds
	}

	return cmd, nil
}

// ==========================================
// 5. DELETE (MICEUN)
// ==========================================
func parseDelete(tokens []string) (*Command, error) {
	// Format: MICEUN TI <table_name> DIMANA ...
	if len(tokens) < 3 {
		return nil, errors.New("format MICEUN salah: MICEUN TI <table> DIMANA ...")
	}

	// Cek TI / FROM
	if strings.ToUpper(tokens[1]) != "TI" && strings.ToUpper(tokens[1]) != "FROM" {
		return nil, errors.New("kedah nganggo TI")
	}

	cmd := &Command{
		Type:  CmdDelete,
		Table: tokens[2],
		Where: []Condition{},
	}

	// Cek DIMANA
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

// ==========================================
// HELPER: CONDITION PARSER
// (Dipake ku SELECT, UPDATE, DELETE)
// ==========================================
func parseConditionsList(tokens []string) ([]Condition, error) {
	var conditions []Condition
	i := 0
	for i < len(tokens) {
		// Minimal butuh 3 token: field op value
		if i+2 >= len(tokens) {
			break 
		}

		field := tokens[i]
		op := tokens[i+1]
		val := tokens[i+2]
		
		// Bersihkeun tanda kutip dina value (misal: 'Asep' -> Asep)
		val = strings.Trim(val, "'\"")

		cond := Condition{
			Field:    field,
			Operator: op,
			Value:    val,
		}

		// Cek Logic Operator saenggeusna (SARENG / ATAWA)
		if i+3 < len(tokens) {
			logic := strings.ToUpper(tokens[i+3])
			if logic == "SARENG" || logic == "AND" || logic == "ATAWA" || logic == "OR" {
				cond.LogicOp = logic
				i++ // Loncat token logic
			}
		}
		
		conditions = append(conditions, cond)
		i += 3 // Maju ka kondisi saterusna
	}
	return conditions, nil
}