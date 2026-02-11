package explain

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type ExplanationResult struct {
	QueryID        string `json:"query_id"`
	Operation      string `json:"operation"`       
	TargetTable    string `json:"target_table"`    
	Strategy       string `json:"strategy"`        
	Cost           int    `json:"estimated_cost"`  
	IndexUsed      string `json:"index_used"`      
	Complexity     string `json:"complexity"`      
	RowsExamined   string `json:"rows_examined"`   
	Message        string `json:"message"`         
	Recommendation string `json:"recommendation"`
	Timestamp      string `json:"timestamp"`
}

type Manager struct {
	DataDir string 
}

func NewManager(dataDir string) *Manager {
	return &Manager{
		DataDir: dataDir,
	}
}


func (m *Manager) Analyze(activeDB string, query string) (*ExplanationResult, error) {
	if m == nil {
		return nil, fmt.Errorf("ExplainManager (JELASKEUN) teu acan di-init. Cek startServer() di server.go")
	}

	cleanQuery := strings.TrimSpace(query)
	upperQuery := strings.ToUpper(cleanQuery)

	if strings.HasPrefix(upperQuery, "JELASKEUN") {
		cleanQuery = strings.TrimSpace(cleanQuery[9:])
		upperQuery = strings.TrimSpace(upperQuery[9:])
	}

	result := &ExplanationResult{
		QueryID:   fmt.Sprintf("Q-%d", time.Now().UnixNano()),
		Timestamp: time.Now().Format(time.RFC3339),
	}

	switch {
	
	case strings.HasPrefix(upperQuery, "DAMEL JARAMBAH"):
		return m.analyzeTrigger(activeDB, upperQuery, result)
	case strings.HasPrefix(upperQuery, "TINGALI"):
		return m.analyzeSelect(activeDB, upperQuery, result)
	case strings.HasPrefix(upperQuery, "SIMPEN"):
		return m.analyzeInsert(activeDB, upperQuery, result)
	case strings.HasPrefix(upperQuery, "OMEAN"):
		return m.analyzeUpdate(activeDB, upperQuery, result)
	case strings.HasPrefix(upperQuery, "MICEUN"):
		return m.analyzeDelete(activeDB, upperQuery, result)
	case strings.HasPrefix(upperQuery, "KOREHAN"):
		return m.analyzeFTS(activeDB, upperQuery, result)
	case strings.HasPrefix(upperQuery, "DAMEL"):
		return m.analyzeDDL(activeDB, upperQuery, result)	
	case strings.HasPrefix(upperQuery, "TANDAIN") || strings.HasPrefix(upperQuery, "TANDAAN") || strings.HasPrefix(upperQuery, "TAWISAN"):
		return m.analyzeIndexing(activeDB, upperQuery, result)
	case strings.HasPrefix(upperQuery, "JADI"):
		return m.analyzeReplication(activeDB, upperQuery, result)

	default:
		return nil, fmt.Errorf("Parentah teu dikenal atanapi teu acan didukung ku JELASKEUN: %s", cleanQuery)
	}
}

func (m *Manager) analyzeTrigger(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	parts := strings.Fields(query)
	res.Operation = "CREATE TRIGGER (JARAMBAH)"
	res.Strategy = "EVENT LISTENER REGISTRATION"
	
	var eventType string
	var targetTable string
	
	for i, p := range parts {
		if p == "PADA" && i+1 < len(parts) {
			targetTable = parts[i+1]
		}
		if p == "WAKTU" && i+1 < len(parts) {
			eventType = parts[i+1] 
		}
	}

	res.TargetTable = targetTable

	res.Cost = 25 
	res.Complexity = "O(1) Setup"	
	msg := "Trigger bakal didaptarkeun kana sistem. "
	if targetTable != "" && eventType != "" {
		msg += fmt.Sprintf("Unggal aya operasi '%s' dina tabel '%s', MaungDB bakal ngajalankeun logika tambahan otomatis.", eventType, targetTable)
	} else {
		msg += "Syntax trigger valid, siap dieksekusi."
	}
	res.Message = msg

	res.Recommendation = "Ati-ati: Trigger nambihan overhead (waktos proses) kana unggal transaksi DML. Pastikeun logika 'LAKUKAN' anjeun efisien & teu looping."

	return res, nil
}

func (m *Manager) analyzeSelect(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	res.Operation = "READ (TINGALI)"
	
	parts := strings.Fields(query)
	var tableName string
	var whereCol string
	var hasJoin string 
	var hasGroup bool
	var hasOrder bool
	var isDiscovery bool

	for i, p := range parts {
		if p == "PANGKAL" {
			isDiscovery = true
		}
		if p == "TI" && i+1 < len(parts) {
			tableName = parts[i+1]
		}
		if p == "DIMANA" && i+1 < len(parts) {
			cond := parts[i+1]
			if strings.Contains(cond, "=") {
				whereCol = strings.Split(cond, "=")[0]
			}
		}
		if p == "GABUNG" || p == "HIJIKEUN" {
			hasJoin = "INNER"
			if i > 0 && parts[i-1] == "KENCA" { hasJoin = "LEFT" }
			if i > 0 && parts[i-1] == "KATUHU" { hasJoin = "RIGHT" }
		}
		if p == "KUMPULKEUN" { hasGroup = true }
		if p == "RUNTUYKEUN" { hasOrder = true }
	}

	if isDiscovery {
		res.TargetTable = "SYSTEM"
		res.Strategy = "METADATA READ"
		res.Cost = 1
		res.Complexity = "O(1)"
		res.Message = "Maca daptar folder database tina sistem file."
		return res, nil
	}

	res.TargetTable = tableName

	idxPath := fmt.Sprintf("%s/%s/%s_%s.idx", m.DataDir, db, tableName, whereCol)
	hasIndex := false
	if whereCol != "" {
		if _, err := os.Stat(idxPath); err == nil {
			hasIndex = true
		}
	}

	if hasJoin != "" {
		res.Strategy = fmt.Sprintf("NESTED LOOP JOIN (%s)", hasJoin)
		res.Cost = 500
		res.Complexity = "O(N*M)"
		res.RowsExamined = "Cartesian Product (Potensi Besar)"
		res.Message = fmt.Sprintf("Query ieu ngagabungkeun tabel (%s JOIN). Ieu operasi paling beurat di database.", hasJoin)
		res.Recommendation = "Pastikeun kolom nu dianggo 'DINA' (ON) parantos di-index di kadua tabel."
	} else if whereCol != "" && hasIndex {
		res.Strategy = "INDEX SCAN (Hash Map LookUp)"
		res.Cost = 5 
		res.Complexity = "O(1)"
		res.IndexUsed = fmt.Sprintf("%s_%s.idx", tableName, whereCol)
		res.RowsExamined = "1 (Langsung kapendak)"
		res.Message = "Gancang pisan! MaungDB langsung nyandak data tina memory index (Jalan Tol)."
	} else if whereCol != "" {
		res.Strategy = "FULL TABLE SCAN (Filtering)"
		res.Cost = 100
		res.Complexity = "O(N)"
		res.RowsExamined = "Sadaya baris (N)"
		res.Message = fmt.Sprintf("Awas! Kolom '%s' teu gaduh index. MaungDB kedah maca hiji-hiji baris (Jalan Satapak).", whereCol)
		res.Recommendation = fmt.Sprintf("Jalankeun: TANDAIN %s DINA %s", tableName, whereCol)
	} else {
		res.Strategy = "FULL TABLE SCAN (All Data)"
		res.Cost = 50
		res.Complexity = "O(N)"
		res.RowsExamined = "Sadaya baris (N)"
		res.Message = "MaungDB bakal maca sadaya data tanpa filter."
		res.Recommendation = "Anggo 'SAKADAR' (Limit) upami data seueur pisan."
	}

	if hasGroup {
		res.Cost += 30
		res.Message += " + Agregasi (Grouping) nambihan beban CPU pikeun hashing keys."
	}
	if hasOrder {
		res.Cost += 20
		res.Message += " + Sorting (Runtuykeun) nambihan beban memori (Quicksort)."
	}

	return res, nil
}

func (m *Manager) analyzeInsert(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return nil, fmt.Errorf("Syntax SIMPEN lepat")
	}
	res.Operation = "WRITE (SIMPEN)"
	res.TargetTable = parts[1]
	res.Strategy = "APPEND ONLY LOG"
	res.Cost = 10 
	res.Complexity = "O(1)"
	res.RowsExamined = "0"
	res.Message = "Operasi panggancangna. Data langsung ditempelkeun di tungtung file .mg. WAL (Write Ahead Log) aktif pikeun durabilitas."

	return res, nil
}

func (m *Manager) analyzeUpdate(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	res.Operation = "MODIFY (OMEAN)"
	
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return nil, fmt.Errorf("Syntax OMEAN lepat")
	}
	res.TargetTable = parts[1]

	var whereCol string
	for i, p := range parts {
		if p == "DIMANA" && i+1 < len(parts) {
			cond := parts[i+1]
			if strings.Contains(cond, "=") {
				whereCol = strings.Split(cond, "=")[0]
			}
		}
	}

	idxPath := fmt.Sprintf("%s/%s/%s_%s.idx", m.DataDir, db, res.TargetTable, whereCol)
	hasIndex := false
	if _, err := os.Stat(idxPath); err == nil {
		hasIndex = true
	}

	if hasIndex {
		res.Strategy = "INDEXED UPDATE"
		res.Cost = 20 
		res.Complexity = "O(1) + IO Write"
		res.IndexUsed = fmt.Sprintf("%s_%s.idx", res.TargetTable, whereCol)
		res.Message = "Update efisien. Baris nu bade dirobah kapendak langsung via Index."
	} else {
		res.Strategy = "SCAN & UPDATE"
		res.Cost = 150
		res.Complexity = "O(N)"
		res.RowsExamined = "Sadaya baris (N)"
		res.Message = "Update lambat. MaungDB kedah milarian baris nu cocog heula sateuacan ngarobah."
		res.Recommendation = "Pastikeun kolom di 'DIMANA' parantos di-index."
	}

	return res, nil
}

func (m *Manager) analyzeDelete(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	res.Operation = "DELETE (MICEUN)"
	parts := strings.Fields(query)
	for i, p := range parts {
		if p == "TI" && i+1 < len(parts) {
			res.TargetTable = parts[i+1]
		}
	}

	res.Strategy = "SOFT DELETE (TOMBSTONE)"
	res.Cost = 25
	res.Message = "Data moal langsung dihapus fisik (Hard Delete), tapi ditandaan (Soft Delete) supados gancang."
	res.Recommendation = "Lakukan 'VACUUM' / Re-organize tabel secara berkala upami seueur data dihapus (Fitur v3)."
	return res, nil
}

func (m *Manager) analyzeFTS(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	res.Operation = "FULL TEXT SEARCH (KOREHAN)"
	res.Complexity = "O(log N)" 
	parts := strings.Fields(query)
	if len(parts) > 1 {
		res.TargetTable = parts[1]
	}

	res.Strategy = "INVERTED INDEX LOOKUP"
	res.Cost = 15
	res.Message = "Pencarian teks canggih. MaungDB milarian token kata dina Inverted Index, sanes scanning string (LIKE)."
	res.RowsExamined = "Hanya dokumen nu ngandung kata kunci"
	
	return res, nil
}

func (m *Manager) analyzeDDL(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	parts := strings.Fields(query)
	
	if len(parts) > 1 {
		switch parts[1] {
		case "KACA":
			res.Operation = "CREATE VIEW (DAMEL KACA)"
			res.Strategy = "METADATA WRITE"
			res.Cost = 5
			res.Message = "Tabel Virtual. Query disimpen salaku definisi (.view), data tetep di tabel asli."
		case "INDEKS_TEKS":
			res.Operation = "CREATE FTS INDEX"
			res.Strategy = "TOKENIZATION & INDEXING"
			res.Cost = 500 
			res.Complexity = "O(N * Words)"
			res.Message = "Proses ieu beurat di awal. MaungDB bakal maca sadaya teks, misahkeun per-kata, sareng nyieun Inverted Index."
		default:
			res.Operation = "CREATE TABLE (DAMEL)"
			res.Strategy = "FILE ALLOCATION"
			res.Cost = 50
			res.Message = "Nyieun file .schema sareng .mg anyar dina folder database."
		}
	}
	return res, nil
}

func (m *Manager) analyzeIndexing(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	res.Operation = "CREATE HASH INDEX"
	res.Strategy = "HASH MAPPING"
	res.Cost = 200 
	res.Complexity = "O(N)"
	res.Message = "MaungDB bakal maca sadaya data nu tos aya, nyieun Hash Map di memori, teras disimpen ka file .idx."
	res.Recommendation = "Lakukan ieu pas trafik sepi, sabab tiasa ngonci tabel sakedap."
	return res, nil
}

func (m *Manager) analyzeReplication(db, query string, res *ExplanationResult) (*ExplanationResult, error) {
	parts := strings.Fields(query)
	if len(parts) > 1 && parts[1] == "INDUNG" {
		res.Operation = "PROMOTE TO MASTER"
		res.Strategy = "ROLE SWITCH"
		res.Message = "Node ieu bakal janten INDUNG (Master). Menerima Read & Write."
	} else {
		res.Operation = "DEMOTE TO SLAVE"
		res.Strategy = "SYNC STREAM START"
		res.Message = "Node ieu bakal janten ANAK (Slave). Mode Read-Only. Data bakal disinkronisasi ti Indung."
	}
	res.Cost = 0
	return res, nil
}