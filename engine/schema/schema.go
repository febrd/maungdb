package schema

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time" // PENTING: Tambahkeun library time

	"github.com/febrd/maungdb/internal/config"
)

type Column struct {
	Name string
	Type string   // "STRING", "INT", "FLOAT", "BOOL", "DATE", "ENUM", "CHAR", "TEXT"
	Args []string // Nyimpen detail jiga panjang CHAR(5) atawa opsi ENUM(A,B)
}

type Definition struct {
	Columns []Column
	Perms   map[string][]string
}

// Create schema anyar
// Input: fieldsRaw = ["nama:STRING", "gender:ENUM(L,P)", "kode:CHAR(5)", "lahir:DATE"]
func Create(database, table string, fieldsRaw []string, perms map[string][]string) error {
	path := filepath.Join(config.DataDir, "db_"+database, table+".schema")

	var headerParts []string
	
	for _, f := range fieldsRaw {
		// Validasi format dasar "nama:tipe"
		parts := strings.SplitN(f, ":", 2) // SplitN supaya aman mun aya ':' dina argumen
		if len(parts) != 2 {
			return errors.New("format salah, gunakeun 'kolom:tipe'")
		}

		colName := parts[0]
		fullType := strings.ToUpper(parts[1]) // Misal: "ENUM(L,P)"

		// Validasi Tipe Data
		baseType := parseBaseType(fullType)
		if !isValidType(baseType) {
			return errors.New("tipe data teu didukung: " + baseType)
		}

		// Simpen format aslina, misal "gender:ENUM(L,P)"
		headerParts = append(headerParts, colName+":"+fullType)
	}

	// 1. Tulis Header (Ganti koma jadi PIPE '|' supaya aman keur ENUM)
	content := strings.Join(headerParts, "|") + "\n"

	// 2. Tulis Permissions
	for role, actions := range perms {
		content += fmt.Sprintf("%s=%s\n", role, strings.Join(actions, ","))
	}

	return os.WriteFile(path, []byte(content), 0644)
}

// Load maca schema
func Load(database, table string) (*Definition, error) {
	path := filepath.Join(config.DataDir, "db_"+database, table+".schema")

	content, err := os.ReadFile(path)
	if err != nil {
		return nil, errors.New("table teu kapanggih")
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) < 1 {
		return nil, errors.New("schema ruksak")
	}

	// Parse Header: "nama:STRING|gender:ENUM(L,P)|kode:CHAR(5)"
	// Awas: Ayeuna dipisah ku PIPE "|"
	rawCols := strings.Split(lines[0], "|")
	var columns []Column

	for _, rc := range rawCols {
		parts := strings.SplitN(rc, ":", 2)
		if len(parts) == 2 {
			colName := parts[0]
			fullType := parts[1] // "ENUM(L,P)" atawa "INT"
			
			baseType, args := parseTypeAndArgs(fullType)
			
			columns = append(columns, Column{
				Name: colName, 
				Type: baseType,
				Args: args,
			})
		}
	}

	def := &Definition{
		Columns: columns,
		Perms:   make(map[string][]string),
	}

	// Parse Permissions
	for _, line := range lines[1:] {
		if line == "" { continue }
		parts := strings.Split(line, "=")
		if len(parts) == 2 {
			def.Perms[parts[0]] = strings.Split(parts[1], ",")
		}
	}

	return def, nil
}

// ValidateRow mastikeun data nu asup valid
func (d *Definition) ValidateRow(data string) error {
	values := strings.Split(data, "|")
	if len(values) != len(d.Columns) {
		return errors.New("jumlah kolom teu sesuai")
	}

	for i, col := range d.Columns {
		val := strings.TrimSpace(values[i])

		switch col.Type {
		case "INT":
			if _, err := strconv.Atoi(val); err != nil {
				return fmt.Errorf("kolom '%s' kudu INT (angka)", col.Name)
			}
		case "FLOAT":
			if _, err := strconv.ParseFloat(val, 64); err != nil {
				return fmt.Errorf("kolom '%s' kudu FLOAT (desimal)", col.Name)
			}
		case "BOOL":
			if val != "true" && val != "false" {
				return fmt.Errorf("kolom '%s' kudu BOOL (true/false)", col.Name)
			}
		case "DATE":
			// Format YYYY-MM-DD
			if _, err := time.Parse("2006-01-02", val); err != nil {
				return fmt.Errorf("kolom '%s' kudu DATE (YYYY-MM-DD)", col.Name)
			}
		case "CHAR":
			// CHAR(5) -> kudu pas 5 karakter (atawa max 5, bebas aturanana)
			// Di dieu urang jieun MAX length
			limit, _ := strconv.Atoi(col.Args[0])
			if len(val) > limit {
				return fmt.Errorf("kolom '%s' maksimal %d karakter", col.Name, limit)
			}
		case "ENUM":
			// ENUM(A,B) -> val kudu salah sahiji ti A atawa B
			valid := false
			for _, opt := range col.Args {
				if val == opt {
					valid = true
					break
				}
			}
			if !valid {
				return fmt.Errorf("kolom '%s' kudu salah sahiji tina: %v", col.Name, col.Args)
			}
		case "STRING", "TEXT":
			// Bebas
		default:
			return fmt.Errorf("tipe data teu dikenal: %s", col.Type)
		}
	}
	return nil
}

// === HELPER FUNCTIONS ===

// parseBaseType nyokot "ENUM" tina "ENUM(A,B)"
func parseBaseType(fullType string) string {
	idx := strings.Index(fullType, "(")
	if idx == -1 {
		return fullType
	}
	return fullType[:idx]
}

// parseTypeAndArgs misahkeun "ENUM(A,B)" jadi "ENUM" jeung ["A", "B"]
func parseTypeAndArgs(fullType string) (string, []string) {
	idxStart := strings.Index(fullType, "(")
	idxEnd := strings.LastIndex(fullType, ")")

	if idxStart == -1 || idxEnd == -1 {
		return fullType, nil
	}

	base := fullType[:idxStart]
	content := fullType[idxStart+1 : idxEnd] // "A,B"
	
	// Split eusi kurung ku koma
	args := strings.Split(content, ",")
	return base, args
}

func isValidType(t string) bool {
	valid := map[string]bool{
		"INT": true, "STRING": true, "FLOAT": true, "BOOL": true,
		"DATE": true, "CHAR": true, "ENUM": true, "TEXT": true,
	}
	return valid[t]
}

func (d *Definition) Can(role, action string) bool {
    // ... (kode Can nu kamari, teu robah) ...
	if role == "supermaung" { return true }
	allowedRoles, ok := d.Perms[action]
	if !ok { return false }
	for _, r := range allowedRoles {
		if r == role { return true }
	}
	return false
}

func (d *Definition) GetFieldNames() []string {
	var names []string
	for _, c := range d.Columns {
		names = append(names, c.Name)
	}
	return names
}