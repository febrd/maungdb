package schema

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/febrd/maungdb/internal/config"
)



type Column struct {
	Name       string
	Type       string
	Args       []string 
	IsPrimary  bool     
	IsUnique   bool     
	IsNotNull  bool     
	ForeignKey string  

	Primary    bool     `json:"primary"`     
	Unique     bool     `json:"unique"`      
	NotNull    bool     `json:"not_null"`   
}

type Definition struct {
	Columns     []Column
	Perms       map[string][]string
	Permissions map[string][]string 

	Database string   `json:"database"`
	Table    string   `json:"table"`
	Read     []string `json:"read"`
	Write    []string `json:"write"`

}


func CreateComplex(database, table string, columns []Column, perms map[string][]string) error {
	path := filepath.Join(config.DataDir, "db_"+database, table+".schema")

	var headerParts []string

	for _, col := range columns {

		defStr := fmt.Sprintf("%s:%s", col.Name, col.Type)

		if len(col.Args) > 0 {
			defStr += fmt.Sprintf("(%s)", strings.Join(col.Args, ","))
		}

		if col.IsPrimary {
			defStr += ":PK"
		} else {
			if col.IsUnique {
				defStr += ":UNIQUE"
			}
			if col.IsNotNull {
				defStr += ":NOT NULL"
			}
		}

		if col.ForeignKey != "" {
			defStr += fmt.Sprintf(":FK(%s)", col.ForeignKey)
		}

		headerParts = append(headerParts, defStr)
	}

	content := strings.Join(headerParts, "|") + "\n"
	for role, actions := range perms {
		content += fmt.Sprintf("%s=%s\n", role, strings.Join(actions, ","))
	}

	return os.WriteFile(path, []byte(content), 0644)
}

func Create(database, table string, fieldsRaw []string, perms map[string][]string) error {
	var columns []Column

	for _, f := range fieldsRaw {
		parts := strings.SplitN(f, ":", 2)
		if len(parts) != 2 {
			return errors.New("format salah, gunakeun 'kolom:tipe'")
		}

		colName := parts[0]
		fullType := strings.ToUpper(parts[1])
		baseType, args := parseTypeAndArgs(fullType)
		if !isValidType(baseType) {
			return errors.New("tipe data teu didukung: " + baseType)
		}

		columns = append(columns, Column{
			Name: colName,
			Type: baseType,
			Args: args,
		})
	}
	
	return CreateComplex(database, table, columns, perms)
}

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

	rawCols := strings.Split(lines[0], "|")
	var columns []Column

	for _, rc := range rawCols {
		parts := strings.Split(rc, ":")
		
		if len(parts) >= 2 {
			colName := parts[0]
			fullType := parts[1]
			baseType, args := parseTypeAndArgs(fullType)

			col := Column{
				Name: colName,
				Type: baseType,
				Args: args,
			}

			if len(parts) > 2 {
				for _, flag := range parts[2:] {
					flag = strings.ToUpper(strings.TrimSpace(flag))
					
					switch {
					case flag == "PK" || flag == "PRIMARY" || flag == "PRIMARY KEY":
						col.IsPrimary = true
						col.IsNotNull = true
						col.IsUnique = true
					case flag == "UNIQUE":
						col.IsUnique = true
					case flag == "NOTNULL" || flag == "NOT NULL":
						col.IsNotNull = true
					case strings.HasPrefix(flag, "FK(") && strings.HasSuffix(flag, ")"):
						inner := flag[3 : len(flag)-1]
						col.ForeignKey = inner
					}
				}
			}

			columns = append(columns, col)
		}
	}

	def := &Definition{
		Columns: columns,
		Perms:   make(map[string][]string),
	}

	for _, line := range lines[1:] {
		if line == "" { continue }
		parts := strings.Split(line, "=")
		if len(parts) == 2 {
			def.Perms[parts[0]] = strings.Split(parts[1], ",")
		}
	}

	return def, nil
}

func (d *Definition) ValidateRow(data string) error {
	values := strings.Split(data, "|")
	if len(values) != len(d.Columns) {
		return errors.New("jumlah kolom teu sesuai")
	}

	for i, col := range d.Columns {
		val := strings.TrimSpace(values[i])

		if val == "" || strings.ToUpper(val) == "NULL" {
			continue
		}

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
			if _, err := time.Parse("2006-01-02", val); err != nil {
				return fmt.Errorf("kolom '%s' kudu DATE (YYYY-MM-DD)", col.Name)
			}
		case "CHAR":
			if len(col.Args) > 0 {
				limit, _ := strconv.Atoi(col.Args[0])
				if len(val) > limit {
					return fmt.Errorf("kolom '%s' maksimal %d karakter", col.Name, limit)
				}
			}
		case "ENUM":
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
		default:
			return fmt.Errorf("tipe data teu dikenal: %s", col.Type)
		}
	}
	return nil
}

func parseBaseType(fullType string) string {
	idx := strings.Index(fullType, "(")
	if idx == -1 {
		return fullType
	}
	return fullType[:idx]
}

func parseTypeAndArgs(fullType string) (string, []string) {
	idxStart := strings.Index(fullType, "(")
	idxEnd := strings.LastIndex(fullType, ")")

	if idxStart == -1 || idxEnd == -1 {
		return fullType, nil
	}

	base := fullType[:idxStart]
	content := fullType[idxStart+1 : idxEnd]

	args := strings.Split(content, ",")
	for i := range args {
		args[i] = strings.TrimSpace(args[i])
	}
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
	if role == "supermaung" {
		return true
	}
	allowedRoles, ok := d.Perms[action]
	if !ok {
		return false
	}
	for _, r := range allowedRoles {
		if r == role {
			return true
		}
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

func (s *Definition) GetColumnIndex(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}