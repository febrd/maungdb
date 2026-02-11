package export

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Manager struct {
	DataDir string
}

func NewManager(dataDir string) *Manager {
	return &Manager{
		DataDir: dataDir,
	}
}

func (m *Manager) ExportDatabase(dbName string) (string, error) {
	if m == nil {
		return "", fmt.Errorf("ExportManager (EKSPOR) teu acan di-init di startServer()")
	}
	
	folderName := "db_" + dbName
	dbPath := filepath.Join(m.DataDir, folderName)

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		fallbackPath := filepath.Join(m.DataDir, dbName)
		if _, errFallback := os.Stat(fallbackPath); errFallback == nil {
			dbPath = fallbackPath
		} else {
			return "", fmt.Errorf("Database '%s' teu kapendak di %s atanapi %s", dbName, dbPath, fallbackPath)
		}
	}

	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("-- 🐯 MAUNGDB EXPORT FILE\n"))
	builder.WriteString(fmt.Sprintf("-- Generated: %s\n", time.Now().Format(time.RFC1123)))
	builder.WriteString(fmt.Sprintf("-- Version: v2.2.9 (Enterprise)\n\n"))
	builder.WriteString("-- [ BAGIAN 0: SETUP DATABASE ] --\n")
	builder.WriteString(fmt.Sprintf("createdb %s;\n", dbName))
	builder.WriteString(fmt.Sprintf("angge %s;\n\n", dbName))

	files, err := os.ReadDir(dbPath)
	if err != nil {
		return "", err
	}

	var schemaFiles []string
	var indexFiles []string
	var viewFiles []string
	var triggerFiles []string
	var ftsFiles []string

	for _, f := range files {
		name := f.Name()
		if f.IsDir() {
			continue 
		}

		if strings.HasSuffix(name, ".schema") {
			schemaFiles = append(schemaFiles, name)
		} else if strings.HasSuffix(name, ".idx") {
			indexFiles = append(indexFiles, name)
		} else if strings.HasSuffix(name, ".view") {
			viewFiles = append(viewFiles, name)
		} else if strings.HasSuffix(name, ".trig") {
			triggerFiles = append(triggerFiles, name)
		} else if strings.HasSuffix(name, ".fts") {
			ftsFiles = append(ftsFiles, name)
		}
	}

	builder.WriteString("-- [ BAGIAN 1: STRUKTUR TABEL & DATA ] --\n\n")
	
	for _, schemaFile := range schemaFiles {
		tableName := strings.TrimSuffix(schemaFile, ".schema")
		
		ddl, err := m.reverseDDL(dbPath, tableName)
		if err != nil {
			builder.WriteString(fmt.Sprintf("-- Error exporting schema %s: %v\n", tableName, err))
			continue
		}
		builder.WriteString(ddl + ";\n")
		dml, count, err := m.reverseDML(dbPath, tableName)
		if err != nil {
			builder.WriteString(fmt.Sprintf("-- Error exporting data %s: %v\n", tableName, err))
		} else {
			builder.WriteString(dml)
			builder.WriteString(fmt.Sprintf("-- Total Data: %d baris\n", count))
		}
		builder.WriteString("\n")
	}

	if len(indexFiles) > 0 {
		builder.WriteString("-- [ BAGIAN 2: OPTIMASI INDEX ] --\n")
		for _, idxFile := range indexFiles {
			name := strings.TrimSuffix(idxFile, ".idx")
			parts := strings.Split(name, "_")
			if len(parts) >= 2 {
				tbl := parts[0]
				col := strings.Join(parts[1:], "_")
				builder.WriteString(fmt.Sprintf("TANDAIN %s DINA %s;\n", tbl, col)) // Tambah ;
			}
		}
		builder.WriteString("\n")
	}

	if len(ftsFiles) > 0 {
		builder.WriteString("-- [ BAGIAN 3: FULL TEXT SEARCH ] --\n")
		for _, ftsFile := range ftsFiles {
			name := strings.TrimSuffix(ftsFile, ".fts")
			parts := strings.Split(name, "_")
			if len(parts) >= 2 {
				tbl := parts[0]
				col := strings.Join(parts[1:], "_")
				builder.WriteString(fmt.Sprintf("DAMEL INDEKS_TEKS %s DINA %s;\n", tbl, col)) // Tambah ;
			}
		}
		builder.WriteString("\n")
	}

	if len(viewFiles) > 0 {
		builder.WriteString("-- [ BAGIAN 4: VIRTUAL VIEWS (KACA) ] --\n")
		for _, viewFile := range viewFiles {
			viewName := strings.TrimSuffix(viewFile, ".view")
			query, err := os.ReadFile(filepath.Join(dbPath, viewFile))
			if err == nil {
				cleanQuery := strings.TrimSpace(string(query))
				builder.WriteString(fmt.Sprintf("DAMEL KACA %s TINA %s;\n", viewName, cleanQuery)) // Tambah ;
			}
		}
		builder.WriteString("\n")
	}

	if len(triggerFiles) > 0 {
		builder.WriteString("-- [ BAGIAN 5: EVENT TRIGGERS (JARAMBAH) ] --\n")
		for _, trigFile := range triggerFiles {
			content, err := os.ReadFile(filepath.Join(dbPath, trigFile))
			if err == nil {
				cleanContent := strings.TrimSpace(string(content))
				if !strings.HasSuffix(cleanContent, ";") {
					cleanContent += ";"
				}
				builder.WriteString(cleanContent + "\n")
			}
		}
		builder.WriteString("\n")
	}

	builder.WriteString("-- EXPORT RÉNGSÉ. HATUR NUHUN --")
	return builder.String(), nil
}


func (m *Manager) reverseDDL(dbPath, tableName string) (string, error) {
	schemaPath := filepath.Join(dbPath, tableName+".schema")
	
	content, err := os.ReadFile(schemaPath)
	if err != nil {
		return "", err
	}

	fullText := string(content)
	lines := strings.Split(fullText, "\n")
	if len(lines) == 0 {
		return "", fmt.Errorf("file schema kosong")
	}

	
	colDef := strings.TrimSpace(lines[0])
	formattedCols := strings.ReplaceAll(colDef, "|", ",")

	return fmt.Sprintf("DAMEL %s %s", tableName, formattedCols), nil
}

func (m *Manager) reverseDML(dbPath, tableName string) (string, int, error) {
	dataPath := filepath.Join(dbPath, tableName+".mg")
	file, err := os.Open(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", 0, nil
		}
		return "", 0, err
	}
	defer file.Close()

	var sb strings.Builder
	scanner := bufio.NewScanner(file)
	count := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		
		sb.WriteString(fmt.Sprintf("SIMPEN %s %s;\n", tableName, line))
		count++
	}

	return sb.String(), count, scanner.Err()
}