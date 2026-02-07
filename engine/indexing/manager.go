package indexing

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/febrd/maungdb/engine/storage"
)

// IndexMap: Peta nilai -> List of PK (Hash Index)
// Contoh: "Bandung" -> ["101", "102"]
type IndexMap map[string][]string

type IndexManager struct {
	mu sync.RWMutex
}

var GlobalIndexManager = &IndexManager{}

// ==========================================
// 1. CORE FUNCTIONS (Build & Lookup)
// ==========================================

// BuildIndex: Nyieun index anyar tina data nu geus aya (TANDAIN ...)
func (im *IndexManager) BuildIndex(tableName, colName string, schemaCols []string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	// Baca sadaya data atah
	rows, err := storage.ReadAll(tableName)
	if err != nil {
		return err
	}

	// Pilarian index kolom dina schema
	colIdx := -1
	for i, col := range schemaCols {
		if col == colName {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return fmt.Errorf("kolom '%s' teu kapendak di tabel '%s'", colName, tableName)
	}


	idxMap := make(IndexMap)
	for _, row := range rows {
		if strings.TrimSpace(row) == "" { continue }
		parts := strings.Split(row, "|")
		if len(parts) > colIdx {
			pk := parts[0]      
			val := parts[colIdx]

			idxMap[val] = append(idxMap[val], pk)
		}
	}

	return im.saveIndexFile(tableName, colName, idxMap)
}

func (im *IndexManager) Lookup(tableName, colName, value string) ([]string, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	idxMap, err := im.loadIndexFile(tableName, colName)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, err
	}

	pks, found := idxMap[value]
	if !found {
		return []string{}, nil
	}
	return pks, nil
}

func (im *IndexManager) UpdateIndexOnInsert(tableName string, rowData string, schemaCols []string) {
	dbPath := storage.GetDBPath()
	if dbPath == "" { return }

	files, err := os.ReadDir(dbPath)
	if err != nil { return }

	parts := strings.Split(rowData, "|")
	if len(parts) == 0 { return }
	pk := parts[0]

	for _, f := range files {
		name := f.Name()
		prefix := tableName + "_"
		suffix := ".idx"

		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			colPart := strings.TrimPrefix(name, prefix)
			colName := strings.TrimSuffix(colPart, suffix)

			colIdx := -1
			for i, c := range schemaCols {
				if c == colName {
					colIdx = i; break
				}
			}

			if colIdx != -1 && colIdx < len(parts) {
				val := parts[colIdx]

				im.mu.Lock()
				idxMap, err := im.loadIndexFile(tableName, colName)
				if err != nil { idxMap = make(IndexMap) } // Mun ruksak/euweuh, jieun anyar
				
				idxMap[val] = append(idxMap[val], pk)
				im.saveIndexFile(tableName, colName, idxMap)
				im.mu.Unlock()
			}
		}
	}
}

func (im *IndexManager) RemoveIndex(tableName, rowID string) {
	dbPath := storage.GetDBPath()
	if dbPath == "" { return }

	files, err := os.ReadDir(dbPath)
	if err != nil { return }
	for _, f := range files {
		name := f.Name()
		if strings.HasPrefix(name, tableName+"_") && strings.HasSuffix(name, ".idx") {
			colPart := strings.TrimPrefix(name, tableName+"_")
			colName := strings.TrimSuffix(colPart, ".idx")

			im.mu.Lock()
			idxMap, err := im.loadIndexFile(tableName, colName)
			if err == nil {
				isChanged := false
				for val, pks := range idxMap {
					newPks := []string{}
					found := false
					for _, p := range pks {
						if p == rowID {
							found = true
						} else {
							newPks = append(newPks, p)
						}
					}
					
					if found {
						if len(newPks) == 0 {
							delete(idxMap, val) // Hapus key mun kosong
						} else {
							idxMap[val] = newPks
						}
						isChanged = true
					}
				}

				if isChanged {
					im.saveIndexFile(tableName, colName, idxMap)
				}
			}
			im.mu.Unlock()
		}
	}
}

func getIndexPath(tableName, colName string) string {
	dbPath := storage.GetDBPath()
	if dbPath == "" { return "" }
	filename := fmt.Sprintf("%s_%s.idx", tableName, colName)
	return filepath.Join(dbPath, filename)
}

func (im *IndexManager) saveIndexFile(tableName, colName string, data IndexMap) error {
	path := getIndexPath(tableName, colName)
	if path == "" { return fmt.Errorf("database path error") }

	file, err := os.Create(path)
	if err != nil { return err }
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(data)
}

func (im *IndexManager) loadIndexFile(tableName, colName string) (IndexMap, error) {
	path := getIndexPath(tableName, colName)
	if path == "" { return nil, fmt.Errorf("database path error") }

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data IndexMap
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}