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


type IndexMap map[string][]string

type IndexManager struct {
    mu sync.RWMutex
}

var GlobalIndexManager = &IndexManager{}

func (im *IndexManager) BuildIndex(tableName, colName string, schemaCols []string) error {
    im.mu.Lock()
    defer im.mu.Unlock()

    rows, err := storage.ReadAll(tableName)
    if err != nil {
        return err
    }

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
        if row == "" { continue }
        parts := strings.Split(row, "|")
        if len(parts) <= colIdx { continue }

        pk := parts[0]      
        val := parts[colIdx] 

        idxMap[val] = append(idxMap[val], pk)
    }

    return im.saveIndexFile(tableName, colName, idxMap)
}

func (im *IndexManager) Lookup(tableName, colName, value string) ([]string, error) {
    im.mu.RLock()
    defer im.mu.RUnlock()

    idxMap, err := im.loadIndexFile(tableName, colName)
    if err != nil {
        return nil, err 
    }

    pks, found := idxMap[value]
    if !found {
        return []string{}, nil
    }
    return pks, nil
}


func getIndexPath(tableName, colName string) string {
    dbPath := storage.GetDBPath()
    if dbPath == "" { return "" }
    filename := fmt.Sprintf("%s_%s.idx", tableName, colName)
    return filepath.Join(dbPath, filename)
}

func (im *IndexManager) saveIndexFile(tableName, colName string, data IndexMap) error {
    path := getIndexPath(tableName, colName)
    if path == "" { return fmt.Errorf("database teu acan dipilih") }

    file, err := os.Create(path)
    if err != nil { return err }
    defer file.Close()

    encoder := json.NewEncoder(file)
    return encoder.Encode(data)
}

func (im *IndexManager) loadIndexFile(tableName, colName string) (IndexMap, error) {
    path := getIndexPath(tableName, colName)
    if path == "" { return nil, fmt.Errorf("database teu acan dipilih") }

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

func (im *IndexManager) UpdateIndexOnInsert(tableName string, rowData string, schemaCols []string) {
    dbPath := storage.GetDBPath()
    files, _ := os.ReadDir(dbPath)
    
    parts := strings.Split(rowData, "|")
    pk := parts[0]

    for _, f := range files {
        if strings.HasPrefix(f.Name(), tableName+"_") && strings.HasSuffix(f.Name(), ".idx") {
            colPart := strings.TrimPrefix(f.Name(), tableName+"_")
            colName := strings.TrimSuffix(colPart, ".idx")

            colIdx := -1
            for i, c := range schemaCols {
                if c == colName {
                    colIdx = i; break
                }
            }

            if colIdx != -1 && colIdx < len(parts) {
                val := parts[colIdx]
                im.mu.Lock()
                idxMap, _ := im.loadIndexFile(tableName, colName)
                if idxMap == nil { idxMap = make(IndexMap) }
                
                idxMap[val] = append(idxMap[val], pk)
                im.saveIndexFile(tableName, colName, idxMap)
                im.mu.Unlock()
            }
        }
    }
}