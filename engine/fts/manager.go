package fts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"unicode"

	"github.com/febrd/maungdb/engine/storage"
)

type InvertedIndex map[string][]string

type FTSManager struct {
	mu sync.RWMutex
}

var GlobalFTS = &FTSManager{}

func tokenize(text string) []string {
	text = strings.ToLower(text)
	
	cleaner := func(c rune) bool {
		return !unicode.IsLetter(c) && !unicode.IsNumber(c)
	}
	
	words := strings.FieldsFunc(text, cleaner)
	uniqueWords := make(map[string]bool)
	var result []string

	for _, w := range words {
		if len(w) > 2 && !uniqueWords[w] { 
			uniqueWords[w] = true
			result = append(result, w)
		}
	}
	return result
}

func (fm *FTSManager) BuildIndex(tableName, colName string, schemaCols []string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	rows, err := storage.ReadAll(tableName)
	if err != nil {
		return err
	}

	colIdx := -1
	for i, c := range schemaCols {
		if c == colName {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return fmt.Errorf("kolom %s teu kapendak", colName)
	}

	index := make(InvertedIndex)

	for _, row := range rows {
		if row == "" { continue }
		parts := strings.Split(row, "|")
		if len(parts) <= colIdx { continue }

		rowID := parts[0]
		content := parts[colIdx]
		tokens := tokenize(content)

		for _, token := range tokens {
			index[token] = append(index[token], rowID)
		}
	}

	return fm.saveToFile(tableName, colName, index)
}

func (fm *FTSManager) Search(tableName, colName, keyword string) ([]string, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	index, err := fm.loadFromFile(tableName, colName)
	if err != nil {
		return nil, fmt.Errorf("index teks teu acan didamel (mangga jalankeun: DAMEL INDEKS_TEKS %s %s)", tableName, colName)
	}

	keyword = strings.ToLower(strings.TrimSpace(keyword))
	rowIDs, found := index[keyword]
	if !found {
		return []string{}, nil 
	}

	return rowIDs, nil
}

func (fm *FTSManager) getPath(tableName, colName string) string {
	dbPath := storage.GetDBPath()
	if dbPath == "" { return "" }
	return filepath.Join(dbPath, fmt.Sprintf("%s_%s.fts", tableName, colName))
}

func (fm *FTSManager) saveToFile(table, col string, data InvertedIndex) error {
	path := fm.getPath(table, col)
	f, err := os.Create(path)
	if err != nil { return err }
	defer f.Close()
	return json.NewEncoder(f).Encode(data)
}

func (fm *FTSManager) loadFromFile(table, col string) (InvertedIndex, error) {
	path := fm.getPath(table, col)
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	
	var data InvertedIndex
	if err := json.NewDecoder(f).Decode(&data); err != nil { return nil, err }
	return data, nil
}