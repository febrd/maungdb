package transaction

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/febrd/maungdb/engine/storage"
)

func InitManager(inputPath string) *TxManager {
	once.Do(func() {
		var finalPath string

		if strings.HasSuffix(inputPath, ".log") {
			finalPath = inputPath
		} else {
			finalPath = filepath.Join(inputPath, "wal.log")
		}

		dir := filepath.Dir(finalPath)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			os.MkdirAll(dir, 0755)
		}

		GlobalManager = &TxManager{
			activeTxs:   make(map[string]*Transaction),
			walFilePath: finalPath,
		}
	})
	return GlobalManager
}

func GetManager() *TxManager {
	if GlobalManager == nil {
		fmt.Println("⚠️ Warning: TxManager can di-init, ngagunakeun default path.")
		// Fallback nu aman
		return InitManager("./maung_data")
	}
	return GlobalManager
}

func (tm *TxManager) Begin(username string) (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if _, exists := tm.activeTxs[username]; exists {
		return "", errors.New("anjeun parantos gaduh transaksi aktif. JADIKEUN atanapi BATALKEUN heula")
	}

	txID := fmt.Sprintf("tx_%d_%s", time.Now().UnixNano(), username)

	tm.activeTxs[username] = &Transaction{
		ID:        txID,
		User:      username,
		StartTime: time.Now(),
		Status:    TxStatusActive,
		Changes:   make([]WALEntry, 0),
	}

	return txID, nil
}

func (tm *TxManager) AddOperation(username string, opType OpType, table string, data string, prevData string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.activeTxs[username]

	if exists && tx.Status == TxStatusActive {
		entry := WALEntry{
			TxID:      tx.ID,
			User:      username,
			Timestamp: time.Now(),
			Type:      opType,
			TableName: table,
			Data:      data,
			PrevData:  prevData,
		}
		tx.Changes = append(tx.Changes, entry)
		return nil
	}

	return tm.applySingleToStorage(opType, table, data)
}

func (tm *TxManager) Commit(username string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.activeTxs[username]
	if !exists {
		return errors.New("teu aya transaksi aktif pikeun di-commit")
	}

	if len(tx.Changes) == 0 {
		delete(tm.activeTxs, username)
		return nil
	}

	if err := tm.writeLog(tx.Changes); err != nil {
		return fmt.Errorf("gagal nulis WAL: %v", err)
	}

	if err := tm.applyBatchToStorage(tx.Changes); err != nil {
		return fmt.Errorf("gagal nyimpen data fisik: %v", err)
	}

	tx.Status = TxStatusCommitted
	delete(tm.activeTxs, username)

	return nil
}

func (tm *TxManager) Rollback(username string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	_, exists := tm.activeTxs[username]
	if !exists {
		return errors.New("teu aya transaksi aktif pikeun di-rollback")
	}

	delete(tm.activeTxs, username)
	return nil
}

func (tm *TxManager) IsActive(username string) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, exists := tm.activeTxs[username]
	return exists
}

func (tm *TxManager) writeLog(entries []WALEntry) error {
	if tm.walFilePath == "" {
		return fmt.Errorf("WAL path teu valid")
	}

	f, err := os.OpenFile(tm.walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return err
		}
	}
	return f.Sync()
}

func (tm *TxManager) applyBatchToStorage(entries []WALEntry) error {
	for _, entry := range entries {
		if err := tm.applySingleToStorage(entry.Type, entry.TableName, entry.Data); err != nil {
			return err
		}
	}
	return nil
}

func (tm *TxManager) applySingleToStorage(opType OpType, table, data string) error {
	var err error

	parts := strings.Split(data, "|")
	rowID := ""
	if len(parts) > 0 {
		rowID = parts[0]
	}

	switch opType {
	case OpInsert:
		err = storage.Append(table, data)

	case OpUpdate:
		err = storage.CommitUpdate(table, rowID, data)

	case OpDelete:
		err = storage.CommitDelete(table, rowID)

	default:
		return fmt.Errorf("operasi teu dikenal: %s", opType)
	}

	return err
}