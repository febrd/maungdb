package transaction

import (
	"encoding/json" 
	"fmt"
	"math/rand"
	"os"           
	"strings"     
	"sync"
	"time"

	"github.com/febrd/maungdb/engine/storage" 
)

type TxManager struct {
	mu          sync.RWMutex             
	activeTxs   map[string]*TransactionContext 
	walFilePath string                
}

var instance *TxManager
var once sync.Once

func InitManager(walPath string) *TxManager {
	once.Do(func() {
		instance = &TxManager{
			activeTxs:   make(map[string]*TransactionContext),
			walFilePath: walPath,
		}
	})
	return instance
}

func GetManager() *TxManager {
	if instance == nil {
		panic("Transaction Manager belum di-init!")
	}
	return instance
}

func (tm *TxManager) Begin() string {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txID := fmt.Sprintf("tx_%d_%d", time.Now().UnixNano(), rand.Intn(1000))

	tm.activeTxs[txID] = &TransactionContext{
		ID:        txID,
		StartTime: time.Now(),
		Status:    TxStatusActive,
		Changes:   make([]WALEntry, 0),
	}

	return txID
}

func (tm *TxManager) AddChange(txID string, opType OpType, table string, data string, prevData string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tx, exists := tm.activeTxs[txID]
	if !exists {
		return fmt.Errorf("transaksi %s tidak ditemukan atau sudah berakhir", txID)
	}

	if tx.Status != TxStatusActive {
		return fmt.Errorf("transaksi %s tidak aktif", txID)
	}

	entry := WALEntry{
		TxID:      txID,
		Timestamp: time.Now(),
		Type:      opType,
		TableName: table,
		Data:      data,
		PrevData:  prevData, 
	}

	tx.Changes = append(tx.Changes, entry)
	return nil
}

func (tm *TxManager) Commit(txID string) error {
	tm.mu.Lock()
	
	tx, exists := tm.activeTxs[txID]
	if !exists {
		tm.mu.Unlock()
		return fmt.Errorf("transaksi %s tidak valid", txID)
	}

	if len(tx.Changes) == 0 {
		delete(tm.activeTxs, txID)
		tm.mu.Unlock()
		return nil 
	}

	if err := tm.writeLog(tx.Changes); err != nil {
		tm.mu.Unlock()
		return fmt.Errorf("gagal menulis WAL: %v", err)
	}


	if err := tm.applyToStorage(tx.Changes); err != nil {
		tm.mu.Unlock()
		return fmt.Errorf("gagal apply data: %v", err)
	}

	tx.Status = TxStatusCommitted
	delete(tm.activeTxs, txID)
	
	tm.mu.Unlock()
	return nil
}

func (tm *TxManager) Rollback(txID string) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	_, exists := tm.activeTxs[txID]
	if !exists {
		return fmt.Errorf("transaksi %s tidak valid", txID)
	}

	delete(tm.activeTxs, txID)
	return nil
}

func (tm *TxManager) writeLog(entries []WALEntry) error {
	f, err := os.OpenFile(tm.walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("gagal membuka wal file: %v", err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)

	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return fmt.Errorf("gagal encode wal entry: %v", err)
		}
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("gagal sync wal ke disk: %v", err)
	}

	return nil
}

func (tm *TxManager) applyToStorage(entries []WALEntry) error {
	for _, entry := range entries {
		var err error

		parts := strings.Split(entry.Data, "|")
		if len(parts) == 0 {
			return fmt.Errorf("format data korup (kosong)")
		}
		rowID := parts[0]

		switch entry.Type {
		case OpInsert:
			err = storage.CommitInsert(entry.TableName, entry.Data)
		case OpUpdate:
			err = storage.CommitUpdate(entry.TableName, rowID, entry.Data)
		case OpDelete:
			err = storage.CommitDelete(entry.TableName, rowID)
		default:
			return fmt.Errorf("operasi tidak dikenal: %s", entry.Type)
		}

		if err != nil {
			return fmt.Errorf("gagal apply %s ke tabel %s: %v", entry.Type, entry.TableName, err)
		}
	}
	return nil
}