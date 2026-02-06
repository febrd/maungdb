package transaction

import (
	"time"
)

type TxStatus int

const (
	TxStatusActive    TxStatus = iota 
	TxStatusCommitted                 
	TxStatusRolledBack                
)

type OpType string

const (
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
)

type WALEntry struct {
	LSN       uint64    `json:"lsn"`       
	TxID      string    `json:"tx_id"`     
	Timestamp time.Time `json:"timestamp"` 
	Type      OpType    `json:"type"`      
	TableName string    `json:"table"`     
	Data      string    `json:"data"`      
	PrevData  string    `json:"prev_data"` 
}

type TransactionContext struct {
	ID        string
	StartTime time.Time
	Status    TxStatus
	Changes   []WALEntry 
}