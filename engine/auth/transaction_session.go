package auth

import (
	"sync"
)

var (
	sessionTxMap = make(map[string]string)
	sessionMu    sync.RWMutex
)

func SetSessionTxID(txID string) {
	user, err := CurrentUser()
	if err != nil {
		return 
	}

	sessionMu.Lock()
	defer sessionMu.Unlock()
	
	if txID == "" {
		delete(sessionTxMap, user.Username) 
	} else {
		sessionTxMap[user.Username] = txID  
	}
}

func GetSessionTxID() string {
	user, err := CurrentUser()
	if err != nil {
		return ""
	}

	sessionMu.RLock()
	defer sessionMu.RUnlock()
	
	return sessionTxMap[user.Username]
}