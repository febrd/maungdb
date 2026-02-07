package trigger

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/febrd/maungdb/engine/storage"
)

type TriggerAction struct {
	Name      string `json:"name"`
	Event     string 
	Table     string 
	ActionQL  string 
	CreatedAt string `json:"created_at"`
}

type TriggerManager struct {
	mu sync.RWMutex
}

var GlobalTriggerManager = &TriggerManager{}

func (tm *TriggerManager) SaveTrigger(dbName string, t TriggerAction) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	triggerDir := filepath.Join(storage.GetDBPathExplicit(dbName), "triggers")
	if _, err := os.Stat(triggerDir); os.IsNotExist(err) {
		os.MkdirAll(triggerDir, 0755)
	}

	filename := fmt.Sprintf("%s_%s_%s.json", t.Table, t.Event, t.Name)
	path := filepath.Join(triggerDir, filename)

	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}

func (tm *TriggerManager) GetTriggers(dbName, table, event string) ([]TriggerAction, error) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	triggerDir := filepath.Join(storage.GetDBPathExplicit(dbName), "triggers")
	if _, err := os.Stat(triggerDir); os.IsNotExist(err) {
		return []TriggerAction{}, nil 
	}

	files, err := os.ReadDir(triggerDir)
	if err != nil {
		return nil, err
	}

	var triggers []TriggerAction
	prefix := fmt.Sprintf("%s_%s_", table, strings.ToUpper(event))

	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix) && strings.HasSuffix(f.Name(), ".json") {
			path := filepath.Join(triggerDir, f.Name())
			content, err := os.ReadFile(path)
			if err == nil {
				var t TriggerAction
				if json.Unmarshal(content, &t) == nil {
					triggers = append(triggers, t)
				}
			}
		}
	}

	return triggers, nil
}