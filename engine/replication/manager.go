package replication

import (
	"errors"
	"fmt"
	"sync"
)

type NodeRole int

const (
	RoleMaster NodeRole = iota 
	RoleSlave            
)

type ReplicationManager struct {
	mu         sync.RWMutex
	CurrentRole NodeRole
	MasterHost  string
}

var GlobalReplication = &ReplicationManager{
	CurrentRole: RoleMaster,
}

func (rm *ReplicationManager) SetMaster() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.CurrentRole = RoleMaster
	rm.MasterHost = ""
	fmt.Println("👑 Node ieu ayeuna janten INDUNG (Read/Write Mode)")
}

func (rm *ReplicationManager) SetSlave(masterHost string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.CurrentRole = RoleSlave
	rm.MasterHost = masterHost
	fmt.Println("👶 Node ieu ayeuna janten ANAK (Read Only Mode)")
	fmt.Printf("📡 Ngintil ka Indung di: %s\n", masterHost)
}

func (rm *ReplicationManager) CanWrite() error {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	if rm.CurrentRole == RoleSlave {
		return errors.New("⛔ AKSES DITOLAK: Node ieu mangrupikeun ANAK (Slave). Ngan tiasa maca (Read-Only).")
	}
	return nil
}

func (rm *ReplicationManager) IsSlave() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.CurrentRole == RoleSlave
}