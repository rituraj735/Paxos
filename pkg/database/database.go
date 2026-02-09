// =======================================
// File: pkg/database/database.go
// Description: Thread-safe in-memory ledger supporting transaction execution and inspection.
// =======================================
package database

import (
	"fmt"
	"log"
	"multipaxos/rituraj735/datatypes"
	"sort"
	"strings"
	"sync"
)

type Database struct {
	Balances map[string]int
	mu       sync.RWMutex
}

// NewDatabase creates a new empty balance store.
func NewDatabase() *Database {
	log.Printf("[Database] initializing new in-memory store")
	return &Database{
		Balances: make(map[string]int),
	}
}

// InitializeClient seeds a client with a starting balance.
func (db *Database) InitializeClient(clientID string, balance int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Balances[clientID] = balance
	log.Printf("[Database] client %s initialized with balance %d", clientID, balance)
}

// ExecuteTransaction debits sender and credits receiver if funds exist.
func (db *Database) ExecuteTransaction(tx datatypes.Txn) (bool, string) {
	db.mu.Lock()
	defer db.mu.Unlock()
	log.Printf("[Database] executing txn %s", tx.String())

	senderBalance, senderExists := db.Balances[tx.Sender]
	if !senderExists {
		log.Printf("[Database] txn failed: sender %s missing", tx.Sender)
		return false, "sender does not exist"
	}

	if senderBalance < tx.Amount {
		log.Printf("[Database] txn failed: sender %s insufficient balance %d (amt %d)", tx.Sender, senderBalance, tx.Amount)
		return false, "insufficient balance"
	}

	if _, receiverExists := db.Balances[tx.Receiver]; !receiverExists {
		db.Balances[tx.Receiver] = 0
	}

	db.Balances[tx.Sender] -= tx.Amount
	db.Balances[tx.Receiver] += tx.Amount

	log.Printf("[Database] txn success: %s -> %s amount %d", tx.Sender, tx.Receiver, tx.Amount)
	return true, "success"
}

// GetBalance returns the current balance for a client.
func (db *Database) GetBalance(clientID string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	bal := db.Balances[clientID]
	log.Printf("[Database] queried balance %s=%d", clientID, bal)
	return bal
}

// PrintDB renders the database contents for a node.
func (db *Database) PrintDB(nodeID int) string {
	// db.mu.RLock()
	// defer db.mu.RUnlock()

	// fmt.Printf("=== Node %d Database State ===\n", nodeID)
	// clients := make([]string, 0, len(db.Balances))
	// for client := range db.Balances {
	// 	clients = append(clients, client)
	// }
	// sort.Strings(clients)

	// for _, client := range clients {
	// 	fmt.Printf("%s: %d\n", client, db.Balances[client])
	// }
	// fmt.Println()
	db.mu.RLock()
	defer db.mu.RUnlock()

	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("=== Node %d Database State ===\n", nodeID))

	clients := make([]string, 0, len(db.Balances))
	for client := range db.Balances {
		clients = append(clients, client)
	}
	sort.Strings(clients)

	for _, client := range clients {

		builder.WriteString(fmt.Sprintf("%s: %d\n", client, db.Balances[client]))
	}
	builder.WriteString("\n")

	report := builder.String()
	log.Printf("[Database] printing DB for node %d (%d accounts)", nodeID, len(clients))
	return report
}

// GetAllBalances returns a copy of all balances.
func (db *Database) GetAllBalances() map[string]int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	log.Printf("[Database] snapshotting %d balances", len(db.Balances))

	balances := make(map[string]int)
	for client, balance := range db.Balances {
		balances[client] = balance
	}
	return balances
}
