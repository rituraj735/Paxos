package database

import (
	"fmt"
	"multipaxos/rituraj735/datatypes"
	"sort"
	"strings"
	"sync"
)

type Database struct {
	Balances map[string]int
	mu       sync.RWMutex
}

func NewDatabase() *Database {
	return &Database{
		Balances: make(map[string]int),
	}
}

func (db *Database) InitializeClient(clientID string, balance int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.Balances[clientID] = balance
}

func (db *Database) ExecuteTransaction(tx datatypes.Txn) (bool, string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	senderBalance, senderExists := db.Balances[tx.Sender]
	if !senderExists {
		return false, "sender does not exist"
	}

	if senderBalance < tx.Amount {
		return false, "insufficient balance"
	}

	if _, receiverExists := db.Balances[tx.Receiver]; !receiverExists {
		db.Balances[tx.Receiver] = 0
	}

	db.Balances[tx.Sender] -= tx.Amount
	db.Balances[tx.Receiver] += tx.Amount

	return true, "success"
}

func (db *Database) GetBalance(clientID string) int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.Balances[clientID]
}

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

	// 1. Create a strings.Builder. This is like an efficient, mutable string.
	var builder strings.Builder

	// 2. Instead of printing, we write to the builder.
	//    fmt.Sprintf formats the string without printing it.
	builder.WriteString(fmt.Sprintf("=== Node %d Database State ===\n", nodeID))

	// The logic for sorting clients remains exactly the same.
	clients := make([]string, 0, len(db.Balances))
	for client := range db.Balances {
		clients = append(clients, client)
	}
	sort.Strings(clients)

	for _, client := range clients {
		// 3. Append each client's data to the builder.
		builder.WriteString(fmt.Sprintf("%s: %d\n", client, db.Balances[client]))
	}
	builder.WriteString("\n")

	// 4. Return the final, complete string from the builder.
	return builder.String()
}

func (db *Database) GetAllBalances() map[string]int {
	db.mu.RLock()
	defer db.mu.RUnlock()

	balances := make(map[string]int)
	for client, balance := range db.Balances {
		balances[client] = balance
	}
	return balances
}
