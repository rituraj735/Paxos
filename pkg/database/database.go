// =======================================
// File: pkg/database/database.go
// Description: Thread-safe in-memory ledger supporting transaction execution and inspection.
// =======================================
package database

import (
    "fmt"
    "log"
    "multipaxos/rituraj735/datatypes"
    "path/filepath"
    "sort"
    "strings"
    "sync"
    "time"

    bbolt "go.etcd.io/bbolt"
)

type Database struct {
    Balances       map[string]int
    mu             sync.RWMutex
    bolt           *bbolt.DB
    bucketBalances []byte
    bucketMeta     []byte
    useBolt        bool
}

// NewDatabase creates a new empty balance store.
func NewDatabase() *Database {
    log.Printf("[Database] initializing new in-memory store")
    return &Database{
        Balances:       make(map[string]int),
        bucketBalances: []byte("balances"),
        bucketMeta:     []byte("meta"),
        useBolt:        false,
    }
}

// NewBoltDatabase opens or creates a BoltDB-backed store at the given path.
func NewBoltDatabase(path string) (*Database, error) {
    log.Printf("[Database] opening BoltDB at %s", filepath.Clean(path))
    db, err := bbolt.Open(path, 0o644, &bbolt.Options{Timeout: 1 * time.Second})
    if err != nil {
        return nil, err
    }

    d := &Database{
        bolt:           db,
        bucketBalances: []byte("balances"),
        bucketMeta:     []byte("meta"),
        useBolt:        true,
    }

    // Ensure buckets exist
    if err := db.Update(func(tx *bbolt.Tx) error {
        if _, e := tx.CreateBucketIfNotExists(d.bucketBalances); e != nil {
            return e
        }
        if _, e := tx.CreateBucketIfNotExists(d.bucketMeta); e != nil {
            return e
        }
        return nil
    }); err != nil {
        _ = db.Close()
        return nil, err
    }
    return d, nil
}

// Close closes the Bolt database if in use.
func (db *Database) Close() error {
    db.mu.Lock()
    defer db.mu.Unlock()
    if db.useBolt && db.bolt != nil {
        err := db.bolt.Close()
        db.bolt = nil
        return err
    }
    return nil
}

func encodeInt(v int) []byte {
    // store as decimal string to keep it simple and human-readable in the DB
    return []byte(fmt.Sprintf("%d", v))
}

func decodeInt(b []byte) int {
    if len(b) == 0 {
        return 0
    }
    var x int
    _, _ = fmt.Sscanf(string(b), "%d", &x)
    return x
}

// InitializeClient seeds a client with a starting balance.
func (db *Database) InitializeClient(clientID string, balance int) {
    if db.useBolt {
        db.mu.Lock()
        defer db.mu.Unlock()
        _ = db.bolt.Update(func(tx *bbolt.Tx) error {
            b := tx.Bucket(db.bucketBalances)
            if b.Get([]byte(clientID)) == nil {
                if err := b.Put([]byte(clientID), encodeInt(balance)); err != nil {
                    return err
                }
                log.Printf("[Database] client %s initialized with balance %d (bolt)", clientID, balance)
            }
            return nil
        })
        return
    }
    db.mu.Lock()
    db.Balances[clientID] = balance
    db.mu.Unlock()
    log.Printf("[Database] client %s initialized with balance %d", clientID, balance)
}

// ExecuteTransaction debits sender and credits receiver if funds exist.
func (db *Database) ExecuteTransaction(tx datatypes.Txn) (bool, string) {
    if db.useBolt {
        db.mu.Lock()
        defer db.mu.Unlock()
        log.Printf("[Database] executing txn (bolt) %s", tx.String())
        var ok bool
        var msg string
        err := db.bolt.Update(func(txn *bbolt.Tx) error {
            b := txn.Bucket(db.bucketBalances)
            sb := decodeInt(b.Get([]byte(tx.Sender)))
            rb := decodeInt(b.Get([]byte(tx.Receiver)))
            if b.Get([]byte(tx.Sender)) == nil {
                msg = "sender does not exist"
                ok = false
                return nil
            }
            if sb < tx.Amount {
                msg = "insufficient balance"
                ok = false
                return nil
            }
            sb -= tx.Amount
            rb += tx.Amount
            if err := b.Put([]byte(tx.Sender), encodeInt(sb)); err != nil { return err }
            if err := b.Put([]byte(tx.Receiver), encodeInt(rb)); err != nil { return err }
            ok = true
            msg = "success"
            return nil
        })
        if err != nil {
            log.Printf("[Database] bolt update error: %v", err)
            return false, "internal error"
        }
        if ok {
            log.Printf("[Database] txn success (bolt): %s -> %s amount %d", tx.Sender, tx.Receiver, tx.Amount)
        } else {
            log.Printf("[Database] txn failed (bolt): %s — %s", tx.String(), msg)
        }
        return ok, msg
    }

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
    if db.useBolt {
        db.mu.RLock()
        defer db.mu.RUnlock()
        var bal int
        _ = db.bolt.View(func(tx *bbolt.Tx) error {
            b := tx.Bucket(db.bucketBalances)
            bal = decodeInt(b.Get([]byte(clientID)))
            return nil
        })
        log.Printf("[Database] queried balance (bolt) %s=%d", clientID, bal)
        return bal
    }
    db.mu.RLock()
    bal := db.Balances[clientID]
    db.mu.RUnlock()
    log.Printf("[Database] queried balance %s=%d", clientID, bal)
    return bal
}

// PrintDB renders the database contents for a node.
func (db *Database) PrintDB(nodeID int) string {
    if db.useBolt {
        db.mu.RLock()
        defer db.mu.RUnlock()
        var builder strings.Builder
        builder.WriteString(fmt.Sprintf("=== Node %d Database State ===\n", nodeID))
        var clients []string
        _ = db.bolt.View(func(tx *bbolt.Tx) error {
            c := tx.Bucket(db.bucketBalances).Cursor()
            for k, _ := c.First(); k != nil; k, _ = c.Next() {
                clients = append(clients, string(k))
            }
            return nil
        })
        sort.Strings(clients)
        _ = db.bolt.View(func(tx *bbolt.Tx) error {
            b := tx.Bucket(db.bucketBalances)
            for _, client := range clients {
                builder.WriteString(fmt.Sprintf("%s: %d\n", client, decodeInt(b.Get([]byte(client)))))
            }
            return nil
        })
        builder.WriteString("\n")
        report := builder.String()
        log.Printf("[Database] printing DB (bolt) for node %d (%d accounts)", nodeID, len(clients))
        return report
    }

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
    if db.useBolt {
        db.mu.RLock()
        defer db.mu.RUnlock()
        balances := make(map[string]int)
        _ = db.bolt.View(func(tx *bbolt.Tx) error {
            c := tx.Bucket(db.bucketBalances).Cursor()
            for k, v := c.First(); k != nil; k, v = c.Next() {
                balances[string(k)] = decodeInt(v)
            }
            return nil
        })
        log.Printf("[Database] snapshotting (bolt) %d balances", len(balances))
        return balances
    }
    db.mu.RLock()
    defer db.mu.RUnlock()
    log.Printf("[Database] snapshotting %d balances", len(db.Balances))
    balances := make(map[string]int)
    for client, balance := range db.Balances {
        balances[client] = balance
    }
    return balances
}
