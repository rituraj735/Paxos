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
    "strconv"
    "sort"
    "strings"
    "sync"
    "time"

    "encoding/json"
    bbolt "go.etcd.io/bbolt"
)

type Database struct {
    Balances       map[string]int
    mu             sync.RWMutex
    bolt           *bbolt.DB
    bucketBalances []byte
    bucketMeta     []byte
    bucketWAL      []byte
    useBolt        bool
    // in-memory WAL when Bolt is not used (tests/fallback)
    walMem map[string][]byte
}

// NewDatabase creates a new empty balance store.
func NewDatabase() *Database {
    log.Printf("[Database] initializing new in-memory store")
    return &Database{
        Balances:       make(map[string]int),
        bucketBalances: []byte("balances"),
        bucketMeta:     []byte("meta"),
        useBolt:        false,
        bucketWAL:      []byte("wal"),
        walMem:         make(map[string][]byte),
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
        bucketWAL:      []byte("wal"),
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
        if _, e := tx.CreateBucketIfNotExists(d.bucketWAL); e != nil {
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

// ===================== WAL API (Phase 2) =====================

// AppendWAL appends/overwrites a WALRecord at key "<TxnID>-<Phase>".
func (db *Database) AppendWAL(rec datatypes.WALRecord) error {
    key := []byte(fmt.Sprintf("%s-%s", rec.TxnID, string(rec.Phase)))
    val, err := json.Marshal(rec)
    if err != nil {
        return err
    }
    db.mu.Lock()
    defer db.mu.Unlock()
    if db.useBolt {
        return db.bolt.Update(func(tx *bbolt.Tx) error {
            b := tx.Bucket(db.bucketWAL)
            log.Printf("[WAL] append key=%s items=%d phase=%s", string(key), len(rec.Items), rec.Phase)
            return b.Put(key, val)
        })
    }
    log.Printf("[WAL] append (mem) key=%s items=%d phase=%s", string(key), len(rec.Items), rec.Phase)
    db.walMem[string(key)] = val
    return nil
}

// LoadWAL loads all WAL records from storage.
func (db *Database) LoadWAL() ([]datatypes.WALRecord, error) {
    out := make([]datatypes.WALRecord, 0)
    db.mu.RLock()
    defer db.mu.RUnlock()
    if db.useBolt {
        err := db.bolt.View(func(tx *bbolt.Tx) error {
            c := tx.Bucket(db.bucketWAL).Cursor()
            for k, v := c.First(); k != nil; k, v = c.Next() {
                var rec datatypes.WALRecord
                if err := json.Unmarshal(v, &rec); err != nil {
                    return err
                }
                // Derive TxnID/Phase from key to be safe
                parts := strings.SplitN(string(k), "-", 2)
                if len(parts) == 2 {
                    rec.TxnID = parts[0]
                    rec.Phase = datatypes.WALPhase(parts[1])
                }
                out = append(out, rec)
            }
            return nil
        })
        return out, err
    }
    for k, v := range db.walMem {
        var rec datatypes.WALRecord
        if err := json.Unmarshal(v, &rec); err != nil {
            return nil, err
        }
        parts := strings.SplitN(k, "-", 2)
        if len(parts) == 2 {
            rec.TxnID = parts[0]
            rec.Phase = datatypes.WALPhase(parts[1])
        }
        out = append(out, rec)
    }
    return out, nil
}

// ApplyWALCommit ensures the DB matches the COMMIT record for txnID.
func (db *Database) ApplyWALCommit(txnID string) error {
    key := []byte(fmt.Sprintf("%s-%s", txnID, string(datatypes.WALCommit)))
    db.mu.Lock()
    defer db.mu.Unlock()
    if db.useBolt {
        return db.bolt.Update(func(tx *bbolt.Tx) error {
            wb := tx.Bucket(db.bucketWAL)
            recBytes := wb.Get(key)
            if recBytes == nil {
                return nil
            }
            var rec datatypes.WALRecord
            if err := json.Unmarshal(recBytes, &rec); err != nil {
                return err
            }
            bb := tx.Bucket(db.bucketBalances)
            for _, it := range rec.Items {
                if err := bb.Put([]byte(strconv.Itoa(it.ID)), encodeInt(it.NewBalance)); err != nil {
                    return err
                }
            }
            log.Printf("[WAL] apply commit tx=%s items=%d", txnID, len(rec.Items))
            return nil
        })
    }
    // mem path
    if recBytes, ok := db.walMem[fmt.Sprintf("%s-%s", txnID, string(datatypes.WALCommit))]; ok {
        var rec datatypes.WALRecord
        if err := json.Unmarshal(recBytes, &rec); err != nil {
            return err
        }
        for _, it := range rec.Items {
            db.Balances[strconv.Itoa(it.ID)] = it.NewBalance
        }
        log.Printf("[WAL] apply commit (mem) tx=%s items=%d", txnID, len(rec.Items))
    }
    return nil
}

// UndoWAL restores OldBalance for a txn that only reached PREPARE.
func (db *Database) UndoWAL(txnID string) error {
    key := []byte(fmt.Sprintf("%s-%s", txnID, string(datatypes.WALPrepare)))
    db.mu.Lock()
    defer db.mu.Unlock()
    if db.useBolt {
        return db.bolt.Update(func(tx *bbolt.Tx) error {
            wb := tx.Bucket(db.bucketWAL)
            recBytes := wb.Get(key)
            if recBytes == nil {
                return nil
            }
            var rec datatypes.WALRecord
            if err := json.Unmarshal(recBytes, &rec); err != nil {
                return err
            }
            bb := tx.Bucket(db.bucketBalances)
            for _, it := range rec.Items {
                if err := bb.Put([]byte(strconv.Itoa(it.ID)), encodeInt(it.OldBalance)); err != nil {
                    return err
                }
            }
            log.Printf("[WAL] undo prepare tx=%s items=%d", txnID, len(rec.Items))
            return nil
        })
    }
    if recBytes, ok := db.walMem[fmt.Sprintf("%s-%s", txnID, string(datatypes.WALPrepare))]; ok {
        var rec datatypes.WALRecord
        if err := json.Unmarshal(recBytes, &rec); err != nil {
            return err
        }
        for _, it := range rec.Items {
            db.Balances[strconv.Itoa(it.ID)] = it.OldBalance
        }
        log.Printf("[WAL] undo prepare (mem) tx=%s items=%d", txnID, len(rec.Items))
    }
    return nil
}

// ClearWAL removes all WAL entries for txnID (any phase).
func (db *Database) ClearWAL(txnID string) error {
    prefix := []byte(fmt.Sprintf("%s-", txnID))
    db.mu.Lock()
    defer db.mu.Unlock()
    if db.useBolt {
        return db.bolt.Update(func(tx *bbolt.Tx) error {
            b := tx.Bucket(db.bucketWAL)
            c := b.Cursor()
            for k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)); k, _ = c.Next() {
                if err := b.Delete(k); err != nil {
                    return err
                }
            }
            log.Printf("[WAL] clear tx=%s", txnID)
            return nil
        })
    }
    for k := range db.walMem {
        if strings.HasPrefix(k, fmt.Sprintf("%s-", txnID)) {
            delete(db.walMem, k)
        }
    }
    log.Printf("[WAL] clear (mem) tx=%s", txnID)
    return nil
}
