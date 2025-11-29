// =======================================
// File: pkg/database/database.go
// Description: BoltDB-backed ledger and WAL for transaction execution and recovery.
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
    mu             sync.RWMutex
    bolt           *bbolt.DB
    bucketBalances []byte
    bucketMeta     []byte
    bucketWAL      []byte
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

// Close closes the Bolt database.
func (db *Database) Close() error {
    db.mu.Lock()
    defer db.mu.Unlock()
    if db.bolt != nil {
        err := db.bolt.Close()
        db.bolt = nil
        return err
    }
    return nil
}

// getBalanceInt returns the integer balance for an account inside a read tx.
// Callers outside of a Bolt Tx should prefer GetBalance.
func (db *Database) getBalanceInt(tx *bbolt.Tx, id int) int {
    b := tx.Bucket(db.bucketBalances)
    return decodeInt(b.Get([]byte(strconv.Itoa(id))))
}

// setBalanceInt sets the integer balance for an account inside a write tx.
func (db *Database) setBalanceInt(tx *bbolt.Tx, id int, val int) error {
    b := tx.Bucket(db.bucketBalances)
    return b.Put([]byte(strconv.Itoa(id)), encodeInt(val))
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
}

// ExecuteTransaction debits sender and credits receiver if funds exist.
func (db *Database) ExecuteTransaction(tx datatypes.Txn) (bool, string) {
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

// GetBalance returns the current balance for a client.
func (db *Database) GetBalance(clientID string) int {
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

// GetBalanceInt is a convenience wrapper to fetch an account balance as int
// outside of a Bolt transaction when the caller only has an int ID.
func (db *Database) GetBalanceInt(id int) int {
    return db.GetBalance(strconv.Itoa(id))
}

// PrintDB renders the database contents for a node.
func (db *Database) PrintDB(nodeID int) string {
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

// GetAllBalances returns a copy of all balances.
func (db *Database) GetAllBalances() map[string]int {
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
    return db.bolt.Update(func(tx *bbolt.Tx) error {
        b := tx.Bucket(db.bucketWAL)
        log.Printf("[WAL] append key=%s items=%d phase=%s", string(key), len(rec.Items), rec.Phase)
        return b.Put(key, val)
    })
}

// appendSingleItemWAL creates/overwrites a WAL record for txnID+phase with a
// single item entry (id, old, new) and current timestamp.
func (db *Database) appendSingleItemWAL(txnID string, phase datatypes.WALPhase, id int, oldBal, newBal int) error {
    rec := datatypes.WALRecord{
        TxnID:     txnID,
        Phase:     phase,
        Items:     []datatypes.WALItem{{ID: id, OldBalance: oldBal, NewBalance: newBal}},
        Timestamp: time.Now().UnixNano(),
    }
    return db.AppendWAL(rec)
}

// LoadWAL loads all WAL records from storage.
func (db *Database) LoadWAL() ([]datatypes.WALRecord, error) {
    out := make([]datatypes.WALRecord, 0)
    db.mu.RLock()
    defer db.mu.RUnlock()
    err := db.bolt.View(func(tx *bbolt.Tx) error {
        c := tx.Bucket(db.bucketWAL).Cursor()
        for _, v := c.First(); v != nil; _, v = c.Next() {
            var rec datatypes.WALRecord
            if err := json.Unmarshal(v, &rec); err != nil {
                return err
            }
            out = append(out, rec)
        }
        return nil
    })
    return out, err
}

// ApplyWALCommit ensures the DB matches the COMMIT record for txnID.
func (db *Database) ApplyWALCommit(txnID string) error {
    key := []byte(fmt.Sprintf("%s-%s", txnID, string(datatypes.WALCommit)))
    db.mu.Lock()
    defer db.mu.Unlock()
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

// UndoWAL restores OldBalance for a txn that only reached PREPARE.
func (db *Database) UndoWAL(txnID string) error {
    key := []byte(fmt.Sprintf("%s-%s", txnID, string(datatypes.WALPrepare)))
    db.mu.Lock()
    defer db.mu.Unlock()
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

// ClearWAL removes all WAL entries for txnID (any phase).
func (db *Database) ClearWAL(txnID string) error {
    prefix := []byte(fmt.Sprintf("%s-", txnID))
    db.mu.Lock()
    defer db.mu.Unlock()
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

// PromoteWALPrepareToCommit copies the WAL record with phase P (prepare) to a
// new record with phase C (commit), overwriting existing C if present.
func (db *Database) PromoteWALPrepareToCommit(txnID string) error {
    keyP := []byte(fmt.Sprintf("%s-%s", txnID, string(datatypes.WALPrepare)))
    keyC := []byte(fmt.Sprintf("%s-%s", txnID, string(datatypes.WALCommit)))
    db.mu.Lock()
    defer db.mu.Unlock()
    return db.bolt.Update(func(tx *bbolt.Tx) error {
        b := tx.Bucket(db.bucketWAL)
        v := b.Get(keyP)
        if v == nil {
            return nil
        }
        return b.Put(keyC, v)
    })
}

// CreditWithWAL applies a tentative credit to account id and writes WAL P.
// On success, the updated balance is visible immediately; the WAL is used to
// finalize (Promote+Apply) or undo on decision.
func (db *Database) CreditWithWAL(txnID string, id int, amount int) error {
    db.mu.Lock()
    defer db.mu.Unlock()
    return db.bolt.Update(func(tx *bbolt.Tx) error {
        oldBal := db.getBalanceInt(tx, id)
        newBal := oldBal + amount
        if err := db.setBalanceInt(tx, id, newBal); err != nil {
            return err
        }
        if err := db.appendSingleItemWAL(txnID, datatypes.WALPrepare, id, oldBal, newBal); err != nil {
            return err
        }
        log.Printf("[WAL] credit P tx=%s id=%d %d->%d", txnID, id, oldBal, newBal)
        return nil
    })
}

// DebitWithWAL applies a tentative debit to account id and writes WAL P.
// Returns error if insufficient funds.
func (db *Database) DebitWithWAL(txnID string, id int, amount int) error {
    db.mu.Lock()
    defer db.mu.Unlock()
    return db.bolt.Update(func(tx *bbolt.Tx) error {
        oldBal := db.getBalanceInt(tx, id)
        newBal := oldBal - amount
        if newBal < 0 {
            return fmt.Errorf("insufficient funds")
        }
        if err := db.setBalanceInt(tx, id, newBal); err != nil {
            return err
        }
        if err := db.appendSingleItemWAL(txnID, datatypes.WALPrepare, id, oldBal, newBal); err != nil {
            return err
        }
        log.Printf("[WAL] debit P tx=%s id=%d %d->%d", txnID, id, oldBal, newBal)
        return nil
    })
}
