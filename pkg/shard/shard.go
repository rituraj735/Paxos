// =======================================
// File: pkg/shard/shard.go
// Description: Shard management utilities for determining cluster ownership of data items.
// =======================================
// =======================================
// File: pkg/shard/shard.go
// Description: Sharding helpers to map account IDs to clusters.
// =======================================
package shard

import (
    "encoding/json"
    "log"
    "multipaxos/rituraj735/config"
    "os"
    "path/filepath"
    "strconv"
    "sync"
)

// overridesPath is the JSON file where reshard overrides are persisted.
const overridesPath = "config/overrides.json"

// shardOverride maps accountID -> clusterID for override-based sharding.
var shardOverride = make(map[int]int)
var mu sync.RWMutex

// ClusterOfItem returns which cluster an account ID belongs to, using
// config.ClusterRanges as the single source of truth.
func ClusterOfItem(id int) int {
    // First, check override map
    mu.RLock()
    if cid, ok := shardOverride[id]; ok && cid != 0 {
        mu.RUnlock()
        return cid
    }
    mu.RUnlock()

    // Fallback to config ranges
    for cid, rng := range config.ClusterRanges {
        if id >= rng.Min && id <= rng.Max {
            return cid
        }
    }
    return 0 // unknown / out of range
}

// SetAccountClusterOverride sets/updates the override mapping for a single account.
// This only mutates the in-memory map. Call SaveOverridesToFile to persist.
func SetAccountClusterOverride(id int, clusterID int) {
    mu.Lock()
    shardOverride[id] = clusterID
    mu.Unlock()
}

// LoadOverridesFromFile loads overrides from disk and merges them into the in-memory map.
// It never clears existing entries; new entries overwrite existing keys.
func LoadOverridesFromFile() error {
    path := filepath.Clean(overridesPath)
    b, err := os.ReadFile(path)
    if err != nil {
        // Not fatal if file is missing or unreadable; propagate error to caller to log if desired.
        return err
    }
    // File format: map[string]int (accountID->clusterID)
    var disk map[string]int
    if err := json.Unmarshal(b, &disk); err != nil {
        return err
    }
    mu.Lock()
    for k, v := range disk {
        if id, convErr := strconv.Atoi(k); convErr == nil {
            shardOverride[id] = v
        }
    }
    mu.Unlock()
    log.Printf("[Shard] loaded %d override(s) from %s", len(disk), path)
    return nil
}

// SaveOverridesToFile persists the current in-memory override map to disk.
func SaveOverridesToFile() error {
    path := filepath.Clean(overridesPath)
    if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
        return err
    }
    // Serialize as map[string]int for readability
    mu.RLock()
    disk := make(map[string]int, len(shardOverride))
    for id, cid := range shardOverride {
        disk[strconv.Itoa(id)] = cid
    }
    mu.RUnlock()
    data, err := json.MarshalIndent(disk, "", "  ")
    if err != nil {
        return err
    }
    if err := os.WriteFile(path, data, 0o644); err != nil {
        return err
    }
    log.Printf("[Shard] saved %d override(s) to %s", len(disk), path)
    return nil
}
