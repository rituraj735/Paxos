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

const overridesPath = "config/overrides.json"

var shardOverride = make(map[int]int)
var mu sync.RWMutex

func ClusterOfItem(id int) int {

	mu.RLock()
	if cid, ok := shardOverride[id]; ok && cid != 0 {
		mu.RUnlock()
		return cid
	}
	mu.RUnlock()

	for cid, rng := range config.ClusterRanges {
		if id >= rng.Min && id <= rng.Max {
			return cid
		}
	}
	return 0
}

func SetAccountClusterOverride(id int, clusterID int) {
	mu.Lock()
	shardOverride[id] = clusterID
	mu.Unlock()
}

func LoadOverridesFromFile() error {
	path := filepath.Clean(overridesPath)
	b, err := os.ReadFile(path)
	if err != nil {

		return err
	}

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

func SaveOverridesToFile() error {
	path := filepath.Clean(overridesPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

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
