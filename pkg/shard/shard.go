// =======================================
// File: pkg/shard/shard.go
// Description: Shard management utilities for determining cluster ownership of data items.
// =======================================
// =======================================
// File: pkg/shard/shard.go
// Description: Sharding helpers to map account IDs to clusters.
// =======================================
package shard

import "multipaxos/rituraj735/config"

// ClusterOfItem returns which cluster an account ID belongs to, using
// config.ClusterRanges as the single source of truth.
func ClusterOfItem(id int) int {
    for cid, rng := range config.ClusterRanges {
        if id >= rng.Min && id <= rng.Max {
            return cid
        }
    }
    return 0 // unknown / out of range
}
