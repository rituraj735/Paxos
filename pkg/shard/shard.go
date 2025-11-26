// =======================================
// File: pkg/shard/shard.go
// Description: Shard management utilities for determining cluster ownership of data items.
// =======================================
package shard

// ClusterOfItem returns which cluster owns a given account ID.
// Phase 1: simple static ranges.
func ClusterOfItem(id int) int {
	switch {
	case id >= 1 && id <= 3000:
		return 1
	case id >= 3001 && id <= 6000:
		return 2
	case id >= 6001 && id <= 9000:
		return 3
	default:
		return 0 // invalid
	}
}
