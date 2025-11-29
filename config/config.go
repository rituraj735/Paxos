// =======================================
// File: config/config.go
// Description: Cluster-wide constants such as replica counts, ports, timeouts, and IDs.
// =======================================
package config

const (
	// Topology
	NumNodes    = 9
	NumClusters = 3
	ClusterSize = 3

	// Accounts
	MinAccountID   = 1
	MaxAccountID   = 9000 // changed for testclient.go
	InitialBalance = 10

	// Paxos/Client timings (unchanged defaults)
	MajoritySize  = 3
	ClientTimeout = 4000
	LeaderTimeout = 3000
	PrepareDelay  = 500

	// Networking
	BaseNodePort   = 8001
	BaseClientPort = 9001

	// Data handling
	WipeDataOnBoot = true
)

var NodeAddresses = map[int]string{
	1: "localhost:8001",
	2: "localhost:8002",
	3: "localhost:8003",
	4: "localhost:8004",
	5: "localhost:8005",
	6: "localhost:8006",
	7: "localhost:8007",
	8: "localhost:8008",
	9: "localhost:8009",
}

// Legacy: keep present to satisfy client code compilation; now unused.
var ClientIDs = []string{}

// Logical cluster composition (metadata only in Phase 1)
var ClusterMembers = map[int][]int{
	1: {1, 2, 3},
	2: {4, 5, 6},
	3: {7, 8, 9},
}

// Range defines a half-closed numeric range [Min, Max].
type Range struct{ Min, Max int }

// ClusterRanges defines which account ID ranges belong to which cluster.
// Single source of truth for sharding in Phase 3+.
var ClusterRanges = map[int]Range{
	1: {Min: 1, Max: 3000},
	2: {Min: 3001, Max: 6000},
	3: {Min: 6001, Max: 9000},
}
