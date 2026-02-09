// =======================================
// File: config/config.go
// Description: Cluster-wide constants such as replica counts, ports, timeouts, and IDs.
// =======================================
package config

const (
	NumNodes       = 5
	NumClients     = 10
	MajoritySize   = 3
	InitialBalance = 10

	ClientTimeout = 4000
	LeaderTimeout = 3000
	PrepareDelay  = 500

	BaseNodePort   = 8001
	BaseClientPort = 9001
)

var NodeAddresses = map[int]string{
	1: "localhost:8001",
	2: "localhost:8002",
	3: "localhost:8003",
	4: "localhost:8004",
	5: "localhost:8005",
}

var ClientIDs = []string{
	"A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
}
