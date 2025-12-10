package config

import "strconv"

const (
	NumNodes    = 9
	NumClusters = 3
	ClusterSize = 3

	MinAccountID   = 1
	MaxAccountID   = 9000
	InitialBalance = 10

	MajoritySize  = 3
	ClientTimeout = 4000
	LeaderTimeout = 3000
	PrepareDelay  = 500

	BaseNodePort   = 8001
	BaseClientPort = 9001

	WipeDataOnBoot = true

	ReshardTopK = 10
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

var ClientIDs []string

func init() {
	total := MaxAccountID - MinAccountID + 1
	if total < 0 {
		total = 0
	}
	ClientIDs = make([]string, 0, total)
	for i := MinAccountID; i <= MaxAccountID; i++ {
		ClientIDs = append(ClientIDs, strconv.Itoa(i))
	}
}

var ClusterMembers = map[int][]int{
	1: {1, 2, 3},
	2: {4, 5, 6},
	3: {7, 8, 9},
}

type Range struct{ Min, Max int }

var ClusterRanges = map[int]Range{
	1: {Min: 1, Max: 3000},
	2: {Min: 3001, Max: 6000},
	3: {Min: 6001, Max: 9000},
}
