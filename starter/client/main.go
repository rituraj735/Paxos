// =======================================
// File: starter/client/main.go
// Description: CLI driver that parses CSV test sets, orchestrates liveness changes, and submits txns.
// =======================================
package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"multipaxos/rituraj735/pkg/client"
	"multipaxos/rituraj735/pkg/shard"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TxnSet struct {
	SetNumber int
	Txns      []datatypes.Txn
	LiveNodes []int
}

var sets []TxnSet

var currentSetIndex int

var clients map[string]*client.Client
var clientsMu sync.Mutex

var backlog []datatypes.Txn

// Phase 8: track modified IDs per set and performance stats
var modifiedIDs map[int]bool

type PerfStats struct {
	TxnCount     int
	TotalLatency time.Duration
	StartWall    time.Time
	EndWall      time.Time
}

var perf PerfStats

// ========================
// Phase 10: TxnSample ring buffer (RW only)
// ========================

type txnPair struct{ S, R int }

type TxnSample struct {
	mu   sync.Mutex
	buf  []txnPair
	size int
	idx  int
	full bool
}

func newTxnSample(n int) *TxnSample { return &TxnSample{size: n, buf: make([]txnPair, n)} }

func (ts *TxnSample) record(s, r int) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.size == 0 {
		return
	}
	ts.buf[ts.idx] = txnPair{S: s, R: r}
	ts.idx = (ts.idx + 1) % ts.size
	if ts.idx == 0 {
		ts.full = true
	}
}

func (ts *TxnSample) snapshot() []txnPair {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if !ts.full {
		out := make([]txnPair, 0, ts.idx)
		out = append(out, ts.buf[:ts.idx]...)
		return out
	}
	out := make([]txnPair, 0, ts.size)
	out = append(out, ts.buf[ts.idx:]...)
	out = append(out, ts.buf[:ts.idx]...)
	return out
}

var txnSample = newTxnSample(1000)

// deferTxn queues a txn for later when quorum is unavailable.
func deferTxn(tx datatypes.Txn) {
	log.Printf("ClientDriver: deferring txn %s", tx.String())
	backlog = append(backlog, tx)
}

// flushBacklog retries deferred transactions against the cluster.
func flushBacklog() {
	if len(backlog) == 0 {
		return
	}
	log.Printf("ClientDriver: flushing backlog (%d txns)", len(backlog))

	//log.Printf("\n--- Returning %d deferred transactions to the backlog ---\n", len(backlog))

	i := 0
	for i < len(backlog) {
		tx := backlog[i]
		c, ok := clients[tx.Sender]
		if !ok {
			//log.Printf("Backlog: client %s not found; skipping\n", tx.Sender)
			i++
			continue
		}

		reply, err := c.SendTransaction(tx)
		if err != nil || !reply.Success {

			i++
			continue
		}

		backlog = append(backlog[:i], backlog[i+1:]...)
	}
}

// main boots the CLI, reads CSV input, and drives the menu loop.
func main() {
	logDir := "logs"
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		log.Fatalf("failed to create log directory: %v", err)
	}
	logPath := fmt.Sprintf("%s/client.log", logDir)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("failed to open log file %s: %v", logPath, err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[Client] ")

	// Load shard overrides at startup (non-fatal)
	_ = shard.LoadOverridesFromFile()

	// Flags for Phase 9 benchmark mode
	bench := flag.Bool("bench", false, "run benchmark mode and exit")
	modeFlag := flag.String("mode", "rw", "benchmark mode: rw or ro (time-based mode)")
	durationFlag := flag.Int("duration", 30, "benchmark duration in seconds (used if -txns is 0)")
	clientsFlag := flag.Int("clients", 4, "number of concurrent workers")
	amountFlag := flag.Int("amount", 1, "transfer amount for rw mode")
	crossRatioFlag := flag.Float64("crossRatio", 0.0, "fraction of cross-shard txns [0..1] (time-based mode)")
	hotKFlag := flag.Int("hotK", 100, "size of hot set for skew")
	hotProbFlag := flag.Float64("hotProb", 0.0, "probability of choosing from hot set [0..1]")
	seedFlag := flag.Int64("seed", 1, "RNG seed")
	sourceCIDFlag := flag.Int("sourceCID", 1, "cluster to choose senders from [1..3]")

	// Fixed-ops benchmark flags
	txnsFlag := flag.Int("txns", 0, "run a fixed total number of operations; overrides -duration when >0")
	rwPctFlag := flag.Int("rwPct", 100, "percentage of operations that are read-write [0..100] (fixed-ops mode)")
	crossPctFlag := flag.Int("crossPct", 0, "percentage of cross-shard among read-write [0..100] (fixed-ops mode)")
	presetFlag := flag.Int("preset", 0, "benchmark preset: 1, 2, or 3 (sets txns/rwPct/crossPct/skew)")
	flag.Parse()

	fmt.Println("Welcome to Bank of Paxos")
	fmt.Println("==========================")
	log.Printf("ClientDriver: started; expecting %d clients", len(config.ClientIDs))
	var option int
	var fileName string

	// Initializing 10 clients
	clients = make(map[string]*client.Client)
	for _, clientID := range config.ClientIDs {
		clients[clientID] = client.NewClient(clientID, config.NodeAddresses)
	}

	time.Sleep(1 * time.Second) //Just waiting for clients to initialize and come up
	fmt.Println("All 10 clients are ready")

	// If -bench is set, run benchmark flow and exit
	if *bench {
		// Optional presets per assignment
		if *presetFlag == 1 {
			*txnsFlag = 200
			*rwPctFlag = 80
			*crossPctFlag = 10
			*hotProbFlag = 0.0 // uniform
			*modeFlag = "rw"
		} else if *presetFlag == 2 {
			*txnsFlag = 2000
			*rwPctFlag = 80
			*crossPctFlag = 20
			*hotProbFlag = 0.95 // highly skewed
			*hotKFlag = 100
			*modeFlag = "rw"
		} else if *presetFlag == 3 {
			*txnsFlag = 30000
			*rwPctFlag = 100
			*crossPctFlag = 0
			*hotProbFlag = 0.0 // uniform
			*modeFlag = "rw"
		}

		if *txnsFlag > 0 {
			if *clientsFlag <= 0 {
				*clientsFlag = config.NumNodes
			}
			if *rwPctFlag < 0 {
				*rwPctFlag = 0
			} else if *rwPctFlag > 100 {
				*rwPctFlag = 100
			}
			if *crossPctFlag < 0 {
				*crossPctFlag = 0
			} else if *crossPctFlag > 100 {
				*crossPctFlag = 100
			}
			if err := runBenchmarkByOps(*txnsFlag, *clientsFlag, *amountFlag, *rwPctFlag, *crossPctFlag, *hotKFlag, *hotProbFlag, *seedFlag, *sourceCIDFlag); err != nil {
				log.Fatalf("Benchmark (fixed-ops) failed: %v", err)
			}
			return
		}

		if err := runBenchmark(*modeFlag, *durationFlag, *clientsFlag, *amountFlag, *crossRatioFlag, *hotKFlag, *hotProbFlag, *seedFlag, *sourceCIDFlag); err != nil {
			log.Fatalf("Benchmark failed: %v", err)
		}
		return
	}

	// Reading CSV file path from user (interactive mode)
	fmt.Println("Please enter the test file path to start: ")
	filePathReader := bufio.NewReader(os.Stdin)
	filePath, err := filePathReader.ReadString('\n')
	if err != nil {
		log.Fatalf("Failed to read file path: %v", err)
	}
	filePath = strings.TrimSpace(filePath)
	sets, err = ParseTxnSetsFromCSV(filePath)
	if err != nil {
		log.Fatalf("Failed to parse CSV file: %v", err)
	}
	log.Printf("ClientDriver: loaded %d transaction sets from %s", len(sets), filePath)

	//write the logic of after reading the file here later
	fmt.Scanln(&fileName)
	fmt.Println("Choose an option:")
	for {
		if option == 8 {
			break
		}
		fmt.Println("1.Process next transactions set")
		fmt.Println("2.PrintLog")
		fmt.Println("3.PrintDB (modified keys on all nodes)")
		fmt.Println("4.PrintStatus")
		fmt.Println("5.PrintView")
		fmt.Println("6.PrintBalance")
		fmt.Println("7.PrintReshard")
		fmt.Println("8.Exit")
		fmt.Println("9.Performance (throughput & latency)")
		fmt.Scanln(&option)
		fmt.Println("You chose option:", option)
		switch option {
		case 1:
			processNextTestSet(filePathReader)
		case 2:
			printLogFromNode(filePathReader)
		case 3:
			printModifiedBalancesAllNodes()
		case 4:
			printStatusFromNode(filePathReader)
		case 5:
			printViewFromAllNodes()
		case 6:
			printBalanceFromCluster(filePathReader)
		case 7:
			runReshard()
		case 8:
			fmt.Println("Exiting...")
			return
		case 9:
			printPerformance()
		default:
			fmt.Println("Invalid option. Please try again.")
		}
	}

}

// ========================
// Phase 9: Benchmark Mode
// ========================

type benchWorkerStats struct {
	totalOps     int64
	successOps   int64
	abortOps     int64
	errorOps     int64
	totalLatency time.Duration
}

func runBenchmark(mode string, durationSec int, workers int, amount int, crossRatio float64, hotK int, hotProb float64, seed int64, sourceCID int) error {
	// Validate flags
	if mode != "rw" && mode != "ro" {
		return fmt.Errorf("invalid -mode: %s", mode)
	}
	if durationSec <= 0 {
		return fmt.Errorf("duration must be > 0")
	}
	if workers <= 0 {
		workers = 1
	}
	if crossRatio < 0.0 {
		crossRatio = 0.0
	}
	if crossRatio > 1.0 {
		crossRatio = 1.0
	}
	if hotProb < 0.0 {
		hotProb = 0.0
	}
	if hotProb > 1.0 {
		hotProb = 1.0
	}
	if sourceCID < 1 || sourceCID > 3 {
		sourceCID = 1
	}

	// 1) Flush state on all nodes
	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		addr := config.NodeAddresses[nodeID]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var fr datatypes.FlushStateReply
				_ = c.Call("NodeService.FlushState", datatypes.FlushStateArgs{ResetDB: true, ResetConsensus: true, ResetWAL: true}, &fr)
			}
		}(addr)
	}
	time.Sleep(300 * time.Millisecond)

	// 2) Prime leaders on cluster anchors (1,4,7)
	for _, nid := range []int{1, 4, 7} {
		addr := config.NodeAddresses[nid]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var trep datatypes.TriggerElectionReply
				_ = c.Call("NodeService.ForceLeader", datatypes.TriggerElectionArgs{}, &trep)
			}
		}(addr)
	}
	time.Sleep(500 * time.Millisecond)

	// 3) Prepare ranges and helpers
	clusterRanges := config.ClusterRanges
	pickInRange := func(rng *rand.Rand, lo, hi int) int {
		if hi < lo {
			return lo
		}
		return lo + rng.Intn(hi-lo+1)
	}
	clamp := func(x, lo, hi int) int {
		if x < lo {
			return lo
		}
		if x > hi {
			return hi
		}
		return x
	}

	// leader cache per cluster
	var cacheMu sync.Mutex
	leaderCache := make(map[int]int)

	getClusterLeader := func(clusterID int, rng *rand.Rand) (int, string, error) {
		members, ok := config.ClusterMembers[clusterID]
		if !ok || len(members) == 0 {
			return 0, "", fmt.Errorf("no members for cluster %d", clusterID)
		}
		// choose a candidate: cached or the first member
		cacheMu.Lock()
		cand := leaderCache[clusterID]
		cacheMu.Unlock()
		if cand == 0 {
			cand = members[0]
		}

		queryLeader := func(nodeID int) (int, error) {
			addr := config.NodeAddresses[nodeID]
			cli, err := rpc.Dial("tcp", addr)
			if err != nil {
				return 0, err
			}
			defer cli.Close()
			var info datatypes.LeaderInfo
			if err := cli.Call("NodeService.GetLeader", true, &info); err != nil {
				return 0, err
			}
			if info.IsLeader && info.LeaderID != 0 {
				return info.LeaderID, nil
			}
			return 0, fmt.Errorf("no leader from node %d", nodeID)
		}

		if lid, err := queryLeader(cand); err == nil {
			cacheMu.Lock()
			leaderCache[clusterID] = lid
			cacheMu.Unlock()
			return lid, config.NodeAddresses[lid], nil
		}
		// try a different random member once
		alt := cand
		for tries := 0; tries < 5 && alt == cand; tries++ {
			alt = members[rng.Intn(len(members))]
		}
		if lid, err := queryLeader(alt); err == nil {
			cacheMu.Lock()
			leaderCache[clusterID] = lid
			cacheMu.Unlock()
			return lid, config.NodeAddresses[lid], nil
		}
		return 0, "", fmt.Errorf("leader discovery failed for cluster %d", clusterID)
	}

	// sender sampler for sourceCID
	senderSampler := func(rng *rand.Rand) int {
		rngDef, ok := clusterRanges[sourceCID]
		if !ok {
			return 1
		}
		if hotProb <= 0.0 || hotK <= 0 {
			return pickInRange(rng, rngDef.Min, rngDef.Max)
		}
		size := rngDef.Max - rngDef.Min + 1
		hk := clamp(hotK, 1, size)
		if rng.Float64() < hotProb {
			// hot set
			return pickInRange(rng, rngDef.Min, rngDef.Min+hk-1)
		}
		// cold set
		coldLo := rngDef.Min + hk
		if coldLo > rngDef.Max {
			coldLo = rngDef.Min
		}
		return pickInRange(rng, coldLo, rngDef.Max)
	}

	// receiver sampler based on sender and cross/intra choice
	receiverSampler := func(rng *rand.Rand, senderID int) (int, int) {
		sCID := shard.ClusterOfItem(senderID)
		targetCID := sCID
		if mode == "rw" && rng.Float64() < crossRatio {
			// pick a different cluster uniformly
			options := []int{}
			for cid := 1; cid <= config.NumClusters; cid++ {
				if cid != sCID {
					options = append(options, cid)
				}
			}
			targetCID = options[rng.Intn(len(options))]
		}
		rngDef, ok := clusterRanges[targetCID]
		if !ok {
			return 0, targetCID
		}
		if hotProb <= 0.0 || hotK <= 0 {
			return pickInRange(rng, rngDef.Min, rngDef.Max), targetCID
		}
		size := rngDef.Max - rngDef.Min + 1
		hk := clamp(hotK, 1, size)
		if rng.Float64() < hotProb {
			return pickInRange(rng, rngDef.Min, rngDef.Min+hk-1), targetCID
		}
		coldLo := rngDef.Min + hk
		if coldLo > rngDef.Max {
			coldLo = rngDef.Min
		}
		return pickInRange(rng, coldLo, rngDef.Max), targetCID
	}

	// worker function
	doRW := func(rng *rand.Rand, wid int, end time.Time) benchWorkerStats {
		st := benchWorkerStats{}
		seq := int64(0)
		for time.Now().Before(end) {
			sid := senderSampler(rng)
			rid, _ := receiverSampler(rng, sid)
			sCID := shard.ClusterOfItem(sid)
			lid, addr, err := getClusterLeader(sCID, rng)
			if err != nil || lid == 0 || addr == "" {
				st.totalOps++
				st.errorOps++
				continue
			}
			req := datatypes.ClientRequest{
				MessageType: "REQUEST",
				Transaction: datatypes.Txn{Sender: fmt.Sprintf("%d", sid), Receiver: fmt.Sprintf("%d", rid), Amount: amount},
				Timestamp:   time.Now().UnixNano(),
				ClientID:    fmt.Sprintf("bench-%d-%d", wid, seq),
				IsNoOp:      false,
			}
			seq++
			args := datatypes.ClientRequestRPC{Request: req}
			var rep datatypes.ClientReplyRPC
			t0 := time.Now()
			cli, err := rpc.Dial("tcp", addr)
			if err != nil {
				st.totalOps++
				st.errorOps++
				continue
			}
			done := make(chan error, 1)
			go func() { done <- cli.Call("NodeService.HandleClientRequest", args, &rep) }()
			callTimeout := time.Duration(config.ClientTimeout) * time.Millisecond
			var callErr error
			select {
			case callErr = <-done:
			case <-time.After(callTimeout):
				callErr = fmt.Errorf("timeout")
			}
			cli.Close()
			t1 := time.Now()

			st.totalOps++
			st.totalLatency += t1.Sub(t0)
			if callErr != nil {
				st.errorOps++
				continue
			}
			// classify reply
			msg := strings.ToLower(rep.Reply.Message)
			if rep.Reply.Success {
				st.successOps++
			} else if strings.Contains(msg, "locked") || strings.Contains(msg, "insufficient") || strings.Contains(msg, "abort") {
				// contentions/logic
				st.abortOps++
			} else if strings.Contains(msg, "insufficient active nodes") || strings.Contains(msg, "not leader") || strings.Contains(msg, "consensus") {
				st.errorOps++
			} else {
				// default to error if unexpected
				st.errorOps++
			}
		}
		return st
	}

	doRO := func(rng *rand.Rand, wid int, end time.Time) benchWorkerStats {
		st := benchWorkerStats{}
		for time.Now().Before(end) {
			sid := senderSampler(rng) // target account for read
			sCID := shard.ClusterOfItem(sid)
			_, addr, err := getClusterLeader(sCID, rng)
			if err != nil || addr == "" {
				st.totalOps++
				st.errorOps++
				continue
			}
			t0 := time.Now()
			cli, err := rpc.Dial("tcp", addr)
			if err != nil {
				st.totalOps++
				st.errorOps++
				continue
			}
			var gbr datatypes.GetBalanceReply
			callErr := cli.Call("NodeService.GetBalance", datatypes.GetBalanceArgs{AccountID: fmt.Sprintf("%d", sid)}, &gbr)
			cli.Close()
			t1 := time.Now()
			st.totalOps++
			st.totalLatency += t1.Sub(t0)
			if callErr != nil {
				st.errorOps++
			} else {
				// success read
				st.successOps++
			}
		}
		return st
	}

	// 4) run workers
	end := time.Now().Add(time.Duration(durationSec) * time.Second)
	startWall := time.Now()
	var wg sync.WaitGroup
	stats := make([]benchWorkerStats, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			wrng := rand.New(rand.NewSource(seed + int64(idx) + 1000))
			if mode == "rw" {
				stats[idx] = doRW(wrng, idx, end)
			} else {
				stats[idx] = doRO(wrng, idx, end)
			}
		}()
	}
	wg.Wait()
	endWall := time.Now()

	// 5) aggregate
	var agg benchWorkerStats
	for i := 0; i < workers; i++ {
		agg.totalOps += stats[i].totalOps
		agg.successOps += stats[i].successOps
		agg.abortOps += stats[i].abortOps
		agg.errorOps += stats[i].errorOps
		agg.totalLatency += stats[i].totalLatency
	}
	wallSecs := endWall.Sub(startWall).Seconds()
	tps := 0.0
	avgLatMs := 0.0
	if wallSecs > 0 {
		tps = float64(agg.totalOps) / wallSecs
	}
	if agg.totalOps > 0 {
		avgLatMs = (float64(agg.totalLatency.Milliseconds())) / float64(agg.totalOps)
	}

	// 6) report
	fmt.Printf("mode=%s dur=%d clients=%d cross=%.2f hotProb=%.2f\n", mode, durationSec, workers, crossRatio, hotProb)
	fmt.Printf("ops=%d tps=%.1f success=%d abort=%d error=%d avgLat=%.1fms\n",
		agg.totalOps, tps, agg.successOps, agg.abortOps, agg.errorOps, avgLatMs)

	return nil
}

// runBenchmarkByOps executes a fixed total number of operations across workers.
// It supports a mix of read-write and read-only ops via rwPct, and applies
// cross-shard selection for RW ops via crossPct. Distribution skew is controlled
// with hotK/hotProb similar to time-based benchmark.
func runBenchmarkByOps(totalOps int, workers int, amount int, rwPct int, crossPct int, hotK int, hotProb float64, seed int64, sourceCID int) error {
	if totalOps <= 0 {
		return fmt.Errorf("txns must be > 0")
	}
	if workers <= 0 {
		workers = 1
	}
	if rwPct < 0 {
		rwPct = 0
	} else if rwPct > 100 {
		rwPct = 100
	}
	if crossPct < 0 {
		crossPct = 0
	} else if crossPct > 100 {
		crossPct = 100
	}
	if hotProb < 0.0 {
		hotProb = 0.0
	} else if hotProb > 1.0 {
		hotProb = 1.0
	}
	if sourceCID < 1 || sourceCID > 3 {
		sourceCID = 1
	}

	crossRatio := float64(crossPct) / 100.0

	// 1) Flush state on all nodes
	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		addr := config.NodeAddresses[nodeID]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var fr datatypes.FlushStateReply
				_ = c.Call("NodeService.FlushState", datatypes.FlushStateArgs{ResetDB: true, ResetConsensus: true, ResetWAL: true}, &fr)
			}
		}(addr)
	}
	time.Sleep(300 * time.Millisecond)

	// 2) Prime leaders on cluster anchors (1,4,7)
	for _, nid := range []int{1, 4, 7} {
		addr := config.NodeAddresses[nid]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var trep datatypes.TriggerElectionReply
				_ = c.Call("NodeService.ForceLeader", datatypes.TriggerElectionArgs{}, &trep)
			}
		}(addr)
	}
	time.Sleep(500 * time.Millisecond)

	// 3) Helpers for ID sampling and leader discovery
	clusterRanges := config.ClusterRanges
	pickInRange := func(rng *rand.Rand, lo, hi int) int {
		if hi < lo {
			return lo
		}
		return lo + rng.Intn(hi-lo+1)
	}
	clamp := func(x, lo, hi int) int {
		if x < lo {
			return lo
		}
		if x > hi {
			return hi
		}
		return x
	}

	var cacheMu sync.Mutex
	leaderCache := make(map[int]int)
	getClusterLeader := func(clusterID int, rng *rand.Rand) (int, string, error) {
		members, ok := config.ClusterMembers[clusterID]
		if !ok || len(members) == 0 {
			return 0, "", fmt.Errorf("no members for cluster %d", clusterID)
		}
		cacheMu.Lock()
		cand := leaderCache[clusterID]
		cacheMu.Unlock()
		if cand == 0 {
			cand = members[0]
		}
		queryLeader := func(nodeID int) (int, error) {
			addr := config.NodeAddresses[nodeID]
			cli, err := rpc.Dial("tcp", addr)
			if err != nil {
				return 0, err
			}
			defer cli.Close()
			var info datatypes.LeaderInfo
			if err := cli.Call("NodeService.GetLeader", true, &info); err != nil {
				return 0, err
			}
			if info.IsLeader && info.LeaderID != 0 {
				return info.LeaderID, nil
			}
			return 0, fmt.Errorf("no leader from node %d", nodeID)
		}
		if lid, err := queryLeader(cand); err == nil {
			cacheMu.Lock()
			leaderCache[clusterID] = lid
			cacheMu.Unlock()
			return lid, config.NodeAddresses[lid], nil
		}
		alt := cand
		for tries := 0; tries < 5 && alt == cand; tries++ {
			alt = members[rng.Intn(len(members))]
		}
		if lid, err := queryLeader(alt); err == nil {
			cacheMu.Lock()
			leaderCache[clusterID] = lid
			cacheMu.Unlock()
			return lid, config.NodeAddresses[lid], nil
		}
		return 0, "", fmt.Errorf("leader discovery failed for cluster %d", clusterID)
	}

	senderSampler := func(rng *rand.Rand) int {
		rngDef, ok := clusterRanges[sourceCID]
		if !ok {
			return 1
		}
		if hotProb <= 0.0 || hotK <= 0 {
			return pickInRange(rng, rngDef.Min, rngDef.Max)
		}
		size := rngDef.Max - rngDef.Min + 1
		hk := clamp(hotK, 1, size)
		if rng.Float64() < hotProb {
			return pickInRange(rng, rngDef.Min, rngDef.Min+hk-1)
		}
		coldLo := rngDef.Min + hk
		if coldLo > rngDef.Max {
			coldLo = rngDef.Min
		}
		return pickInRange(rng, coldLo, rngDef.Max)
	}

	receiverSamplerRW := func(rng *rand.Rand, senderID int) (int, int) {
		sCID := shard.ClusterOfItem(senderID)
		targetCID := sCID
		if rng.Float64() < crossRatio {
			// choose a different target cluster uniformly
			options := []int{}
			for cid := 1; cid <= config.NumClusters; cid++ {
				if cid != sCID {
					options = append(options, cid)
				}
			}
			targetCID = options[rng.Intn(len(options))]
		}
		rngDef, ok := clusterRanges[targetCID]
		if !ok {
			return 0, targetCID
		}
		if hotProb <= 0.0 || hotK <= 0 {
			return pickInRange(rng, rngDef.Min, rngDef.Max), targetCID
		}
		size := rngDef.Max - rngDef.Min + 1
		hk := clamp(hotK, 1, size)
		if rng.Float64() < hotProb {
			return pickInRange(rng, rngDef.Min, rngDef.Min+hk-1), targetCID
		}
		coldLo := rngDef.Min + hk
		if coldLo > rngDef.Max {
			coldLo = rngDef.Min
		}
		return pickInRange(rng, coldLo, rngDef.Max), targetCID
	}

	// 4) run workers for fixed ops
	var done int64
	var wg sync.WaitGroup
	stats := make([]benchWorkerStats, workers)
	startWall := time.Now()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		idx := i
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(idx) + 5000))
			st := benchWorkerStats{}

			for {
				cur := atomic.AddInt64(&done, 1)
				if cur > int64(totalOps) {
					break
				}

				// Decide RW vs RO per op
				if rng.Intn(100) < rwPct {
					// RW op
					sid := senderSampler(rng)
					rid, _ := receiverSamplerRW(rng, sid)
					sCID := shard.ClusterOfItem(sid)
					_, addr, err := getClusterLeader(sCID, rng)
					if err != nil || addr == "" {
						st.totalOps++
						st.errorOps++
						continue
					}
					// record for resharding heuristic
					txnSample.record(sid, rid)
					req := datatypes.ClientRequest{
						MessageType: "REQUEST",
						Transaction: datatypes.Txn{Sender: fmt.Sprintf("%d", sid), Receiver: fmt.Sprintf("%d", rid), Amount: amount},
						Timestamp:   time.Now().UnixNano(),
						ClientID:    fmt.Sprintf("benchfx-%d-%d", idx, cur),
						IsNoOp:      false,
					}
					args := datatypes.ClientRequestRPC{Request: req}
					var rep datatypes.ClientReplyRPC
					t0 := time.Now()
					cli, err := rpc.Dial("tcp", addr)
					if err != nil {
						st.totalOps++
						st.errorOps++
						continue
					}
					doneCh := make(chan error, 1)
					go func() { doneCh <- cli.Call("NodeService.HandleClientRequest", args, &rep) }()
					callTimeout := time.Duration(config.ClientTimeout) * time.Millisecond
					var callErr error
					select {
					case callErr = <-doneCh:
					case <-time.After(callTimeout):
						callErr = fmt.Errorf("timeout")
					}
					cli.Close()
					t1 := time.Now()
					st.totalOps++
					st.totalLatency += t1.Sub(t0)
					if callErr != nil {
						st.errorOps++
						continue
					}
					msg := strings.ToLower(rep.Reply.Message)
					if rep.Reply.Success {
						st.successOps++
					} else if strings.Contains(msg, "locked") || strings.Contains(msg, "insufficient") || strings.Contains(msg, "abort") {
						st.abortOps++
					} else if strings.Contains(msg, "insufficient active nodes") || strings.Contains(msg, "not leader") || strings.Contains(msg, "consensus") {
						st.errorOps++
					} else {
						st.errorOps++
					}
				} else {
					// RO op
					sid := senderSampler(rng)
					sCID := shard.ClusterOfItem(sid)
					_, addr, err := getClusterLeader(sCID, rng)
					if err != nil || addr == "" {
						st.totalOps++
						st.errorOps++
						continue
					}
					t0 := time.Now()
					cli, err := rpc.Dial("tcp", addr)
					if err != nil {
						st.totalOps++
						st.errorOps++
						continue
					}
					var gbr datatypes.GetBalanceReply
					callErr := cli.Call("NodeService.GetBalance", datatypes.GetBalanceArgs{AccountID: fmt.Sprintf("%d", sid)}, &gbr)
					cli.Close()
					t1 := time.Now()
					st.totalOps++
					st.totalLatency += t1.Sub(t0)
					if callErr != nil {
						st.errorOps++
					} else {
						st.successOps++
					}
				}
			}

			stats[idx] = st
		}()
	}
	wg.Wait()
	endWall := time.Now()

	// 5) aggregate
	var agg benchWorkerStats
	for i := 0; i < workers; i++ {
		agg.totalOps += stats[i].totalOps
		agg.successOps += stats[i].successOps
		agg.abortOps += stats[i].abortOps
		agg.errorOps += stats[i].errorOps
		agg.totalLatency += stats[i].totalLatency
	}
	wallSecs := endWall.Sub(startWall).Seconds()
	tps := 0.0
	avgLatMs := 0.0
	if wallSecs > 0 {
		tps = float64(agg.totalOps) / wallSecs
	}
	if agg.totalOps > 0 {
		avgLatMs = float64(agg.totalLatency.Milliseconds()) / float64(agg.totalOps)
	}

	// 6) report
	fmt.Printf("mode=fixed-ops txns=%d clients=%d rwPct=%d crossPct=%d hotProb=%.2f\n", totalOps, workers, rwPct, crossPct, hotProb)
	fmt.Printf("ops=%d tps=%.1f success=%d abort=%d error=%d avgLat=%.1fms\n", agg.totalOps, tps, agg.successOps, agg.abortOps, agg.errorOps, avgLatMs)

	return nil
}

// ========================
// Phase 10: Resharding
// ========================

type move struct {
	ID, OldCID, NewCID int
	Score              int
}

// buildReshardMoves computes candidate moves using the TxnSample heuristic.
func buildReshardMoves() []move {
	pairs := txnSample.snapshot()
	// counts[account][cluster] -> frequency
	counts := make(map[int]map[int]int)
	totals := make(map[int]int)

	// For each transaction, attribute interactions to both participants
	for _, p := range pairs {
		sPeerCID := shard.ClusterOfItem(p.R)
		rPeerCID := shard.ClusterOfItem(p.S)
		if sPeerCID != 0 {
			if counts[p.S] == nil {
				counts[p.S] = make(map[int]int)
			}
			counts[p.S][sPeerCID]++
			totals[p.S]++
		}
		if rPeerCID != 0 {
			if counts[p.R] == nil {
				counts[p.R] = make(map[int]int)
			}
			counts[p.R][rPeerCID]++
			totals[p.R]++
		}
	}

	moves := make([]move, 0)
	for id, per := range counts {
		// pick best cluster by frequency
		bestCID, bestScore := 0, 0
		for cid, c := range per {
			if c > bestScore {
				bestCID, bestScore = cid, c
			}
		}
		if bestCID == 0 {
			continue
		}
		oldCID := shard.ClusterOfItem(id)
		if oldCID == 0 || oldCID == bestCID {
			continue
		}
		moves = append(moves, move{ID: id, OldCID: oldCID, NewCID: bestCID, Score: totals[id]})
	}
	// sort by Score desc
	sort.Slice(moves, func(i, j int) bool { return moves[i].Score > moves[j].Score })
	// cap at config.ReshardTopK
	if len(moves) > config.ReshardTopK {
		moves = moves[:config.ReshardTopK]
	}
	return moves
}

// findClusterLeader returns (nodeID, address) for a cluster leader by probing members.
func findClusterLeader(clusterID int) (int, string, error) {
	members, ok := config.ClusterMembers[clusterID]
	if !ok || len(members) == 0 {
		return 0, "", fmt.Errorf("no members for cluster %d", clusterID)
	}
	for _, nid := range members {
		addr := config.NodeAddresses[nid]
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			continue
		}
		var info datatypes.LeaderInfo
		_ = c.Call("NodeService.GetLeader", true, &info)
		c.Close()
		if info.IsLeader && info.LeaderID != 0 {
			return info.LeaderID, config.NodeAddresses[info.LeaderID], nil
		}
	}
	return 0, "", fmt.Errorf("no leader for cluster %d", clusterID)
}

func adminGetBalance(addr string, id int) (int, error) {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	var rep datatypes.AdminGetBalanceReply
	err = c.Call("NodeService.AdminGetBalance", datatypes.AdminGetBalanceArgs{AccountID: fmt.Sprintf("%d", id)}, &rep)
	if err != nil {
		return 0, err
	}
	if !rep.Ok {
		return 0, fmt.Errorf("admin get failed")
	}
	return rep.Balance, nil
}

func adminSetBalance(addr string, id int, bal int) error {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	var rep datatypes.AdminSetBalanceReply
	err = c.Call("NodeService.AdminSetBalance", datatypes.AdminSetBalanceArgs{AccountID: fmt.Sprintf("%d", id), Balance: bal}, &rep)
	if err != nil {
		return err
	}
	if !rep.Ok {
		return fmt.Errorf("admin set failed")
	}
	return nil
}

func adminDeleteAccount(addr string, id int) error {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	var rep datatypes.AdminDeleteAccountReply
	err = c.Call("NodeService.AdminDeleteAccount", datatypes.AdminDeleteAccountArgs{AccountID: fmt.Sprintf("%d", id)}, &rep)
	if err != nil {
		return err
	}
	if !rep.Ok {
		return fmt.Errorf("admin delete failed")
	}
	return nil
}

func adminReloadOverridesAllNodes() {
	for nid := 1; nid <= config.NumNodes; nid++ {
		addr := config.NodeAddresses[nid]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var rep datatypes.AdminReloadOverridesReply
				_ = c.Call("NodeService.AdminReloadOverrides", datatypes.AdminReloadOverridesArgs{}, &rep)
			}
		}(addr)
	}
}

// applyReshardMoves performs the migration for a set of moves.
func applyReshardMoves(moves []move) {
	for _, m := range moves {
		// 1) Get balance from old leader
		_, oldAddr, err := findClusterLeader(m.OldCID)
		if err != nil {
			log.Printf("[Reshard] skip id=%d: old leader err: %v", m.ID, err)
			continue
		}
		bal, err := adminGetBalance(oldAddr, m.ID)
		if err != nil {
			log.Printf("[Reshard] skip id=%d: admin get err: %v", m.ID, err)
			continue
		}
		// 2) Fan-out set to destination cluster
		destNodes := config.ClusterMembers[m.NewCID]
		destOK := true
		for _, nid := range destNodes {
			addr := config.NodeAddresses[nid]
			if err := adminSetBalance(addr, m.ID, bal); err != nil {
				log.Printf("[Reshard] abort move id=%d -> cid=%d: set fail on n%d: %v", m.ID, m.NewCID, nid, err)
				destOK = false
				break
			}
		}
		if !destOK {
			// Abort move
			continue
		}
		// 3) Fan-out delete to source cluster (best-effort)
		srcNodes := config.ClusterMembers[m.OldCID]
		for _, nid := range srcNodes {
			addr := config.NodeAddresses[nid]
			if err := adminDeleteAccount(addr, m.ID); err != nil {
				log.Printf("[Reshard] WARN delete old id=%d on n%d failed: %v", m.ID, nid, err)
			}
		}
		// 4) Update override map in-process
		shard.SetAccountClusterOverride(m.ID, m.NewCID)
	}
	// Persist and fan-out reload
	_ = shard.SaveOverridesToFile()
	adminReloadOverridesAllNodes()
}

// runReshard builds a move list and applies it.
func runReshard() {
	log.Printf("[Reshard] computing moves from sample of %d txns", len(txnSample.snapshot()))
	mv := buildReshardMoves()
	if len(mv) == 0 {
		fmt.Println("No moves suggested.")
		return
	}
	for _, m := range mv {
		fmt.Printf("Move account %d: %d -> %d (score=%d)\n", m.ID, m.OldCID, m.NewCID, m.Score)
	}
	applyReshardMoves(mv)
	fmt.Println("=== RESHARD DONE ===")
}

// ClientWorker is a placeholder goroutine for future async work per client.
func ClientWorker(clientID int, inputChan <-chan string) {
	//log.Printf("Client %d started and listening for commands...\n", clientID)
	log.Printf("ClientWorker %d: started", clientID)

	for txn := range inputChan {
		fmt.Println(txn)
		log.Printf("ClientWorker %d: received txn %s", clientID, txn)
	}
}

// ParseTxnSetsFromCSV ingests the CSV test plan into structured sets.
func ParseTxnSetsFromCSV(filePath string) ([]TxnSet, error) {
	log.Printf("ClientDriver: parsing CSV file %s", filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	sets := make([]TxnSet, 0)
	currentSet := TxnSet{Txns: make([]datatypes.Txn, 0)}

	// Each CSV record is read row wise
	for i, record := range records {
		if i == 0 {
			continue
		}

		setNum, _ := strconv.Atoi(strings.TrimSpace(record[0]))

		if setNum != currentSet.SetNumber {
			// if setNum != currentSet.SetNumber {
			// 	if currentSet.SetNumber == 0 { // When a column is empty, the set number will be 0
			// 		currentSet = TxnSet{SetNumber: setNum, Txns: make([]datatypes.Txn, 0)}

			// 	} else {
			// 		sets = append(sets, currentSet) // So whenever we see 0, it means it's part of current set
			// 	}
			// } else{
			// 	sets = append(sets, currentSet)
			// }
			if setNum != 0 && currentSet.SetNumber == 0 {
				currentSet = TxnSet{SetNumber: setNum, Txns: make([]datatypes.Txn, 0)}
			} else if setNum != 0 && currentSet.SetNumber != 0 {
				sets = append(sets, currentSet)
				currentSet = TxnSet{SetNumber: setNum, Txns: make([]datatypes.Txn, 0)}
			}
		}

		txnStr := strings.Trim(record[1], " \"()")

		if strings.EqualFold(strings.TrimSpace(txnStr), "LF") {
			currentSet.Txns = append(currentSet.Txns, datatypes.Txn{Sender: "__LF__", Receiver: "", Amount: 0})
			continue
		}

		txnParts := strings.Split(txnStr, ",") // Splits A,B,3 into [A B 3]

		if len(txnParts) == 3 {
			amount, _ := strconv.Atoi(strings.TrimSpace(txnParts[2]))
			txn := datatypes.Txn{
				Sender:   strings.TrimSpace(txnParts[0]),
				Receiver: strings.TrimSpace(txnParts[1]),
				Amount:   amount,
			}
			currentSet.Txns = append(currentSet.Txns, txn)
		} else if len(txnParts) == 1 {
			// Treat single (s) as read-only for sender s
			sender := strings.TrimSpace(txnParts[0])
			if sender != "" {
				txn := datatypes.Txn{Sender: sender, Receiver: "", Amount: 0}
				currentSet.Txns = append(currentSet.Txns, txn)
			}
		}

		if len(currentSet.Txns) == 1 {
			nodesStr := strings.Trim(record[2], " \"[]")
			nodeStrs := strings.Split(nodesStr, ",")
			for _, nodeStr := range nodeStrs {
				nodeStr = strings.TrimSpace(nodeStr)
				nodeStr = strings.TrimPrefix(nodeStr, "n")
				if nodeID, err := strconv.Atoi(nodeStr); err == nil {
					currentSet.LiveNodes = append(currentSet.LiveNodes, nodeID)
				}
			}
		}
	}
	if currentSet.SetNumber != 0 {
		sets = append(sets, currentSet)
	}

	log.Printf("ClientDriver: parsed %d sets from %s", len(sets), filePath)
	return sets, nil

}

const lfSentinelSender = "__LF__"

// triggerLeaderFailure disables the current leader and waits for a new one.
func triggerLeaderFailure() (int, error) {
	log.Printf("ClientDriver: triggerLeaderFailure invoked")
	currentLeader, err := findCurrentLeader()
	if err != nil {
		return 0, fmt.Errorf("unable to determine current leader: %w", err)
	}

	if err := disableLeaderAcrossCluster(currentLeader); err != nil {
		return 0, fmt.Errorf("failed to disable leader Node %d: %w", currentLeader, err)
	}
	log.Printf("ClientDriver: disabled leader %d", currentLeader)

	waitDuration := 8 * time.Second

	newLeader, err := waitForNewLeader(currentLeader, waitDuration)
	if err != nil {
		return 0, err
	}

	for _, c := range clients {
		c.UpdateLeader(newLeader)
	}

	log.Printf("ClientDriver: new leader after LF is %d", newLeader)
	return newLeader, nil
}

// findCurrentLeader queries nodes to determine the prevailing leader.
func findCurrentLeader() (int, error) {
	log.Printf("ClientDriver: findCurrentLeader scanning nodes")

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address, ok := config.NodeAddresses[nodeID]
		if !ok {
			continue
		}

		client, err := rpc.Dial("tcp", address)
		if err != nil {
			continue
		}

		var info datatypes.LeaderInfo
		err = client.Call("NodeService.GetLeader", true, &info)
		client.Close()
		if err != nil {
			continue
		}

		if info.IsLeader && info.LeaderID != 0 {
			return info.LeaderID, nil
		}
	}

	return 0, fmt.Errorf("no leader information available from active nodes")
}

// disableLeaderAcrossCluster asks every node to mark the leader inactive.
func disableLeaderAcrossCluster(leaderID int) error {
	var firstErr error
	log.Printf("ClientDriver: disabling leader %d cluster-wide", leaderID)

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address, ok := config.NodeAddresses[nodeID]
		if !ok {
			continue
		}

		client, err := rpc.Dial("tcp", address)
		if err != nil {

			if firstErr == nil {
				firstErr = fmt.Errorf("node %d unreachable: %w", nodeID, err)
			}
			continue
		}

		args := datatypes.UpdateNodeArgs{NodeID: leaderID, IsLive: false}
		var reply bool
		if err := client.Call("NodeService.UpdateActiveStatus", args, &reply); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("update on node %d failed: %w", nodeID, err)
		}
		client.Close()
	}
	return firstErr
}

// waitForNewLeader polls until a different leader is observed or timeout.
func waitForNewLeader(oldLeader int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	log.Printf("ClientDriver: waiting for new leader (old=%d timeout=%s)", oldLeader, timeout)
	var lastObserved int

	for time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)

		newLeader, err := findCurrentLeader()
		if err != nil {
			continue
		}
		lastObserved = newLeader

		if newLeader != 0 && newLeader != oldLeader {
			return newLeader, nil
		}
	}

	if lastObserved == 0 {
		log.Printf("ClientDriver: no leader observed before timeout")
		return 0, fmt.Errorf("timed out waiting for new leader (previous leader: %d)", oldLeader)
	}
	log.Printf("ClientDriver: timeout still shows leader %d", lastObserved)
	return 0, fmt.Errorf("timed out waiting for new leader: still seeing Node %d as leader", lastObserved)
}

// waitForStableLeader blocks until a node reports IsLeader==true or timeout.
func waitForStableLeader(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		id, err := findCurrentLeader()
		if err == nil && id != 0 {
			return id, nil
		}
		lastErr = err
		time.Sleep(300 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no leader observed before timeout")
	}
	return 0, lastErr
}

// processNextTestSet enforces liveness pattern then executes the set's txns.
func processNextTestSet(reader *bufio.Reader) {
	if currentSetIndex >= len(sets) {
		log.Printf("ClientDriver: no remaining transaction sets")

		return
	}

	currentSet := sets[currentSetIndex]
	log.Printf("ClientDriver: processing set %d with %d txns", currentSet.SetNumber, len(currentSet.Txns))
	// Flush per-set state and reset DB balances across nodes
	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address := config.NodeAddresses[nodeID]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var fr datatypes.FlushStateReply
				_ = c.Call("NodeService.FlushState", datatypes.FlushStateArgs{ResetDB: true, ResetConsensus: true, ResetWAL: true}, &fr)
			}
		}(address)
	}
	time.Sleep(300 * time.Millisecond)

	// Apply initial liveness via bulk update on each node
	active := make(map[int]bool)
	for id := 1; id <= config.NumNodes; id++ {
		active[id] = false
	}
	for _, live := range currentSet.LiveNodes {
		active[live] = true
	}
	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address := config.NodeAddresses[nodeID]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var ok bool
				_ = c.Call("NodeService.UpdateActiveStatusForBulk", datatypes.UpdateClusterStatusArgs{Active: active}, &ok)
			}
		}(address)
	}
	time.Sleep(500 * time.Millisecond)

	// Prime leaders on n1,n4,n7 if they are live using ForceLeader (runs Phase-1 safely)
	for _, nid := range []int{1, 4, 7} {
		if !active[nid] {
			continue
		}
		addr := config.NodeAddresses[nid]
		go func(addr string) {
			if c, err := rpc.Dial("tcp", addr); err == nil {
				defer c.Close()
				var trep datatypes.TriggerElectionReply
				_ = c.Call("NodeService.ForceLeader", datatypes.TriggerElectionArgs{}, &trep)
			}
		}(addr)
	}
	time.Sleep(500 * time.Millisecond)

	// Init per-set tracking
	modifiedIDs = make(map[int]bool)
	perf = PerfStats{}
	perf.StartWall = time.Now()

	// No global leader updates here; routing is cluster-aware in client.SendTransaction

	// Send transactions in concurrent segments between control commands (LF/F/R)
	segment := make([]datatypes.Txn, 0)
	successCount := 0
	failCount := 0

	runSegment := func(seg []datatypes.Txn) (int, int) {
		if len(seg) == 0 {
			return 0, 0
		}
		sc, fc, segPerf := runSegmentConcurrent(seg)
		perf.TxnCount += segPerf.TxnCount
		perf.TotalLatency += segPerf.TotalLatency
		perf.EndWall = time.Now()
		return sc, fc
	}

	for i, tx := range currentSet.Txns {
		log.Printf("\n[%d/%d] Transaction: %s\n", i+1, len(currentSet.Txns), tx)
		log.Printf("ClientDriver: set %d txn %d/%d %s", currentSet.SetNumber, i+1, len(currentSet.Txns), tx.String())

		// Identify control commands as segment boundaries
		if tx.Sender == lfSentinelSender {
			sc, fc := runSegment(segment)
			successCount += sc
			failCount += fc
			segment = segment[:0]
			// Apply LF synchronously
			newLeader, err := triggerLeaderFailure()
			if err != nil {
				failCount++
			} else {
				log.Printf("LF succeeded: new leader elected -> Node %d", newLeader)
			}
			time.Sleep(2 * time.Second)
			continue
		}

		s := strings.TrimSpace(tx.Sender)
		isF := strings.HasPrefix(strings.ToUpper(s), "F(")
		isR := strings.HasPrefix(strings.ToUpper(s), "R(")
		if isF || isR {
			// Drain previous segment first
			sc, fc := runSegment(segment)
			successCount += sc
			failCount += fc
			segment = segment[:0]
			// Apply F/R synchronously (existing logic)
			inside := s
			if idx := strings.Index(inside, "("); idx >= 0 {
				inside = inside[idx+1:]
			}
			if idx := strings.Index(inside, ")"); idx >= 0 {
				inside = inside[:idx]
			}
			inside = strings.TrimPrefix(strings.TrimSpace(inside), "n")
			nid, perr := strconv.Atoi(inside)
			if perr != nil || nid < 1 || nid > config.NumNodes {
				log.Printf("ClientDriver: invalid F/R command target=%q", s)
				continue
			}
			isRecover := isR
			for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
				addr := config.NodeAddresses[nodeID]
				go func(addr string) {
					if c, err := rpc.Dial("tcp", addr); err == nil {
						defer c.Close()
						var ok bool
						_ = c.Call("NodeService.UpdateActiveStatus", datatypes.UpdateNodeArgs{NodeID: nid, IsLive: isRecover}, &ok)
					}
				}(addr)
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// Accumulate normal tx into the current segment
		// Record attempted keys (for summary after set)
		if sid, err := strconv.Atoi(tx.Sender); err == nil {
			modifiedIDs[sid] = true
		}
		if rid, err := strconv.Atoi(tx.Receiver); err == nil {
			modifiedIDs[rid] = true
		}
		segment = append(segment, tx)
	}

	// Drain tail segment
	sc, fc := runSegment(segment)
	successCount += sc
	failCount += fc

	log.Printf("ClientDriver: set %d complete success=%d fail=%d", currentSet.SetNumber, successCount, failCount)
	currentSetIndex++
	currentSet.SetNumber++

	// Post-set quick summary
	avg := time.Duration(0)
	dur := perf.EndWall.Sub(perf.StartWall)
	if perf.TxnCount > 0 {
		avg = perf.TotalLatency / time.Duration(perf.TxnCount)
	}
	log.Printf("Performance: txns=%d avgLatency=%v throughput=%.2f/s", perf.TxnCount, avg, float64(perf.TxnCount)/maxf(dur.Seconds(), 0.001))
}

// runSegmentConcurrent sends a batch of txns concurrently with a bounded worker pool.
func runSegmentConcurrent(seg []datatypes.Txn) (successCount int, failCount int, segPerf PerfStats) {
	if len(seg) == 0 {
		return 0, 0, PerfStats{}
	}
	// Choose a reasonable pool size
	k := 8
	if len(seg) < k {
		k = len(seg)
	}
	jobs := make(chan datatypes.Txn, len(seg))
	var wg sync.WaitGroup
	var mu sync.Mutex

	worker := func() {
		defer wg.Done()
		for tx := range jobs {
			// Read-only if Receiver empty or amount 0
			if strings.TrimSpace(tx.Receiver) == "" || tx.Amount == 0 {
				sid, _ := strconv.Atoi(tx.Sender)
				cid := shard.ClusterOfItem(sid)
				leaderCand := 1
				if cid == 2 {
					leaderCand = 4
				} else if cid == 3 {
					leaderCand = 7
				}
				addr := config.NodeAddresses[leaderCand]
				t0 := time.Now()
				if c, err := rpc.Dial("tcp", addr); err == nil {
					var r datatypes.GetBalanceReply
					_ = c.Call("NodeService.GetBalance", datatypes.GetBalanceArgs{AccountID: tx.Sender}, &r)
					c.Close()
				}
				t1 := time.Now()
				mu.Lock()
				segPerf.TxnCount++
				segPerf.TotalLatency += t1.Sub(t0)
				mu.Unlock()
				continue
			}

			// RW path with retry-on-locked policy (up to 5 attempts, 30ms jitter)
			c, exists := clients[tx.Sender]
			if !exists {
				mu.Lock()
				failCount++
				mu.Unlock()
				continue
			}
			// record into TxnSample (RW only)
			if sid, err1 := strconv.Atoi(tx.Sender); err1 == nil {
				if rid, err2 := strconv.Atoi(tx.Receiver); err2 == nil && tx.Amount > 0 {
					txnSample.record(sid, rid)
				}
			}

			start := time.Now()
			attempts := 0
			finalSuccess := false
			for {
				attempts++
				reply, err := c.SendTransaction(tx)
				if err != nil {
					// Any RPC error is final per spec
					break
				}
				if reply.Success {
					finalSuccess = true
					break
				}
				// classify reply
				msg := strings.ToLower(reply.Message)
				if strings.Contains(msg, "locked") && attempts < 5 {
					time.Sleep(300 * time.Millisecond)
					continue
				}
				if strings.Contains(msg, "abort") || strings.Contains(msg, "aborted") ||
					strings.Contains(msg, "insufficient funds") || strings.Contains(msg, "insufficient active nodes") ||
					strings.Contains(msg, "consensus failed") {
					// final failure
					break
				}
				// default: final failure
				break
			}
			mu.Lock()
			segPerf.TxnCount++
			segPerf.TotalLatency += time.Since(start)
			if finalSuccess {
				successCount++
			} else {
				failCount++
			}
			mu.Unlock()
		}
	}

	// Start workers
	for i := 0; i < k; i++ {
		wg.Add(1)
		go worker()
	}
	// Feed jobs
	for _, tx := range seg {
		jobs <- tx
	}
	close(jobs)
	wg.Wait()
	segPerf.EndWall = time.Now()
	segPerf.StartWall = segPerf.EndWall // unused granularity here
	return
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// printLogFromNode calls the PrintLog RPC on a chosen node.
func printLogFromNode(reader *bufio.Reader) {
	log.Printf("ClientDriver: PrintLog command selected")
	fmt.Print("Enter node ID (1-5): ")
	nodeInput, _ := reader.ReadString('\n')
	nodeID, err := strconv.Atoi(strings.TrimSpace(nodeInput))
	if err != nil || nodeID < 1 || nodeID > config.NumNodes {
		fmt.Println("❌ Invalid node ID")
		return
	}

	address, ok := config.NodeAddresses[nodeID]
	if !ok {
		log.Printf("❌ Unknown node ID %d\n", nodeID)
		return
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Printf("❌ Could not connect to node %d at %s: %v\n", nodeID, address, err)
		return
	}
	defer client.Close()

	var reply string
	err = client.Call("NodeService.PrintLog", true, &reply)
	if err != nil {
		log.Printf("❌ RPC error calling PrintLog on node %d: %v\n", nodeID, err)
		return
	}

	fmt.Println(reply)
}

// printDBFromNode fetches the DB contents from a node.
func printDBFromNode(reader *bufio.Reader) {
	log.Printf("ClientDriver: PrintDB command selected")
	fmt.Print("Enter node ID (1-5): ")
	nodeInput, _ := reader.ReadString('\n')
	nodeID, err := strconv.Atoi(strings.TrimSpace(nodeInput))
	address, exists := config.NodeAddresses[nodeID]
	if !exists {
		fmt.Println("❌ Node address not found")
		return
	}
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		fmt.Println("❌ Failed to connect to node:", err)
		return
	}
	defer client.Close()

	args := datatypes.PrintDBArgs{NodeID: nodeID}
	var reply datatypes.PrintDBReply

	err = client.Call("NodeService.PrintDB", args, &reply)
	if err != nil {
		log.Printf("Error calling PrintDB on Node %d: %v", nodeID, err)
		return
	}

	fmt.Println(reply.DBContents)

}

// printStatusFromNode prints consensus status for one seq across nodes.
func printStatusFromNode(reader *bufio.Reader) {
	log.Printf("ClientDriver: PrintStatus command selected")

	fmt.Print("Enter sequence number: ")
	seqInput, _ := reader.ReadString('\n')
	seqNum, err := strconv.Atoi(strings.TrimSpace(seqInput))
	if err != nil || seqNum < 1 {
		fmt.Println("❌ Invalid sequence number")
		return
	}

	log.Printf("\n===== Status of Seq %d across all nodes =====\n", seqNum)
	log.Printf("ClientDriver: querying status for seq %d", seqNum)

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address, ok := config.NodeAddresses[nodeID]
		if !ok {
			log.Printf("❌ Unknown node ID %d\n", nodeID)
			continue
		}
		client, err := rpc.Dial("tcp", address)
		if err != nil {
			log.Printf("❌ Could not connect to node %d: %v\n", nodeID, err)
			return
		}
		var reply string
		err = client.Call("NodeService.PrintStatus", seqNum, &reply)
		client.Close()

		if err != nil {
			log.Printf("❌ RPC error calling PrintStatus on node %d: %v\n", nodeID, err)
			return
		}

		fmt.Println(reply)
	}
}

// printViewFromAllNodes invokes PrintView on every node for diagnostics.
func printViewFromAllNodes() {
	fmt.Println("===== Printing NEW-VIEW messages from all nodes =====")
	log.Printf("ClientDriver: PrintViewAll command selected")

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address := config.NodeAddresses[nodeID]

		client, err := rpc.Dial("tcp", address)
		if err != nil {
			log.Printf("❌ Node %d unreachable: %v\n", nodeID, err)
			continue
		}

		var reply string
		err = client.Call("NodeService.PrintView", true, &reply)
		client.Close()

		if err != nil {
			log.Printf("❌ RPC error on Node %d: %v\n", nodeID, err)
			continue
		}

		fmt.Println(reply)
	}

	fmt.Println("==============================================")
}

// printBalanceFromCluster prompts for a client ID and prints balances on all
// three nodes in the owning cluster in the format: nX : bal, nY : bal, nZ : bal
func printBalanceFromCluster(reader *bufio.Reader) {
	fmt.Print("Enter client ID: ")
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	id, err := strconv.Atoi(line)
	if err != nil || id < config.MinAccountID || id > config.MaxAccountID {
		fmt.Println("❌ Invalid client ID")
		return
	}

	cid := shard.ClusterOfItem(id)
	if cid == 0 {
		fmt.Println("❌ No cluster for this ID")
		return
	}

	nodes, ok := config.ClusterMembers[cid]
	if !ok || len(nodes) == 0 {
		fmt.Println("❌ No nodes for cluster")
		return
	}

	parts := make([]string, 0, len(nodes))
	for _, nid := range nodes {
		addr := config.NodeAddresses[nid]
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			parts = append(parts, fmt.Sprintf("n%d : down/disconnected", nid))
			continue
		}
		var rep datatypes.GetBalanceReply
		_ = c.Call("NodeService.GetBalance", datatypes.GetBalanceArgs{AccountID: fmt.Sprintf("%d", id)}, &rep)
		c.Close()
		parts = append(parts, fmt.Sprintf("n%d : %d", nid, rep.Balance))
	}

	fmt.Println(strings.Join(parts, ", "))
}

// printModifiedBalancesAllNodes prints, in parallel, the balances of only those
// data items that were modified in the current test set, across all 9 nodes.
// Output format: one line per node -> nX : id1=bal1, id2=bal2, ...
func printModifiedBalancesAllNodes() {
	// Snapshot and sort modified IDs
	if len(modifiedIDs) == 0 {
		fmt.Println("No modified keys recorded in this test case. Run a set first.")
		return
	}
	ids := make([]int, 0, len(modifiedIDs))
	for id := range modifiedIDs {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	type nodeResult struct {
		node int
		line string
	}
	resCh := make(chan nodeResult, config.NumNodes)
	var wg sync.WaitGroup

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		wg.Add(1)
		go func(nid int) {
			defer wg.Done()
			addr := config.NodeAddresses[nid]
			c, err := rpc.Dial("tcp", addr)
			if err != nil {
				resCh <- nodeResult{node: nid, line: fmt.Sprintf("n%d : down/disconnected", nid)}
				return
			}
			defer c.Close()
			parts := make([]string, 0, len(ids))
			for _, id := range ids {
				var rep datatypes.GetBalanceReply
				_ = c.Call("NodeService.GetBalance", datatypes.GetBalanceArgs{AccountID: fmt.Sprintf("%d", id)}, &rep)
				parts = append(parts, fmt.Sprintf("%d=%d", id, rep.Balance))
			}
			resCh <- nodeResult{node: nid, line: fmt.Sprintf("n%d : %s", nid, strings.Join(parts, ", "))}
		}(nodeID)
	}

	go func() { wg.Wait(); close(resCh) }()

	// Collect in map to print in node order (results arrive out-of-order)
	lines := make(map[int]string)
	for r := range resCh {
		lines[r.node] = r.line
	}
	for nid := 1; nid <= config.NumNodes; nid++ {
		if s, ok := lines[nid]; ok {
			fmt.Println(s)
		} else {
			fmt.Printf("n%d : no data\n", nid)
		}
	}
}

// printPerformance reports throughput and average latency captured during
// the latest Process Set run. Throughput measured wall-clock from perf.StartWall
// to perf.EndWall; latency is average per operation measured from send to reply.
func printPerformance() {
	if perf.StartWall.IsZero() || perf.EndWall.IsZero() || perf.TxnCount == 0 {
		fmt.Println("No performance data yet. Run a transaction set first.")
		return
	}
	wall := perf.EndWall.Sub(perf.StartWall)
	tps := float64(perf.TxnCount) / maxf(wall.Seconds(), 0.001)
	avg := time.Duration(0)
	if perf.TxnCount > 0 {
		avg = perf.TotalLatency / time.Duration(perf.TxnCount)
	}
	fmt.Printf("Throughput: %.2f ops/s\n", tps)
	fmt.Printf("Average latency: %v\n", avg)
}
