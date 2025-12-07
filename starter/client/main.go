// =======================================
// File: starter/client/main.go
// Description: CLI driver that parses CSV test sets, orchestrates liveness changes, and submits txns.
// =======================================
package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"multipaxos/rituraj735/pkg/client"
	"multipaxos/rituraj735/pkg/shard"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
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

	// Reading CSV file path from user
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
		if option == 7 {
			break
		}
		fmt.Println("1.Process next transactions set")
		fmt.Println("2.PrintLog")
		fmt.Println("3.PrintDB")
		fmt.Println("4.PrintStatus")
		fmt.Println("5.PrintView")
		fmt.Println("6.PrintBalance")
		fmt.Println("7.Exit")
		fmt.Scanln(&option)
		fmt.Println("You chose option:", option)
		switch option {
		case 1:
			processNextTestSet(filePathReader)
		case 2:
			printLogFromNode(filePathReader)
		case 3:
			printDBFromNode(filePathReader)
		case 4:
			printStatusFromNode(filePathReader)
		case 5:
			printViewFromAllNodes()
		case 6:
			printBalanceFromCluster(filePathReader)
		case 7:
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid option. Please try again.")
		}
	}

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

	//flushBacklog()
	successCount := 0
	failCount := 0
	for i, tx := range currentSet.Txns {
		log.Printf("\n[%d/%d] Transaction: %s\n", i+1, len(currentSet.Txns), tx)
		log.Printf("ClientDriver: set %d txn %d/%d %s", currentSet.SetNumber, i+1, len(currentSet.Txns), tx.String())

		if tx.Sender == lfSentinelSender {
			//fmt.Println("⚠️  LF event detected: initiating leader failover simulation")
			newLeader, err := triggerLeaderFailure()
			if err != nil {
				//log.Printf("❌ LF failed: %v\n", err)
				failCount++
			} else {
				log.Printf("LF succeeded: new leader elected, it'll automatically continue, please wait-> Node %d\n", newLeader)
			}
			// experimenting with sleep time after LF
			time.Sleep(2 * time.Second)
			continue
		}

		// F/R admin commands
		s := strings.TrimSpace(tx.Sender)
		if strings.HasPrefix(strings.ToUpper(s), "F(") || strings.HasPrefix(strings.ToUpper(s), "R(") {
			inside := s
			if i := strings.Index(inside, "("); i >= 0 {
				inside = inside[i+1:]
			}
			if j := strings.Index(inside, ")"); j >= 0 {
				inside = inside[:j]
			}
			inside = strings.TrimSpace(inside)
			inside = strings.TrimPrefix(inside, "n")
			nid, perr := strconv.Atoi(inside)
			if perr != nil || nid < 1 || nid > config.NumNodes {
				log.Printf("ClientDriver: invalid F/R command target=%q", s)
				continue
			}
			isRecover := strings.HasPrefix(strings.ToUpper(s), "R(")
			// Broadcast liveness change to all nodes
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
			// allow brief settle
			time.Sleep(200 * time.Millisecond)
			continue
		}

		// Read-only if Receiver is empty
		if strings.TrimSpace(tx.Receiver) == "" || tx.Amount == 0 {
			sid, _ := strconv.Atoi(tx.Sender)
			// pick cluster anchor leader candidate
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
			perf.TxnCount++
			perf.TotalLatency += t1.Sub(t0)
			perf.EndWall = t1
			continue
		}

		// RW transaction path
		c, exists := clients[tx.Sender]
		if !exists {
			log.Printf("Client %s not found\n", tx.Sender)
			failCount++
			continue
		}
		// record attempted keys
		if sid, err := strconv.Atoi(tx.Sender); err == nil {
			modifiedIDs[sid] = true
		}
		if rid, err := strconv.Atoi(tx.Receiver); err == nil {
			modifiedIDs[rid] = true
		}

		t0 := time.Now()
		reply, err := c.SendTransaction(tx)
		t1 := time.Now()
		perf.TxnCount++
		perf.TotalLatency += t1.Sub(t0)
		perf.EndWall = t1
		if err != nil {

			// Defer if no leader is currently available or quorum is insufficient
			if strings.Contains(strings.ToLower(reply.Message), "insufficient active nodes") ||
				strings.Contains(strings.ToLower(err.Error()), "no leader available") {
				deferTxn(tx)
			}
			failCount++
		} else if reply.Success {
			log.Printf("ClientDriver: txn applied seq=%d", reply.SeqNum)
			successCount++
		} else {

			if strings.Contains(strings.ToLower(reply.Message), "insufficient active nodes") {
				deferTxn(tx)
			}
			failCount++
		}

		time.Sleep(200 * time.Millisecond)

	}
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
