package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"multipaxos/rituraj735/pkg/client"
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

func deferTxn(tx datatypes.Txn) {
	backlog = append(backlog, tx)
}

func flushBacklog() {
	if len(backlog) == 0 {
		return
	}

	fmt.Printf("\n--- Returning %d deferred transactions to the backlog ---\n", len(backlog))

	i := 0
	for i < len(backlog) {
		tx := backlog[i]
		c, ok := clients[tx.Sender]
		if !ok {
			fmt.Printf("Backlog: client %s not found; skipping\n", tx.Sender)
			i++
			continue
		}

		reply, err := c.SendTransaction(tx)
		if err != nil || !reply.Success {
			// still failed → keep for next round
			i++
			continue
		}

		// success → remove it
		backlog = append(backlog[:i], backlog[i+1:]...)
	}
}

func main() {
	fmt.Println("Welcome to Bank of Paxos")
	fmt.Println("==========================")
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
	fmt.Println(sets)
	if err != nil {
		log.Fatalf("Failed to parse CSV file: %v", err)
	}

	//write the logic of after reading the file here later
	fmt.Scanln(&fileName)
	fmt.Println("Choose an option:")
	for {
		if option == 6 {
			break
		}
		fmt.Println("1.Process next transactions set")
		fmt.Println("2.PrintLog")
		fmt.Println("3.PrintDB")
		fmt.Println("4.PrintStatus")
		fmt.Println("5.PrintView")
		fmt.Println("6.Exit")
		fmt.Scanln(&option)
		fmt.Println("You chose option:", option)
		switch option {
		case 1:
			processNextTestSet(filePathReader)
		case 2:
			printLogFromNode(filePathReader)
		case 3:
			fmt.Println("PrintDB()")
			printDBFromNode(filePathReader)
		case 4:
			fmt.Println("PrintStatus()")
			printStatusFromNode(filePathReader)
		case 5:
			fmt.Println("PrintView()")
			printViewFromAllNodes()
		case 6:
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid option. Please try again.")
		}
	}

}

func ClientWorker(clientID int, inputChan <-chan string) {
	fmt.Printf("Client %d started and listening for commands...\n", clientID)

	for txn := range inputChan {
		fmt.Println(txn)
	}
}

func ParseTxnSetsFromCSV(filePath string) ([]TxnSet, error) {
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

		txnStr := strings.Trim(record[1], " \"()") //Cutset removes space, " and ( and )"

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
		}

		//Handle the LF case here

		// Find out the live nodes
		if len(currentSet.Txns) == 1 { // Why 1? Because the very first row of txn has live nodes entry
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
	// fmt.Println("set is here :", sets)
	return sets, nil

}

const lfSentinelSender = "__LF__"

func triggerLeaderFailure() (int, error) {
	currentLeader, err := findCurrentLeader()
	if err != nil {
		return 0, fmt.Errorf("unable to determine current leader: %w", err)
	}

	fmt.Printf("LF: Disabling current leader Node %d across cluster...\n", currentLeader)
	if err := disableLeaderAcrossCluster(currentLeader); err != nil {
		return 0, fmt.Errorf("failed to disable leader Node %d: %w", currentLeader, err)
	}

	waitDuration := 8 * time.Second
	fmt.Printf("LF: Waiting up to %s for new leader election...\n", waitDuration)
	newLeader, err := waitForNewLeader(currentLeader, waitDuration)
	if err != nil {
		return 0, err
	}

	for _, c := range clients {
		c.UpdateLeader(newLeader)
	}

	return newLeader, nil
}

// Ask the cluster who the current leader is.
// Returns the leader ID if known; otherwise an error.
func findCurrentLeader() (int, error) {
	leaderCounts := make(map[int]int)

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address, ok := config.NodeAddresses[nodeID]
		if !ok {
			continue
		}

		client, err := rpc.Dial("tcp", address)
		if err != nil {
			continue // node might be down or partitioned
		}

		var info datatypes.LeaderInfo
		err = client.Call("NodeService.GetLeader", true, &info)
		client.Close()
		if err != nil {
			continue
		}

		// If a node says *it* is leader, trust that immediately.
		if info.IsLeader && info.LeaderID != 0 {
			return info.LeaderID, nil
		}

		// Otherwise tally what it believes is the leader.
		if info.LeaderID != 0 {
			leaderCounts[info.LeaderID]++
		}
	}

	// Fallback: pick the most commonly reported leader among responders.
	bestLeader, bestCount := 0, 0
	for id, cnt := range leaderCounts {
		if cnt > bestCount {
			bestLeader, bestCount = id, cnt
		}
	}
	if bestLeader == 0 {
		return 0, fmt.Errorf("no leader information available from active nodes")
	}
	return bestLeader, nil
}

// Tell every node to mark `leaderID` as not live in its ActiveNodes map.
func disableLeaderAcrossCluster(leaderID int) error {
	var firstErr error

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address, ok := config.NodeAddresses[nodeID]
		if !ok {
			continue
		}

		client, err := rpc.Dial("tcp", address)
		if err != nil {
			// keep the first error to return; continue trying others
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

// Poll the cluster until a different leader than oldLeader appears, or time out.
func waitForNewLeader(oldLeader int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
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
		return 0, fmt.Errorf("timed out waiting for new leader (previous leader: %d)", oldLeader)
	}
	return 0, fmt.Errorf("timed out waiting for new leader: still seeing Node %d as leader", lastObserved)
}

func processNextTestSet(reader *bufio.Reader) {
	if currentSetIndex >= len(sets) {
		fmt.Println("All sets have been processed.")
		return
	}

	currentSet := sets[currentSetIndex]
	fmt.Println("printing current set", currentSet)
	fmt.Printf("\n========================================\n")
	fmt.Printf("=== Processing Set %d ===\n", currentSet.SetNumber)
	fmt.Printf("=========================================\n")
	fmt.Printf("Live Nodes: %v\n", currentSet.LiveNodes)
	fmt.Printf("Total transactions: %d\n", len(currentSet.Txns))
	fmt.Printf("-----------------------------------------\n")

	// Update ActiveNodes for all nodes in the cluster
	for _, nodeID := range []int{1, 2, 3, 4, 5} {
		isLive := false
		for _, liveID := range currentSet.LiveNodes {
			if liveID == nodeID {
				isLive = true
				break
			}
		}

		// Send an RPC to update that node's ActiveNodes map
		for _, targetID := range []int{1, 2, 3, 4, 5} {
			go func(targetID, nodeID int, live bool) {
				address := config.NodeAddresses[targetID]
				client, err := rpc.Dial("tcp", address)
				if err != nil {
					return
				}
				defer client.Close()

				args := datatypes.UpdateNodeArgs{
					NodeID: nodeID,
					IsLive: live,
				} // or define a struct like UpdateNodeStatusArgs
				var reply bool
				_ = client.Call("NodeService.UpdateActiveStatus", args, &reply)
			}(targetID, nodeID, isLive)
		}
	}

	time.Sleep(300 * time.Millisecond)
	flushBacklog()
	successCount := 0
	failCount := 0
	for i, tx := range currentSet.Txns {
		fmt.Printf("\n[%d/%d] Transaction: %s\n", i+1, len(currentSet.Txns), tx)

		if tx.Sender == lfSentinelSender {
			fmt.Println("⚠️  LF event detected: initiating leader failover simulation")
			newLeader, err := triggerLeaderFailure()
			if err != nil {
				fmt.Printf("❌ LF failed: %v\n", err)
				failCount++
			} else {
				fmt.Printf("✅ LF succeeded: new leader elected -> Node %d\n", newLeader)
			}
			time.Sleep(8 * time.Second) // allow election to stabilize
			continue
		}

		c, exists := clients[tx.Sender]
		if !exists {
			fmt.Printf("Client %s not found\n", tx.Sender)
			failCount++
			continue
		}

		reply, err := c.SendTransaction(tx)
		if err != nil {
			fmt.Printf("Transaction failed: %v\n", err)
			if strings.Contains(strings.ToLower(reply.Message), "insufficient active nodes") {
				deferTxn(tx)
			}
			failCount++
		} else if reply.Success {
			fmt.Printf(" Success: %s (Seq: %d, Leader: Node %d)\n", reply.Message, reply.SeqNum, reply.Ballot.NodeID)
			successCount++
		} else {
			fmt.Printf("Failed: %s\n", reply.Message)
			if strings.Contains(strings.ToLower(reply.Message), "insufficient active nodes") {
				deferTxn(tx)
			}
			failCount++
		}

		time.Sleep(200 * time.Millisecond) //Slight delay has been put for clarity

	}
	currentSetIndex++
}

// printLogFromNode prompts for a node ID and fetches its log via RPC
func printLogFromNode(reader *bufio.Reader) {
	fmt.Print("Enter node ID (1-5): ")
	nodeInput, _ := reader.ReadString('\n')
	nodeID, err := strconv.Atoi(strings.TrimSpace(nodeInput))
	if err != nil || nodeID < 1 || nodeID > config.NumNodes {
		fmt.Println("❌ Invalid node ID")
		return
	}

	address, ok := config.NodeAddresses[nodeID]
	if !ok {
		fmt.Printf("❌ Unknown node ID %d\n", nodeID)
		return
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		fmt.Printf("❌ Could not connect to node %d at %s: %v\n", nodeID, address, err)
		return
	}
	defer client.Close()

	var reply string
	err = client.Call("NodeService.PrintLog", true, &reply)
	if err != nil {
		fmt.Printf("❌ RPC error calling PrintLog on node %d: %v\n", nodeID, err)
		return
	}

	fmt.Println(reply)
}

func printDBFromNode(reader *bufio.Reader) {
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

	// var balances map[string]int
	args := datatypes.PrintDBArgs{NodeID: nodeID}
	var reply datatypes.PrintDBReply
	fmt.Printf("\nRequesting database contents from Node %d..\n", nodeID)

	err = client.Call("NodeService.PrintDB", args, &reply)
	if err != nil {
		log.Printf("Error calling PrintDB on Node %d: %v", nodeID, err)
		return
	}

	fmt.Printf("Database contents from Node %d:\n", nodeID)
	fmt.Println(reply.DBContents)
	fmt.Printf("\n========================================\n")
	fmt.Printf("Requesting PrintDB from Node %d...\n", nodeID)
	fmt.Println("========================================")

	fmt.Printf("⚠️  Note: PrintDB should be called directly on Node %d terminal\n", nodeID)
	fmt.Printf("On Node %d terminal, enter command: 1 (Print Database)\n", nodeID)
	fmt.Println("========================================")
}

// printStatusFromNode prompts for node ID and sequence number, then prints transaction status (A/C/E/X)
func printStatusFromNode(reader *bufio.Reader) {

	fmt.Print("Enter sequence number: ")
	seqInput, _ := reader.ReadString('\n')
	seqNum, err := strconv.Atoi(strings.TrimSpace(seqInput))
	if err != nil || seqNum < 1 {
		fmt.Println("❌ Invalid sequence number")
		return
	}

	fmt.Printf("\n===== Status of Seq %d across all nodes =====\n", seqNum)

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address, ok := config.NodeAddresses[nodeID]
		if !ok {
			fmt.Printf("❌ Unknown node ID %d\n", nodeID)
			continue
		}
		client, err := rpc.Dial("tcp", address)
		if err != nil {
			fmt.Printf("❌ Could not connect to node %d: %v\n", nodeID, err)
			return
		}
		var reply string
		err = client.Call("NodeService.PrintStatus", seqNum, &reply)
		client.Close()

		if err != nil {
			fmt.Printf("❌ RPC error calling PrintStatus on node %d: %v\n", nodeID, err)
			return
		}

		fmt.Println(reply)
	}
}

func printViewFromAllNodes() {
	fmt.Println("===== Printing NEW-VIEW messages from all nodes =====")

	for nodeID := 1; nodeID <= config.NumNodes; nodeID++ {
		address := config.NodeAddresses[nodeID]

		client, err := rpc.Dial("tcp", address)
		if err != nil {
			fmt.Printf("❌ Node %d unreachable: %v\n", nodeID, err)
			continue
		}

		var reply string
		err = client.Call("NodeService.PrintView", true, &reply)
		client.Close()

		if err != nil {
			fmt.Printf("❌ RPC error on Node %d: %v\n", nodeID, err)
			continue
		}

		fmt.Println(reply)
	}

	fmt.Println("==============================================")
}
