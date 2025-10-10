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
			fmt.Printf("%+v\n", sets)
			fmt.Println("PrintLog()")
			printLogFromNode(filePathReader)
		case 3:
			fmt.Println("PrintDB()")
			printDBFromNode(filePathReader)
		case 4:
			fmt.Println("PrintStatus()")
			printStatusFromNode(filePathReader)
		case 5:
			fmt.Println("PrintView()")
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
		txnParts := strings.Split(txnStr, ",")     // Splits A,B,3 into [A B 3]

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

	successCount := 0
	failCount := 0
	for i, tx := range currentSet.Txns {
		fmt.Printf("\n[%d/%d] Transaction: %s\n", i+1, len(currentSet.Txns), tx)

		c, exists := clients[tx.Sender]
		if !exists {
			fmt.Printf("Client %s not found\n", tx.Sender)
			failCount++
			continue
		}

		reply, err := c.SendTransaction(tx)
		if err != nil {
			fmt.Printf("Transaction failed: %v\n", err)
			failCount++
		} else if reply.Success {
			fmt.Printf(" Success: %s (Seq: %d, Leader: Node %d)\n", reply.Message, reply.Ballot.NodeID)
			successCount++
		} else {
			fmt.Printf("Failed: %s\n", reply.Message)
			failCount++
		}

		time.Sleep(200 * time.Millisecond) //Slight delay has been put for clarity

	}
	currentSetIndex++
}

func printLogFromNode(reader *bufio.Reader) {
	fmt.Print("Enter node ID (1-5): ")
	nodeInput, _ := reader.ReadString('\n')
	nodeID, err := strconv.Atoi(strings.TrimSpace(nodeInput))
	if err != nil || nodeID < 1 || nodeID > config.NumNodes {
		fmt.Println("❌ Invalid node ID")
		return
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("Requesting PrintLog from Node %d...\n", nodeID)
	fmt.Println("========================================")

	fmt.Printf("⚠️  Note: PrintLog should be called directly on Node %d terminal\n", nodeID)
	fmt.Printf("On Node %d terminal, enter command: 2 (Print Log)\n", nodeID)
	fmt.Println("========================================")
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

func printStatusFromNode(reader *bufio.Reader) {
	fmt.Print("Enter node ID (1-5): ")
	nodeInput, _ := reader.ReadString('\n')
	nodeID, err := strconv.Atoi(strings.TrimSpace(nodeInput))
	if err != nil || nodeID < 1 || nodeID > config.NumNodes {
		fmt.Println("❌ Invalid node ID")
		return
	}

	fmt.Print("Enter sequence number: ")
	seqInput, _ := reader.ReadString('\n')
	seqNum, err := strconv.Atoi(strings.TrimSpace(seqInput))
	if err != nil {
		fmt.Println("❌ Invalid sequence number")
		return
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("Requesting PrintStatus (Seq: %d) from Node %d...\n", seqNum, nodeID)
	fmt.Println("========================================")

	fmt.Printf("⚠️  Note: PrintStatus should be called directly on Node %d terminal\n", nodeID)
	fmt.Printf("On Node %d terminal:\n", nodeID)
	fmt.Printf("  1. Enter command: 3 (Print Status)\n")
	fmt.Printf("  2. Enter sequence number: %d\n", seqNum)
	fmt.Println("========================================")
}

func printViewFromNode(reader *bufio.Reader) {
	fmt.Print("Enter node ID (1-5): ")
	nodeInput, _ := reader.ReadString('\n')
	nodeID, err := strconv.Atoi(strings.TrimSpace(nodeInput))
	if err != nil || nodeID < 1 || nodeID > config.NumNodes {
		fmt.Println("❌ Invalid node ID")
		return
	}

	fmt.Printf("\n========================================\n")
	fmt.Printf("Requesting PrintView from Node %d...\n", nodeID)
	fmt.Println("========================================")

	fmt.Printf("⚠️  Note: PrintView should be called directly on Node %d terminal\n", nodeID)
	fmt.Printf("On Node %d terminal, enter command: 4 (Print View)\n", nodeID)
	fmt.Println("========================================")
}
