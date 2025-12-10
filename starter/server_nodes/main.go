package main

import (
	"fmt"
	"io"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/pkg/node"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run main.go <node_id>")
		fmt.Println("Example: go run main.go 1")
		return
	}

	nodeID, err := strconv.Atoi(os.Args[1])
	if err != nil || nodeID < 1 || nodeID > config.NumNodes {
		fmt.Println("Invalid node ID. It should be between 1 and", config.NumNodes)
		os.Exit(1)
	}

	logDir := "logs"
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		log.Fatalf("failed to create log directory: %v", err)
	}
	logPath := fmt.Sprintf("%s/node-%d.log", logDir, nodeID)
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("failed to open log file %s: %v", logPath, err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix(fmt.Sprintf("[Node %d] ", nodeID))

	n := node.NewNode(nodeID, config.NodeAddresses[nodeID], config.NodeAddresses)

	err = n.StartRPCServer()
	if err != nil {
		log.Fatalf("Failed to start node %d: %v", nodeID, err)
	}

	//log.Printf("Node %d started at %s\n", nodeID, config.NodeAddresses[nodeID])

	// time.Sleep(2 * time.Second)

	// Node 1 starts first leader election
	if nodeID == 1 {
		log.Printf("Node %d: Starting initial leader election\n", nodeID)
		n.StartLeaderElection()
	}

	// Interactive command loop
	// go func() {
	// 	reader := bufio.NewReader(os.Stdin)
	// 	for {
	// 		fmt.Printf("\n=== Node %d Commands ===\n", nodeID)
	// 		fmt.Println("1. Print Database")
	// 		fmt.Println("2. Print Log")
	// 		fmt.Println("3. Print Status (seq num)")
	// 		fmt.Println("4. Print View")
	// 		fmt.Println("5. Start Leader Election")
	// 		fmt.Println("6. Check if Leader")
	// 		fmt.Print("Command: ")

	// 		cmd, _ := reader.ReadString('\n')
	// 		cmd = strings.TrimSpace(cmd)

	// 		switch cmd {
	// 		case "1":
	// 			n.Database.PrintDB(nodeID)

	// 		case "2":
	// 			n.PrintLog()

	// 		case "3":
	// 			fmt.Print("Enter sequence number: ")
	// 			seqStr, _ := reader.ReadString('\n')
	// 			seqNum, err := strconv.Atoi(strings.TrimSpace(seqStr))
	// 			if err != nil {
	// 				fmt.Println("Invalid sequence number")
	// 				continue
	// 			}
	// 			status := n.PrintStatus(seqNum)
	// 			fmt.Printf("Node %d, Sequence %d: Status %s\n", nodeID, seqNum, status)

	// 		case "4":
	// 			n.PrintView()

	// 		case "5":
	// 			success := n.StartLeaderElection()
	// 			if success {
	// 				fmt.Printf("Node %d is now the leader\n", nodeID)
	// 			} else {
	// 				fmt.Printf("Node %d failed to become leader\n", nodeID)
	// 			}

	// 		case "6":
	// 			if n.GetIsLeader() {
	// 				fmt.Printf("Node %d is the LEADER (ballot: %s)\n", nodeID, n.GetCurrentBallot())
	// 			} else {
	// 				fmt.Printf("Node %d is a BACKUP\n", nodeID)
	// 			}

	// 		default:
	// 			if cmd != "" {
	// 				fmt.Println("Invalid command")
	// 			}
	// 		}
	// 	}
	// }()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan
	log.Printf("Node %d shutting down...\n", nodeID)
	n.Stop()
}
