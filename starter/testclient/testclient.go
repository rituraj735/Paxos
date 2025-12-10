package main

import (
	"flag"
	"fmt"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"net/rpc"
	"time"
)

func GetBalance(nodeID int, accountID int) (int, error) {
	addr, ok := config.NodeAddresses[nodeID]
	if !ok {
		return 0, fmt.Errorf("unknown node ID %d", nodeID)
	}

	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return 0, fmt.Errorf("dial error: %v", err)
	}
	defer client.Close()

	args := datatypes.GetBalanceArgs{AccountID: fmt.Sprintf("%d", accountID)}
	var reply datatypes.GetBalanceReply
	if err := client.Call("NodeService.GetBalance", args, &reply); err != nil {
		return 0, fmt.Errorf("GetBalance RPC error: %v", err)
	}
	return reply.Balance, nil

}

var (
	nodeID      = flag.Int("node", 2, "target node ID (1-9)")
	account     = flag.String("account", "1", "account ID to query (1..9000)")
	leaderID    = flag.Int("leader", 1, "node ID to send requests to")
	senderAcc   = flag.String("sender", "100", "sender account ID")
	receiverAcc = flag.String("receiver", "200", "receiver account ID")
	amount      = flag.Int("amount", 3, "transfer amount")
	bankTxn     = flag.Bool("bank", true, "use MessageType=BANK_TXN")
	concurrent  = flag.Int("concurrent", 1, "number of concurrent requests")
	seq         = flag.Int("seq", 0, "Paxos sequence number to inspect via PrintStatus")
)

func main() {
	flag.Parse()

	addr, ok := config.NodeAddresses[*nodeID]
	if !ok {
		log.Fatalf("Unknown node ID %d", *nodeID)
	}

	log.Printf("Connecting to node %d at %s", *nodeID, addr)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial error: %v", err)
	}
	defer client.Close()

	if *seq > 0 {
		var status string
		if err := client.Call("NodeService.PrintStatus", *seq, &status); err != nil {
			log.Fatalf("PrintStatus RPC error: %v", err)
		}
		fmt.Printf("Node %d: %s\n", *nodeID, status)
		return
	}

	// Invoke GetBalance RPC
	// 	args := datatypes.GetBalanceArgs{AccountID: *account}
	// 	var reply datatypes.GetBalanceReply
	// 	if err := client.Call("NodeService.GetBalance", args, &reply); err != nil {
	// 		log.Fatalf("GetBalance RPC error: %v", err)
	// 	}
	// 	fmt.Printf("Node %d: Account %s balance = %d\n", *nodeID, *account, reply.Balance)
	// }
	for i := 1; i <= 9; i++ {
		balance, err := GetBalance(i, 100)
		if err != nil {
			log.Fatalf("GetBalance failed: %v", err)
		}
		log.Printf("Account 100 balance at Node %d = %d", i, balance)
	}
	for i := 1; i <= 9; i++ {
		balance, err := GetBalance(i, 200)
		if err != nil {
			log.Fatalf("GetBalance failed: %v", err)
		}
		log.Printf("Account 4000 balance at Node %d = %d", i, balance)
	}
	for i := 1; i <= 9; i++ {
		balance, err := GetBalance(i, 4000)
		if err != nil {
			log.Fatalf("GetBalance failed: %v", err)
		}
		log.Printf("Account 4000 balance at Node %d = %d", i, balance)
	}

	// Simple single-threaded case
	// if *concurrent == 1 {
	// 	sendOne(addr)
	// 	return
	// }

	// // Concurrent stress test
	// done := make(chan struct{}, *concurrent)
	// for i := 0; i < *concurrent; i++ {
	// 	go func(idx int) {
	// 		log.Printf("[goroutine %d] sending request", idx)
	// 		sendOne(addr)
	// 		done <- struct{}{}
	// 	}(i)
	// }

	// for i := 0; i < *concurrent; i++ {
	// 	<-done
	// }

	// for i := 1; i <= 9; i++ {
	// 	balance, err := GetBalance(i, 100)
	// 	if err != nil {
	// 		log.Fatalf("GetBalance failed: %v", err)
	// 	}
	// 	log.Printf("Account 100 balance after txn at Node %d = %d", i, balance)
	// }

	// for i := 1; i <= 9; i++ {
	// 	balance, err := GetBalance(i, 4000)
	// 	if err != nil {
	// 		log.Fatalf("GetBalance failed: %v", err)
	// 	}
	// 	log.Printf("Account 4000 balance at Node %d = %d", i, balance)
	// }

}

func sendOne(addr string) {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer client.Close()

	msgType := ""
	if *bankTxn {
		msgType = "BANK_TXN"
	}

	req := datatypes.ClientRequest{
		ClientID:    fmt.Sprintf("testclient-%d", time.Now().UnixNano()),
		Timestamp:   time.Now().UnixNano(),
		MessageType: msgType,
		Transaction: datatypes.Txn{
			Sender:   *senderAcc,
			Receiver: *receiverAcc,
			Amount:   *amount,
		},
		IsNoOp: false,
	}

	rpcArgs := datatypes.ClientRequestRPC{Request: req}
	var rpcReply datatypes.ClientReplyRPC

	err = client.Call("NodeService.HandleClientRequest", rpcArgs, &rpcReply)
	if err != nil {
		log.Printf("RPC error: %v", err)
		return
	}

	rep := rpcReply.Reply
	log.Printf("Reply: success=%v msg=%q ballot=%v seq=%d",
		rep.Success, rep.Message, rep.Ballot, rep.SeqNum)
}
