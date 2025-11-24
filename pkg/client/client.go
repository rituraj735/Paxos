// =======================================
// File: pkg/client/client.go
// Description: Client implementation that submits transactions, tracks leaders, and retries on failure.
// =======================================
package client

import (
	"fmt"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"net/rpc"
	"sync"
	"time"
)

type Client struct {
	ID            string
	CurrentLeader int
	NodeAddresses map[int]string
	LastTimestamp int64
	mu            sync.Mutex
}

func NewClient(id string, nodeAddresses map[int]string) *Client {
	return &Client{
		ID:            id,
		CurrentLeader: 1,
		NodeAddresses: nodeAddresses,
		LastTimestamp: 0,
	}
}

func (c *Client) SendTransaction(txn datatypes.Txn) (datatypes.ReplyMsg, error) {
	c.mu.Lock()
	c.LastTimestamp = time.Now().UnixNano()
	timestamp := c.LastTimestamp
	c.mu.Unlock()

	request := datatypes.ClientRequest{
		MessageType: "REQUEST",
		Transaction: txn,
		Timestamp:   timestamp,
		ClientID:    c.ID,
		IsNoOp:      false,
	}

	// Send the request to the current leader first
	reply, err := c.sendToNode(c.CurrentLeader, request)
	if err == nil {
		return reply, nil
	}

	//If the request to current leader fails, try other nodes in a broadcast manner
	log.Printf("Client %s: Leader %d failed, trying all nodes now\n", c.ID, c.CurrentLeader)

	for nodeID := range c.NodeAddresses {
		if nodeID == c.CurrentLeader {
			continue // Skip the current leader as we've already tried it
		}
		reply, err := c.sendToNode(nodeID, request)
		if err == nil && reply.Success {
			c.mu.Lock()
			c.CurrentLeader = reply.Ballot.NodeID
			c.mu.Unlock()
			return reply, nil
		} else if reply.Message != "not leader" {
			return reply, nil
		}
	}

	return datatypes.ReplyMsg{}, fmt.Errorf("No leader available")

}

func (c *Client) sendToNode(nodeID int, request datatypes.ClientRequest) (datatypes.ReplyMsg, error) {
	address, exists := c.NodeAddresses[nodeID]
	if !exists {
		return datatypes.ReplyMsg{}, fmt.Errorf("Node %d address not found", nodeID)
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return datatypes.ReplyMsg{}, err
	}

	defer client.Close()

	args := datatypes.ClientRequestRPC{Request: request}
	var reply datatypes.ClientReplyRPC

	done := make(chan error, 1)
	go func() {
		done <- client.Call("NodeService.HandleClientRequest", args, &reply)
	}()

	select {
	case err := <-done:
		if err != nil {
			return datatypes.ReplyMsg{}, err
		}
		return reply.Reply, nil
	case <-time.After(config.ClientTimeout * time.Millisecond):
		return datatypes.ReplyMsg{}, fmt.Errorf("Timeout while waiting for response from Node %d", nodeID)
	}
}

func (c *Client) GetCurrentLeader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CurrentLeader
}

func (c *Client) UpdateLeader(leaderID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CurrentLeader = leaderID
	log.Printf("Client %s: leader updated to Node %d", c.ID, leaderID)
}
