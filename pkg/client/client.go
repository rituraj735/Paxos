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

// NewClient constructs a client bound to known node addresses.
func NewClient(id string, nodeAddresses map[int]string) *Client {
	return &Client{
		ID:            id,
		CurrentLeader: 1,
		NodeAddresses: nodeAddresses,
		LastTimestamp: 0,
	}
}

// SendTransaction submits a txn to the current leader with broadcast fallback.
func (c *Client) SendTransaction(txn datatypes.Txn) (datatypes.ReplyMsg, error) {
	c.mu.Lock()
	c.LastTimestamp = time.Now().UnixNano()
	timestamp := c.LastTimestamp
	c.mu.Unlock()

	log.Printf("Client %s: sending txn %s with ts=%d (leader=%d)", c.ID, txn.String(), timestamp, c.GetCurrentLeader())

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
        if reply.Success {
            log.Printf("Client %s: leader %d succeeded for ts=%d seq=%d", c.ID, c.CurrentLeader, reply.Timestamp, reply.SeqNum)
            return reply, nil
        }

        // Minimal fallback: if not leader, try hinted leader once, then broadcast
        if reply.Message == "not leader" {
            triedHint := false
            hint := reply.Ballot.NodeID
            if hint != 0 && hint != c.CurrentLeader {
                hintedReply, herr := c.sendToNode(hint, request)
                triedHint = true
                if herr == nil && hintedReply.Success {
                    c.mu.Lock()
                    c.CurrentLeader = hintedReply.Ballot.NodeID
                    c.mu.Unlock()
                    log.Printf("Client %s: adopted hinted leader %d", c.ID, hintedReply.Ballot.NodeID)
                    return hintedReply, nil
                }
                if herr == nil && hintedReply.Message != "not leader" {
                    // e.g., insufficient active nodes — bubble up immediately
                    return hintedReply, nil
                }
            }

            // Try other nodes (skip current leader and tried hint if any)
            for nodeID := range c.NodeAddresses {
                if nodeID == c.CurrentLeader || (triedHint && nodeID == hint) {
                    continue
                }
                r, e := c.sendToNode(nodeID, request)
                if e == nil && r.Success {
                    // update leader if hinted
                    if r.Ballot.NodeID != 0 {
                        c.mu.Lock()
                        c.CurrentLeader = r.Ballot.NodeID
                        c.mu.Unlock()
                    }
                    log.Printf("Client %s: node %d processed txn with new leader hint=%d", c.ID, nodeID, r.Ballot.NodeID)
                    return r, nil
                } else if e == nil && r.Message != "not leader" {
                    // return first definitive failure (e.g., insufficient active nodes)
                    return r, nil
                }
            }

            log.Printf("Client %s: no leader available for txn %s", c.ID, txn.String())
            return datatypes.ReplyMsg{}, fmt.Errorf("No leader available")
        }

        // For other non-success replies, keep old behavior (return immediately)
        return reply, nil
    }
    log.Printf("Client %s: leader %d request failed: %v", c.ID, c.CurrentLeader, err)

    //If the request to current leader fails, try other nodes in a broadcast manner
    log.Printf("Client %s: Leader %d failed, trying all nodes now\n", c.ID, c.CurrentLeader)

    for nodeID := range c.NodeAddresses {
        if nodeID == c.CurrentLeader {
            continue // Skip the current leader as we've already tried it
        }
        reply, err := c.sendToNode(nodeID, request)
        if err == nil && reply.Success {
            log.Printf("Client %s: node %d processed txn with new leader hint=%d", c.ID, nodeID, reply.Ballot.NodeID)
            c.mu.Lock()
            c.CurrentLeader = reply.Ballot.NodeID
            c.mu.Unlock()
            return reply, nil
        } else if reply.Message != "not leader" {
            return reply, nil
        }
    }

    log.Printf("Client %s: no leader available for txn %s", c.ID, txn.String())
    return datatypes.ReplyMsg{}, fmt.Errorf("No leader available")

}

// sendToNode performs the RPC to a specific node with timeout protection.
func (c *Client) sendToNode(nodeID int, request datatypes.ClientRequest) (datatypes.ReplyMsg, error) {
	address, exists := c.NodeAddresses[nodeID]
	if !exists {
		log.Printf("Client %s: node %d address missing", c.ID, nodeID)
		return datatypes.ReplyMsg{}, fmt.Errorf("Node %d address not found", nodeID)
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Printf("Client %s: dial to node %d(%s) failed: %v", c.ID, nodeID, address, err)
		return datatypes.ReplyMsg{}, err
	}

	defer client.Close()
	log.Printf("Client %s: sending request to node %d seq? pending ts=%d", c.ID, nodeID, request.Timestamp)

	args := datatypes.ClientRequestRPC{Request: request}
	var reply datatypes.ClientReplyRPC

	done := make(chan error, 1)
	go func() {
		done <- client.Call("NodeService.HandleClientRequest", args, &reply)
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("Client %s: node %d RPC error: %v", c.ID, nodeID, err)
			return datatypes.ReplyMsg{}, err
		}
		log.Printf("Client %s: node %d replied success=%v seq=%d", c.ID, nodeID, reply.Reply.Success, reply.Reply.SeqNum)
		return reply.Reply, nil
	case <-time.After(config.ClientTimeout * time.Millisecond):
		log.Printf("Client %s: timeout talking to node %d", c.ID, nodeID)
		return datatypes.ReplyMsg{}, fmt.Errorf("Timeout while waiting for response from Node %d", nodeID)
	}
}

// GetCurrentLeader returns the leader ID the client believes in.
func (c *Client) GetCurrentLeader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CurrentLeader
}

// UpdateLeader updates the local cached leader ID.
func (c *Client) UpdateLeader(leaderID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CurrentLeader = leaderID
	log.Printf("Client %s: leader updated to Node %d", c.ID, leaderID)
}
