package client

import (
	"fmt"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"multipaxos/rituraj735/pkg/shard"
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

	Leaders map[int]int
}

func NewClient(id string, nodeAddresses map[int]string) *Client {
	return &Client{
		ID:            id,
		CurrentLeader: 1,
		NodeAddresses: nodeAddresses,
		LastTimestamp: 0,
		Leaders:       make(map[int]int),
	}
}

func (c *Client) SendTransaction(txn datatypes.Txn) (datatypes.ReplyMsg, error) {
	c.mu.Lock()
	c.LastTimestamp = time.Now().UnixNano()
	timestamp := c.LastTimestamp
	c.mu.Unlock()

	sID := 0
	_, _ = fmt.Sscanf(txn.Sender, "%d", &sID)
	cid := shard.ClusterOfItem(sID)
	leader := c.GetLeaderForCluster(cid)
	log.Printf("Client %s: sending txn %s ts=%d (cluster=%d leader=%d)", c.ID, txn.String(), timestamp, cid, leader)

	request := datatypes.ClientRequest{
		MessageType: "REQUEST",
		Transaction: txn,
		Timestamp:   timestamp,
		ClientID:    c.ID,
		IsNoOp:      false,
	}

	reply, err := c.sendToNode(leader, request)
	if err == nil {
		if reply.Success {
			log.Printf("Client %s: cluster %d leader %d succeeded ts=%d seq=%d", c.ID, cid, leader, reply.Timestamp, reply.SeqNum)
			return reply, nil
		}

		if reply.Message == "not leader" {
			triedHint := false
			hint := reply.Ballot.NodeID
			if hint != 0 && hint != leader {
				hintedReply, herr := c.sendToNode(hint, request)
				triedHint = true
				if herr == nil && hintedReply.Success {
					c.UpdateLeaderForCluster(cid, hintedReply.Ballot.NodeID)
					log.Printf("Client %s: adopted hinted leader %d for cluster %d", c.ID, hintedReply.Ballot.NodeID, cid)
					return hintedReply, nil
				}
				if herr == nil && hintedReply.Message != "not leader" {

					return hintedReply, nil
				}
			}

			members, ok := config.ClusterMembers[cid]
			if ok {
				for _, nodeID := range members {
					address := c.NodeAddresses[nodeID]
					cli, derr := rpc.Dial("tcp", address)
					if derr != nil {
						continue
					}
					var info datatypes.LeaderInfo
					_ = cli.Call("NodeService.GetLeader", true, &info)
					cli.Close()
					if info.IsLeader && info.LeaderID != 0 {
						c.UpdateLeaderForCluster(cid, info.LeaderID)
						nr, ne := c.sendToNode(info.LeaderID, request)
						if ne == nil {
							return nr, nil
						}
					}
				}

				for _, nodeID := range members {
					if triedHint && nodeID == hint {
						continue
					}
					r, e := c.sendToNode(nodeID, request)
					if e == nil && r.Success {
						if r.Ballot.NodeID != 0 {
							c.UpdateLeaderForCluster(cid, r.Ballot.NodeID)
						}
						log.Printf("Client %s: node %d processed txn with new leader hint=%d for cluster %d", c.ID, nodeID, r.Ballot.NodeID, cid)
						return r, nil
					} else if e == nil && r.Message != "not leader" {
						return r, nil
					}
				}
			}

			log.Printf("Client %s: no leader available for txn %s", c.ID, txn.String())
			return datatypes.ReplyMsg{}, fmt.Errorf("No leader available")
		}

		return reply, nil
	}
	log.Printf("Client %s: leader %d request failed: %v", c.ID, leader, err)

	if members, ok := config.ClusterMembers[cid]; ok {
		deadline := time.Now().Add(800 * time.Millisecond)
		for time.Now().Before(deadline) {
			for _, nodeID := range members {
				addr := c.NodeAddresses[nodeID]
				cli, derr := rpc.Dial("tcp", addr)
				if derr != nil {
					continue
				}
				var info datatypes.LeaderInfo
				_ = cli.Call("NodeService.GetLeader", true, &info)
				cli.Close()
				if info.IsLeader && info.LeaderID != 0 {
					c.UpdateLeaderForCluster(cid, info.LeaderID)
					if r, e := c.sendToNode(info.LeaderID, request); e == nil {
						return r, nil
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	log.Printf("Client %s: cluster %d leader %d failed, trying cluster members\n", c.ID, cid, leader)

	if members, ok := config.ClusterMembers[cid]; ok {
		for _, nodeID := range members {
			if nodeID == leader {
				continue
			}
			reply, err := c.sendToNode(nodeID, request)
			if err == nil && reply.Success {
				c.UpdateLeaderForCluster(cid, reply.Ballot.NodeID)
				return reply, nil
			} else if reply.Message != "not leader" {
				return reply, nil
			}
		}
	}

	log.Printf("Client %s: no leader available for txn %s", c.ID, txn.String())
	return datatypes.ReplyMsg{}, fmt.Errorf("No leader available")

}

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

func (c *Client) GetCurrentLeader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CurrentLeader
}

func (c *Client) UpdateLeader(leaderID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.CurrentLeader = leaderID

}

func (c *Client) GetLeaderForCluster(cid int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if id, ok := c.Leaders[cid]; ok && id != 0 {
		return id
	}
	switch cid {
	case 1:
		return 1
	case 2:
		return 4
	case 3:
		return 7
	default:
		return c.CurrentLeader
	}
}

func (c *Client) UpdateLeaderForCluster(cid, leaderID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Leaders == nil {
		c.Leaders = make(map[int]int)
	}
	c.Leaders[cid] = leaderID
}
