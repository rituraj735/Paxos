// =======================================
// File: pkg/node/node.go
// Description: Multi-Paxos replica with leader election, RPC handlers, log management, and execution.
// =======================================
package node

import (
	"fmt"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"multipaxos/rituraj735/pkg/database"
	"net"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"
)

type Node struct {
	ID       int
	Address  string
	Peers    map[int]string
	IsLeader bool

	lastProcessedView datatypes.BallotNumber

	acceptedFromNewViewCount int

	ackFromNewView map[int]bool

	lastLeaderMsg time.Time

	electionCoolDown time.Time

	CurrentBallot   datatypes.BallotNumber
	HighestPromised datatypes.BallotNumber
	NextSeqNum      int

	AcceptedLog map[int]datatypes.LogEntry
	RequestLog  []datatypes.LogEntry
	NewViewMsgs []datatypes.NewViewMsg

	LastReply map[string]datatypes.ReplyMsg
	Database  *database.Database

	rpcServer *rpc.Server
	listener  net.Listener

	mu sync.RWMutex

	pendingAccepts map[int]map[int]datatypes.AcceptedMsg

	ActiveNodes  map[int]bool
	MajoritySize int

	shutdown chan bool
}

type NodeService struct {
	node *Node
}

// statusRank assigns ordering weights to request statuses.
func statusRank(status datatypes.RequestStatus) int {
	switch status {
	case datatypes.StatusExecuted:
		return 3
	case datatypes.StatusCommitted:
		return 2
	case datatypes.StatusAccepted:
		return 1
	default:
		return 0
	}
}

// maxStatus returns whichever status reflects the furthest progress.
func maxStatus(a, b datatypes.RequestStatus) datatypes.RequestStatus {
	if statusRank(b) > statusRank(a) {
		return b
	}
	return a
}

// NewNode wires up a node with default state, DB, and leader monitor.
func NewNode(id int, address string, peers map[int]string) *Node {
	log.Printf("Node %d: initializing at %s with %d peers", id, address, len(peers))
	node := &Node{
		ID:              id,
		Address:         address,
		Peers:           peers,
		IsLeader:        false,
		CurrentBallot:   datatypes.BallotNumber{Number: 0, NodeID: id},
		HighestPromised: datatypes.BallotNumber{Number: 0, NodeID: 0},
		NextSeqNum:      1,
		AcceptedLog:     make(map[int]datatypes.LogEntry),
		RequestLog:      make([]datatypes.LogEntry, 0),
		NewViewMsgs:     make([]datatypes.NewViewMsg, 0),
		LastReply:       make(map[string]datatypes.ReplyMsg),
		Database:        database.NewDatabase(),
		pendingAccepts:  make(map[int]map[int]datatypes.AcceptedMsg),
		ActiveNodes:     make(map[int]bool),
		MajoritySize:    config.MajoritySize,
		shutdown:        make(chan bool),
		lastLeaderMsg:   time.Now(),
		ackFromNewView:  make(map[int]bool),
	}
	go node.monitorLeaderTimeout()
	// Initialize all clients with initial balance of 10
	for _, clientID := range config.ClientIDs {
		node.Database.InitializeClient(clientID, config.InitialBalance)
	}

	// Initially all nodes are active
	for nodeID := range peers {
		node.ActiveNodes[nodeID] = true
	}
	node.ActiveNodes[id] = true

	log.Printf("Node %d: initialization complete (maj=%d)", id, node.MajoritySize)
	return node
}

// monitorLeaderTimeout watches for missing leader messages and triggers elections.
func (n *Node) monitorLeaderTimeout() {
	log.Printf("Node %d: leader timeout monitor running", n.ID)
	for {
		time.Sleep(200 * time.Millisecond)
		n.mu.RLock()
		isLeader := n.IsLeader
		last := n.lastLeaderMsg
		cooldown := n.electionCoolDown
		n.mu.RUnlock()

		if isLeader {
			n.mu.Lock()
			n.lastLeaderMsg = time.Now() // the leader keeps its own timer fresh
			n.mu.Unlock()
			continue
		}

		if time.Since(cooldown) < 2*time.Second {
			continue
		}

		if time.Since(last) > time.Duration(config.LeaderTimeout)*time.Millisecond {
			log.Printf("Node %d: Leader timeout (%1fs), no leader msgs, starting election\n", n.ID, time.Since(last).Seconds())
			n.mu.Lock()
			n.electionCoolDown = time.Now()
			n.mu.Unlock()

			n.mu.RLock()
			if n.IsLeader {
				n.mu.RUnlock()
				continue
			}
			n.mu.RUnlock()
			success := n.StartLeaderElection()

			if success {
				log.Printf("Node %d: Became leader after election, successful\n", n.ID)
				go n.sendHeartbeats()
			}
		}

		select {
		case <-n.shutdown:
			log.Printf("Node %d: leader timeout monitor exiting", n.ID)
			return
		default:
		}
	}
}

// StartRPCServer registers RPC handlers and begins accepting connections.
func (n *Node) StartRPCServer() error {
	n.rpcServer = rpc.NewServer()
	service := &NodeService{node: n}
	n.rpcServer.Register(service)
	log.Printf("Node %d: starting RPC server at %s", n.ID, n.Address)
	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		log.Printf("Node %d: failed to listen on %s: %v", n.ID, n.Address, err)
		return err
	}
	n.listener = listener

	go func() {
		for {
			select {
			case <-n.shutdown:
				log.Printf("Node %d: RPC server shutting down", n.ID)
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					log.Printf("Node %d: accept error: %v", n.ID, err)
					continue
				}
				go n.rpcServer.ServeConn(conn)
			}
		}
	}()

	return nil
}

// Stop shuts down network listeners and background goroutines.
func (n *Node) Stop() {
	log.Printf("Node %d: stopping node", n.ID)
	close(n.shutdown)
	if n.listener != nil {
		n.listener.Close()
	}
}

// callRPC performs a best-effort RPC to a peer respecting active-node status.
func (n *Node) callRPC(nodeID int, method string, args interface{}, reply interface{}) error {
	n.mu.RLock()
	if !n.ActiveNodes[nodeID] {
		n.mu.RUnlock()
		log.Printf("Node %d: skipping RPC %s to inactive node %d", n.ID, method, nodeID)
		return fmt.Errorf("node %d is not active", nodeID)
	}
	n.mu.RUnlock()
	log.Printf("Node %d: RPC %s->node %d", n.ID, method, nodeID)

	address, exists := n.Peers[nodeID]
	if !exists {
		return fmt.Errorf("unknown node %d", nodeID)
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		log.Printf("Node %d: RPC dial %s to node %d failed: %v", n.ID, method, nodeID, err)
		return err
	}
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		done <- client.Call("NodeService."+method, args, reply)
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("Node %d: RPC %s to node %d error: %v", n.ID, method, nodeID, err)
		}
		return err
	case <-time.After(1 * time.Second):
		log.Printf("Node %d: RPC %s to node %d timed out", n.ID, method, nodeID)
		return fmt.Errorf("RPC timeout")
	}
}

// GetLeader reports the node's view of the current leader and ballot.
func (s *NodeService) GetLeader(_ bool, reply *datatypes.LeaderInfo) error {
	log.Printf("Node %d: GetLeader RPC invoked", s.node.ID)
	s.node.mu.RLock()
	defer s.node.mu.RUnlock()

	now := time.Now()
	leaderID := 0

	if s.node.IsLeader && s.node.ActiveNodes[s.node.ID] {
		leaderID = s.node.ID
	} else {

		if s.node.ActiveNodes[s.node.CurrentBallot.NodeID] &&
			now.Sub(s.node.lastLeaderMsg) <= time.Duration(config.LeaderTimeout)*time.Millisecond {
			leaderID = s.node.CurrentBallot.NodeID
		} else {
			leaderID = 0
		}
	}

	*reply = datatypes.LeaderInfo{
		LeaderID: leaderID,
		Ballot:   s.node.CurrentBallot,
		IsLeader: s.node.IsLeader && s.node.ActiveNodes[s.node.ID],
	}
	return nil
}

// HandleClientRequest routes client traffic into the node's consensus pipeline.
func (ns *NodeService) HandleClientRequest(args datatypes.ClientRequestRPC, reply *datatypes.ClientReplyRPC) error {
	//fmt.Println("something reached handleClientRequest", args)
	log.Printf("Node %d: HandleClientRequest type=%s client=%s ts=%d", ns.node.ID, args.Request.MessageType, args.Request.ClientID, args.Request.Timestamp)
	replyMsg := ns.node.ProcessClientRequest(args.Request)
	reply.Reply = replyMsg
	return nil
}

// Prepare handles Phase-1 prepare RPCs during leader election.
func (ns *NodeService) Prepare(args datatypes.PrepareMsg, reply *datatypes.PromiseMsg) error {
	log.Printf("Node %d: Prepare RPC from ballot (%d,%d)", ns.node.ID, args.Ballot.Number, args.Ballot.NodeID)
	return ns.node.HandlePrepare(args, reply)
}

// Accept handles Phase-2 accept RPCs from the leader.
func (ns *NodeService) Accept(args datatypes.AcceptMsg, reply *datatypes.AcceptedMsg) error {
	log.Printf("Node %d: Accept RPC seq=%d ballot=(%d,%d)", ns.node.ID, args.SeqNum, args.Ballot.Number, args.Ballot.NodeID)
	return ns.node.HandleAccept(args, reply)
}

// Commit records a leader's commit notification for a sequence number.
func (ns *NodeService) Commit(args datatypes.CommitMsg, reply *bool) error {
	log.Printf("Node %d: Commit RPC seq=%d ballot=(%d,%d)", ns.node.ID, args.SeqNum, args.Ballot.Number, args.Ballot.NodeID)
	return ns.node.HandleCommit(args, reply)
}

// NewView ingests the leader's accepted log during view changes.
func (ns *NodeService) NewView(args datatypes.NewViewMsg, reply *bool) error {
	log.Printf("Node %d: NewView RPC ballot=(%d,%d) entries=%d", ns.node.ID, args.Ballot.Number, args.Ballot.NodeID, len(args.AcceptLog))
	return ns.node.HandleNewView(args, reply)
}

// AcceptedFromNewView acknowledges acceptance of entries during recovery.
func (ns *NodeService) AcceptedFromNewView(args datatypes.AcceptedMsg, reply *datatypes.AcceptedMsg) error {
	log.Printf("Node %d: AcceptedFromNewView from node %d seq=%d", ns.node.ID, args.NodeID, args.SeqNum)
	ns.node.mu.Lock()
	defer ns.node.mu.Unlock()

	if args.Ballot.Number != ns.node.CurrentBallot.Number ||
		args.Ballot.NodeID != ns.node.CurrentBallot.NodeID {
		*reply = args
		return nil
	}

	if ns.node.pendingAccepts[args.SeqNum] == nil {
		ns.node.pendingAccepts[args.SeqNum] = make(map[int]datatypes.AcceptedMsg)
	}

	if _, exists := ns.node.pendingAccepts[args.SeqNum][args.NodeID]; !exists {
		ns.node.pendingAccepts[args.SeqNum][args.NodeID] = args
	}

	if ns.node.ackFromNewView == nil {
		ns.node.ackFromNewView = make(map[int]bool)
	}

	if !ns.node.ackFromNewView[args.NodeID] {
		ns.node.ackFromNewView[args.NodeID] = true
		ns.node.acceptedFromNewViewCount++
		//log.Printf("Node %d ACCEPTED_FROM_NEWVIEW from Node %d (first ack for ballot (%d,%d))",
		//	ns.node.ID, args.NodeID, args.Ballot.Number, args.Ballot.NodeID)
	}

	*reply = args
	return nil
}

// RequestStateTransfer lets lagging nodes fetch a snapshot from the leader.
func (ns *NodeService) RequestStateTransfer(args datatypes.StateTransferArgs, reply *datatypes.StateTransferReply) error {
	log.Printf("Node %d: RequestStateTransfer from node %d", ns.node.ID, args.RequesterID)
	ns.node.mu.RLock()
	isLeaderActive := ns.node.IsLeader && ns.node.ActiveNodes[ns.node.ID]
	ns.node.mu.RUnlock()

	if !isLeaderActive {
		reply.Success = false
		return nil
	}

	snapshot := ns.node.buildStateSnapshot()
	reply.Snapshot = snapshot
	reply.Success = true
	return nil
}

// UpdateActiveStatus toggles a node's liveness entry and triggers sync/election.
func (s *NodeService) UpdateActiveStatus(args datatypes.UpdateNodeArgs, reply *bool) error {
	log.Printf("Node %d: UpdateActiveStatus node=%d live=%v", s.node.ID, args.NodeID, args.IsLive)
	s.node.mu.Lock()
	leaderDemoted, activated := s.node.setNodeLiveness(args.NodeID, args.IsLive)
	isLeaderActive := s.node.IsLeader && s.node.ActiveNodes[s.node.ID]
	//activeSnapshot := fmt.Sprintf("%v", s.node.ActiveNodes)
	selfID := s.node.ID
	s.node.mu.Unlock()

	*reply = true
	//log.Printf("Node %d: Active status set to %v", s.node.ID, args.IsLive)
	//log.Printf("Node %d: Active status updated -> %v", s.node.ID, activeSnapshot)

	if leaderDemoted && !args.IsLive && selfID != args.NodeID {
		go s.node.StartLeaderElection()
	}

	if activated {
		if args.NodeID == selfID {
			go s.node.requestStateSync()
		} else if isLeaderActive {
			go s.node.sendStateSnapshot(args.NodeID)
		}
	}
	return nil
}

// UpdateActiveStatusForBulk applies liveness changes for many nodes at once.
func (s *NodeService) UpdateActiveStatusForBulk(args datatypes.UpdateClusterStatusArgs, reply *bool) error {
	log.Printf("Node %d: Bulk active update request (%d entries)", s.node.ID, len(args.Active))
	s.node.mu.Lock()
	leaderDemoted := false
	activatedNodes := make([]int, 0)
	for id, live := range args.Active {
		ld, activated := s.node.setNodeLiveness(id, live)
		if ld && !live && s.node.ID != id {
			leaderDemoted = true
		}
		if activated {
			activatedNodes = append(activatedNodes, id)
		}
	}
	isLeaderActive := s.node.IsLeader && s.node.ActiveNodes[s.node.ID]
	activeSnapshot := fmt.Sprintf("%v", s.node.ActiveNodes)
	selfID := s.node.ID
	s.node.mu.Unlock()

	*reply = true
	log.Printf("Node %d: Bulk active status update -> %v", s.node.ID, activeSnapshot)

	if leaderDemoted {
		go s.node.StartLeaderElection()
	}

	for _, id := range activatedNodes {
		if id == selfID {
			go s.node.requestStateSync()
		} else if isLeaderActive {
			go s.node.sendStateSnapshot(id)
		}
	}

	return nil
}

// Utility Methods
// SetActiveNodes replaces the active set with the provided IDs.
func (n *Node) SetActiveNodes(activeNodeIDs []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	log.Printf("Node %d: SetActiveNodes %v", n.ID, activeNodeIDs)

	for nodeID := range n.ActiveNodes {
		n.ActiveNodes[nodeID] = false
	}

	for _, nodeID := range activeNodeIDs {
		n.ActiveNodes[nodeID] = true
	}

	if !n.ActiveNodes[n.ID] {
		n.IsLeader = false
	}
}

// calcMajorityFromActive computes majority threshold from a count.
func (n *Node) calcMajorityFromActive(activeCount int) int {
	if activeCount <= 0 {
		if n.MajoritySize > 0 {
			return n.MajoritySize
		}
		return 1
	}
	maj := activeCount/2 + 1
	if maj < 1 {
		maj = 1
	}
	log.Printf("Node %d: calc majority from active=%d => %d", n.ID, activeCount, maj)
	return maj
}

// majorityThresholdLocked counts active nodes and returns threshold (locked).
func (n *Node) majorityThresholdLocked() int {
	activeCount := 0
	for _, live := range n.ActiveNodes {
		if live {
			activeCount++
		}
	}
	threshold := n.calcMajorityFromActive(activeCount)
	log.Printf("Node %d: majorityThresholdLocked active=%d threshold=%d", n.ID, activeCount, threshold)
	return threshold
}

// majorityThreshold safely returns the majority threshold.
func (n *Node) majorityThreshold() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.majorityThresholdLocked()
}

// setNodeLiveness updates a node's status and returns leaderDemoted/becameActive.
func (n *Node) setNodeLiveness(nodeID int, isLive bool) (bool, bool) {
	log.Printf("Node %d: setNodeLiveness node=%d live=%v", n.ID, nodeID, isLive)
	prev, existed := n.ActiveNodes[nodeID]
	n.ActiveNodes[nodeID] = isLive

	becameActive := (!prev || !existed) && isLive
	leaderDemoted := false

	if nodeID == n.ID && !isLive {
		n.IsLeader = false
	}

	if !isLive && n.CurrentBallot.NodeID == nodeID {
		n.CurrentBallot.NodeID = 0
		n.lastLeaderMsg = time.Now().Add(-2 * time.Duration(config.LeaderTimeout) * time.Millisecond)
		n.electionCoolDown = time.Time{}
		leaderDemoted = true
	}

	if isLive && nodeID == n.ID {
		n.lastLeaderMsg = time.Now()
	}

	return leaderDemoted, becameActive
}

// GetCurrentBallot returns the node's ballot under lock.
func (n *Node) GetCurrentBallot() datatypes.BallotNumber {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CurrentBallot
}

// GetIsLeader reports whether this node currently leads.
func (n *Node) GetIsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsLeader
}

// HandleHeartbeat refreshes leader liveness info and ballot tracking.
func (n *NodeService) HandleHeartbeat(msg datatypes.HeartbeatMsg, reply *bool) error {
	// log.Printf("Node %d: HandleHeartbeat from leader %d ballot=(%d,%d)", n.node.ID, msg.LeaderID, msg.Ballot.Number, msg.Ballot.NodeID)
	n.node.mu.Lock()
	defer n.node.mu.Unlock()

	if msg.LeaderID == n.node.ID {
		*reply = true
		return nil
	}

	n.node.lastLeaderMsg = time.Now()

	if n.node.CurrentBallot.LessThan(msg.Ballot) {
		n.node.CurrentBallot = msg.Ballot
		n.node.IsLeader = false
	}

	*reply = true
	return nil
}

// sendHeartbeats periodically notifies peers while leader remains active.
func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	log.Printf("Node %d: heartbeat loop started", n.ID)

	for {
		select {
		case <-ticker.C:
			if !n.GetIsLeader() {
				return
			}

			n.mu.RLock()
			selfActive := n.ActiveNodes[n.ID]
			n.mu.RUnlock()
			if !selfActive {
				return
			}

			msg := datatypes.HeartbeatMsg{
				Ballot:    n.CurrentBallot,
				LeaderID:  n.ID,
				Timestamp: time.Now().UnixNano(),
			}

			for peerID := range n.Peers {
				if peerID == n.ID {
					continue
				}
				go func(pid int) {
					var ack bool
					err := n.callRPC(pid, "HandleHeartbeat", msg, &ack)
					if err != nil {
						// log.Printf("Leader %d: heartbeat to %d failed: %v", n.ID, pid, err)
					}
				}(peerID)
			}
		case <-n.shutdown:
			log.Printf("Node %d: heartbeat loop exiting", n.ID)
			return
		}
	}
}

// ProcessClientRequest validates leader status and drives accept/commit.
func (n *Node) ProcessClientRequest(request datatypes.ClientRequest) datatypes.ReplyMsg {
	n.mu.Lock()
	log.Printf("Node %d: ProcessClientRequest client=%s ts=%d no-op=%v", n.ID, request.ClientID, request.Timestamp, request.IsNoOp)
	//fmt.Println("Inside processClientRequest")
	// Check for duplicate request to not process it again
	if lastReply, exists := n.LastReply[request.ClientID]; exists {
		if request.Timestamp <= lastReply.Timestamp {
			n.mu.Unlock()
			return lastReply
		}
	}

	if !n.IsLeader {
		n.mu.Unlock()
		return datatypes.ReplyMsg{
			Ballot:    n.CurrentBallot,
			Timestamp: request.Timestamp,
			ClientID:  request.ClientID,
			Success:   false,
			Message:   "not leader",
		}
	}

	// Check if majority nodes are active
	activeCount := 0
	for _, active := range n.ActiveNodes {
		if active {
			activeCount++
		}
	}

	//fmt.Printf("----number of activeCount:------- %d\n", activeCount)

	if activeCount < n.MajoritySize {
		log.Printf("Node %d: insufficient active nodes %d (needed %d) so skipping transactions",
			n.ID, activeCount, n.MajoritySize)
		n.mu.Unlock()
		return datatypes.ReplyMsg{
			Ballot:    n.CurrentBallot,
			Timestamp: request.Timestamp,
			ClientID:  request.ClientID,
			Success:   false,
			Message:   "insufficient active nodes",
		}
	}

	seqNum := n.NextSeqNum
	n.NextSeqNum++

	acceptMsg := datatypes.AcceptMsg{
		Type:    "ACCEPT",
		Ballot:  n.CurrentBallot,
		SeqNum:  seqNum,
		Request: request,
	}

	// Leader logs accepted entry
	logEntry := datatypes.LogEntry{
		Ballot:  n.CurrentBallot,
		SeqNum:  seqNum,
		Request: request,
		Status:  datatypes.StatusAccepted,
	}
	n.AcceptedLog[seqNum] = logEntry
	updated := false
	for i := range n.RequestLog {
		if n.RequestLog[i].SeqNum == logEntry.SeqNum {
			n.RequestLog[i] = logEntry
			updated = true
			break
		}
	}
	if !updated {
		n.RequestLog = append(n.RequestLog, logEntry)
	}

	if n.pendingAccepts[seqNum] == nil {
		n.pendingAccepts[seqNum] = make(map[int]datatypes.AcceptedMsg)
	}
	n.pendingAccepts[seqNum][n.ID] = datatypes.AcceptedMsg{
		Ballot:  n.CurrentBallot,
		SeqNum:  seqNum,
		Request: request,
		NodeID:  n.ID,
	}

	n.mu.Unlock()

	for nodeID := range n.Peers {
		if nodeID != n.ID {
			go func(id int) {
				var reply datatypes.AcceptedMsg
				err := n.callRPC(id, "Accept", acceptMsg, &reply)
				if err == nil {
					n.mu.Lock()
					if n.pendingAccepts[seqNum] == nil {
						n.pendingAccepts[seqNum] = make(map[int]datatypes.AcceptedMsg)
					}
					n.pendingAccepts[seqNum][id] = reply
					n.mu.Unlock()
				}
			}(nodeID)
		}
	}

	// Wait for majority ACCEPTS to move forward
	maxWait := 50
	for i := 0; i < maxWait; i++ {
		time.Sleep(10 * time.Millisecond)
		n.mu.RLock()
		acceptCount := len(n.pendingAccepts[seqNum])
		n.mu.RUnlock()
		if acceptCount >= n.MajoritySize {
			break
		}
	}

	n.mu.Lock()
	acceptCount := len(n.pendingAccepts[seqNum])
	n.mu.Unlock()

	if acceptCount >= n.MajoritySize {
		commitMsg := datatypes.CommitMsg{
			Ballot:  n.CurrentBallot,
			SeqNum:  seqNum,
			Request: request,
		}

		for nodeID := range n.Peers {
			if nodeID == n.ID {
				continue
			}
			n.mu.RLock()
			targetActive := n.ActiveNodes[nodeID]
			n.mu.RUnlock()
			if !targetActive {
				continue
			}
			go func(id int) {
				var reply bool
				n.callRPC(id, "Commit", commitMsg, &reply)
			}(nodeID)
		}

		//log.Printf("Node %d: COMMITTED seq=%d (%s→%s, %d)",n.ID,seqNum,request.Transaction.Sender,request.Transaction.Receiver,request.Transaction.Amount)

		//log.Printf("Node %d: EXECUTING seq=%d (%s→%s,%d)",n.ID,seqNum,request.Transaction.Sender,request.Transaction.Receiver,request.Transaction.Amount)

		n.mu.Lock()
		success, message := n.executeRequest(seqNum, request)
		n.mu.Unlock()

		// if success {
		// 	log.Printf("Node %d: EXECUTED seq=%d SUCCESS (%s)", n.ID, seqNum, message)
		// } else {
		// 	log.Printf("Node %d: EXECUTION FAILED seq=%d (%s)", n.ID, seqNum, message)
		// }

		n.mu.Lock()
		reply := datatypes.ReplyMsg{
			Ballot:    n.CurrentBallot,
			Timestamp: request.Timestamp,
			ClientID:  request.ClientID,
			Success:   success,
			Message:   message,
			SeqNum:    seqNum,
		}
		n.LastReply[request.ClientID] = reply
		n.mu.Unlock()
		log.Printf("Node %d: ProcessClientRequest seq=%d success=%v msg=%s", n.ID, seqNum, success, message)

		return reply
	}

	log.Printf("Node %d: consensus failed for client=%s ts=%d", n.ID, request.ClientID, request.Timestamp)
	return datatypes.ReplyMsg{
		Ballot:    n.CurrentBallot,
		Timestamp: request.Timestamp,
		ClientID:  request.ClientID,
		Success:   false,
		Message:   "consensus failed",
	}
}

// HandlePrepare responds to prepare RPCs with promises and prior log state.
func (n *Node) HandlePrepare(args datatypes.PrepareMsg, reply *datatypes.PromiseMsg) error {
	n.mu.Lock()
	//n.lastHeartBeat = time.Now()
	n.lastLeaderMsg = time.Now()

	defer n.mu.Unlock()

	recent := time.Since(n.lastLeaderMsg) <= time.Duration(config.LeaderTimeout)*time.Millisecond
	if recent && n.CurrentBallot.NodeID != 0 && args.Ballot.LessThan(n.CurrentBallot) {

		*reply = datatypes.PromiseMsg{Ballot: args.Ballot, Success: false}
		log.Printf("Node %d: Ignoring prepare from Node %d (fresh leader %d, ballot %s)",
			n.ID, args.Ballot.NodeID, n.CurrentBallot.NodeID, n.CurrentBallot)
		return nil
	}

	if args.Ballot.GreaterThan(n.HighestPromised) {
		n.HighestPromised = args.Ballot

		acceptLog := make([]datatypes.AcceptLogEntry, 0)
		for seqNum, entry := range n.AcceptedLog {
			acceptLog = append(acceptLog, datatypes.AcceptLogEntry{
				AcceptNum: entry.Ballot,
				SeqNum:    seqNum,
				Request:   entry.Request,
				Status:    entry.Status,
			})
		}

		*reply = datatypes.PromiseMsg{
			Ballot:    args.Ballot,
			AcceptLog: acceptLog,
			Success:   true,
		}

		log.Printf("Node %d: Promised ballot %s\n", n.ID, args.Ballot)
	} else {
		*reply = datatypes.PromiseMsg{
			Ballot:  args.Ballot,
			Success: false,
		}
	}

	return nil
}

// HandleAccept stores a leader's proposal when the ballot is acceptable.
func (n *Node) HandleAccept(args datatypes.AcceptMsg, reply *datatypes.AcceptedMsg) error {
	n.mu.Lock()
	//n.lastHeartBeat = time.Now()
	n.lastLeaderMsg = time.Now()

	defer n.mu.Unlock()

	// If this node is marked inactive, ignore accept requests
	if !n.ActiveNodes[n.ID] {
		return nil
	}

	if args.Ballot.GreaterThanOrEqual(n.HighestPromised) {
		// Do not downgrade an already more-advanced status (e.g., Executed)
		prev, ok := n.AcceptedLog[args.SeqNum]
		newStatus := datatypes.StatusAccepted
		if ok {
			newStatus = maxStatus(prev.Status, newStatus)
		}

		logEntry := datatypes.LogEntry{
			Ballot:  args.Ballot,
			SeqNum:  args.SeqNum,
			Request: args.Request,
			Status:  newStatus,
		}

		n.AcceptedLog[args.SeqNum] = logEntry
		updated := false
		for i := range n.RequestLog {
			if n.RequestLog[i].SeqNum == logEntry.SeqNum {
				// Preserve the higher status in the request log
				n.RequestLog[i].Status = maxStatus(n.RequestLog[i].Status, logEntry.Status)
				n.RequestLog[i].Ballot = logEntry.Ballot
				n.RequestLog[i].Request = logEntry.Request
				updated = true
				break
			}
		}
		if !updated {
			n.RequestLog = append(n.RequestLog, logEntry)
		}
		tx := args.Request.Transaction
		log.Printf("Node %d ACCEPT seq=%d ballot=(%d,%d) txn=(%s→%s,%d)\n",
			n.ID, args.SeqNum, args.Ballot.Number, args.Ballot.NodeID, tx.Sender, tx.Receiver, tx.Amount)

		*reply = datatypes.AcceptedMsg{
			Ballot:  args.Ballot,
			SeqNum:  args.SeqNum,
			Request: args.Request,
			NodeID:  n.ID,
		}

		log.Printf("Node %d: Accepted seq %d\n", n.ID, args.SeqNum)
	}

	return nil
}

// HandleCommit marks an entry committed and triggers execution ordering.
func (n *Node) HandleCommit(args datatypes.CommitMsg, reply *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.lastLeaderMsg = time.Now()

	// If this node is marked inactive, ignore commit to keep it unchanged
	if !n.ActiveNodes[n.ID] {
		*reply = false
		return nil
	}

	// Accept commits from current/newer ballots; ignore only stale ballots
	if args.Ballot.LessThan(n.HighestPromised) {
		log.Printf("Node %d: ignoring stale commit seq=%d ballot=%s (promise=%s)", n.ID, args.SeqNum, args.Ballot.String(), n.HighestPromised.String())
		*reply = false
		return nil
	}

	entry, exists := n.AcceptedLog[args.SeqNum]
	if !exists {

		entry = datatypes.LogEntry{
			Ballot:  args.Ballot,
			SeqNum:  args.SeqNum,
			Request: args.Request,
			Status:  datatypes.StatusCommitted,
		}
		n.AcceptedLog[args.SeqNum] = entry
	} else {
		// Do not downgrade Executed -> Committed
		if entry.Status != datatypes.StatusExecuted {
			entry.Status = datatypes.StatusCommitted
			n.AcceptedLog[args.SeqNum] = entry
		}
	}

	for i := range n.RequestLog {
		if n.RequestLog[i].SeqNum == args.SeqNum {
			if n.RequestLog[i].Status != datatypes.StatusExecuted {
				n.RequestLog[i].Status = datatypes.StatusCommitted
			}
			break
		}
	}

	//log.Printf("Node %d: COMMITTED seq=%d (%s→%s, %d)",n.ID,args.SeqNum,args.Request.Transaction.Sender,args.Request.Transaction.Receiver,args.Request.Transaction.Amount)

	go n.executeRequestsInOrder()

	*reply = true
	return nil
}

// HandleNewView rebuilds local logs from the leader's snapshot.
func (n *Node) HandleNewView(args datatypes.NewViewMsg, reply *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Ballot.LessThan(n.HighestPromised) {
		//log.Printf("Node %d: Ignoring outdated New-View for ballot %v (current promise %v)", n.ID, args.Ballot, n.HighestPromised)
		*reply = false
		return nil
	}

	if args.Ballot.Number == n.lastProcessedView.Number && args.Ballot.NodeID == n.lastProcessedView.NodeID {
		*reply = true
		return nil
	}
	n.lastProcessedView = args.Ballot

	n.lastLeaderMsg = time.Now()
	n.HighestPromised = args.Ballot
	n.CurrentBallot = args.Ballot
	n.IsLeader = args.Ballot.NodeID == n.ID

	n.NewViewMsgs = append(n.NewViewMsgs, args)

	prevAccepted := n.AcceptedLog
	newAcceptedLog := make(map[int]datatypes.LogEntry, len(args.AcceptLog))
	newRequestLog := make([]datatypes.LogEntry, 0, len(args.AcceptLog))
	maxSeq := 0

	for _, entry := range args.AcceptLog {
		if entry.SeqNum > maxSeq {
			maxSeq = entry.SeqNum
		}

		status := entry.Status
		if prevEntry, ok := prevAccepted[entry.SeqNum]; ok {
			status = maxStatus(status, prevEntry.Status)
		}

		logEntry := datatypes.LogEntry{
			Ballot:  entry.AcceptNum,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
			Status:  status,
		}

		newAcceptedLog[entry.SeqNum] = logEntry
		newRequestLog = append(newRequestLog, logEntry)
	}

	sort.Slice(newRequestLog, func(i, j int) bool {
		return newRequestLog[i].SeqNum < newRequestLog[j].SeqNum
	})

	n.AcceptedLog = newAcceptedLog
	n.RequestLog = newRequestLog

	n.pendingAccepts = make(map[int]map[int]datatypes.AcceptedMsg)

	if maxSeq == 0 {
		n.NextSeqNum = 1
	} else {
		n.NextSeqNum = maxSeq + 1
	}

	n.applyCommittedEntries(prevAccepted, maxSeq)

	if len(args.AcceptLog) > 0 && args.Ballot.NodeID != n.ID {
		for _, entry := range args.AcceptLog {
			go func(e datatypes.AcceptLogEntry) {
				var acceptedReply datatypes.AcceptedMsg
				acceptedMsg := datatypes.AcceptedMsg{
					Ballot:  args.Ballot,
					SeqNum:  e.SeqNum,
					Request: e.Request,
					NodeID:  n.ID,
				}
				_ = n.callRPC(args.Ballot.NodeID, "AcceptedFromNewView", acceptedMsg, &acceptedReply)
			}(entry)
		}
	}
	// Execute the entries from the new view here
	go n.executeRequestsInOrder()

	*reply = true
	//log.Printf("Node %d: Processed New-View from ballot %s with %d entries\n",n.ID, args.Ballot, len(args.AcceptLog))
	return nil
}

// executeRequest applies a request or marks a no-op executed.
func (n *Node) executeRequest(seqNum int, request datatypes.ClientRequest) (bool, string) {
	if request.IsNoOp {
		if entry, exists := n.AcceptedLog[seqNum]; exists {
			entry.Status = datatypes.StatusExecuted
			n.AcceptedLog[seqNum] = entry
		}
		return true, "no-op executed"
	}

	// Try applying the transaction
	success, message := n.Database.ExecuteTransaction(request.Transaction)

	if entry, exists := n.AcceptedLog[seqNum]; exists {
		entry.Status = datatypes.StatusExecuted
		n.AcceptedLog[seqNum] = entry

		for i := range n.RequestLog {
			if n.RequestLog[i].SeqNum == seqNum {
				n.RequestLog[i].Status = datatypes.StatusExecuted
				break
			}
		}
	}

	// if success {
	// 	log.Printf("Node %d: EXECUTED seq=%d SUCCESS (%s→%s,%d)",n.ID, seqNum,request.Transaction.Sender,request.Transaction.Receiver,request.Transaction.Amount)
	// } else {
	// 	log.Printf("Node %d: EXECUTION FAILED seq=%d (%s→%s,%d) — %s",n.ID, seqNum, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount, message)
	// }

	return success, message
}

// executeRequestsInOrder walks committed entries sequentially and executes them.
func (n *Node) executeRequestsInOrder() {
	log.Printf("Node %d: executeRequestsInOrder triggered", n.ID)
	for seqNum := 1; ; seqNum++ {
		entry, exists := n.AcceptedLog[seqNum]
		if !exists {
			log.Printf("Node %d: executeRequestsInOrder stopping at gap seq=%d", n.ID, seqNum)
			break
		}

		if entry.Status == datatypes.StatusExecuted {
			continue
		}

		if entry.Status == datatypes.StatusCommitted {
			//log.Printf("Node %d: EXECUTING seq=%d (%s→%s,%d)",n.ID, seqNum,entry.Request.Transaction.Sender,entry.Request.Transaction.Receiver,entry.Request.Transaction.Amount)

			n.executeRequest(seqNum, entry.Request)
		}
	}
}

// applyCommittedEntries replays committed entries, preserving executed ones.
func (n *Node) applyCommittedEntries(prevAccepted map[int]datatypes.LogEntry, maxSeq int) {
	log.Printf("Node %d: applyCommittedEntries up to seq=%d", n.ID, maxSeq)
	for seqNum := 1; seqNum <= maxSeq; seqNum++ {
		entry, exists := n.AcceptedLog[seqNum]
		if !exists {
			continue
		}

		if entry.Status == datatypes.StatusCommitted || entry.Status == datatypes.StatusExecuted {
			if prevEntry, ok := prevAccepted[seqNum]; ok && prevEntry.Status == datatypes.StatusExecuted {
				if entry.Status == datatypes.StatusCommitted {
					entry.Status = datatypes.StatusExecuted
					n.AcceptedLog[seqNum] = entry
					for i := range n.RequestLog {
						if n.RequestLog[i].SeqNum == seqNum {
							n.RequestLog[i].Status = datatypes.StatusExecuted
							break
						}
					}
				}
				continue
			}

			n.executeRequest(seqNum, entry.Request)
		}
	}
}

// StartLeaderElection initiates Paxos phase-1 to try becoming leader.
func (n *Node) StartLeaderElection() bool {
	n.mu.Lock()
	log.Printf("Node %d: StartLeaderElection invoked (leader=%v active=%v)", n.ID, n.IsLeader, n.ActiveNodes[n.ID])
	if !n.ActiveNodes[n.ID] {
		n.mu.Unlock()
		return false
	}
	if n.IsLeader {
		n.mu.Unlock()
		return false
	}

	// if n.CurrentBallot.Number <= n.HighestPromised.Number {
	// 	n.CurrentBallot.Number = n.HighestPromised.Number + 1
	// }

	//log.Printf("Node %d: Starting election, current=%d, highestPromised=%d",n.ID, n.CurrentBallot.Number, n.HighestPromised.Number)

	next := n.CurrentBallot.Number + 1
	if next <= n.HighestPromised.Number {
		next = n.HighestPromised.Number + 1
	}
	n.CurrentBallot.Number = next

	//n.CurrentBallot.Number++
	n.CurrentBallot.NodeID = n.ID
	ballot := n.CurrentBallot
	n.mu.Unlock()

	log.Printf("Node %d: Starting leader election with ballot %s", n.ID, ballot)

	promises := make(map[int]datatypes.PromiseMsg)
	promiseMu := sync.Mutex{}

	var wg sync.WaitGroup
	for nodeID := range n.Peers {
		if nodeID != n.ID {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				prepareMsg := datatypes.PrepareMsg{Ballot: ballot}
				var reply datatypes.PromiseMsg
				err := n.callRPC(id, "Prepare", prepareMsg, &reply)
				if err == nil && reply.Success {
					promiseMu.Lock()
					promises[id] = reply
					promiseMu.Unlock()
				}
			}(nodeID)
		}
	}

	// Self promise
	n.mu.Lock()
	selfLog := make([]datatypes.AcceptLogEntry, 0)
	for seqNum, entry := range n.AcceptedLog {
		selfLog = append(selfLog, datatypes.AcceptLogEntry{
			AcceptNum: entry.Ballot,
			SeqNum:    seqNum,
			Request:   entry.Request,
			Status:    entry.Status,
		})
	}
	promises[n.ID] = datatypes.PromiseMsg{Ballot: ballot, AcceptLog: selfLog, Success: true}
	n.mu.Unlock()

	done := make(chan bool)
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
	}

	promiseMu.Lock()
	promiseCount := len(promises)
	promiseMu.Unlock()

	//requiredMajority := n.majorityThreshold()
	requiredMajority := config.MajoritySize
	if promiseCount >= requiredMajority {
		n.mu.Lock()
		if !n.ActiveNodes[n.ID] {
			n.mu.Unlock()
			return false
		}
		n.IsLeader = true
		n.acceptedFromNewViewCount = 0
		n.ackFromNewView = make(map[int]bool)

		n.CurrentBallot = ballot
		go n.sendHeartbeats()
		//log.Printf("Node %d: Became leader with ballot %s (promises: %d)\n",n.ID, ballot, promiseCount)

		acceptLog := n.createNewViewFromPromises(promises)

		newViewMsg := datatypes.NewViewMsg{
			Ballot:    ballot,
			AcceptLog: acceptLog,
		}
		n.NewViewMsgs = append(n.NewViewMsgs, newViewMsg)
		n.mu.Unlock()

		n.acceptedFromNewViewCount = 0

		n.sendNewViewMessages(newViewMsg)

		//log.Printf("Node %d: Waiting for majority AcceptedFromNewView responses...", n.ID)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			n.mu.RLock()
			count := n.acceptedFromNewViewCount
			majority := n.MajoritySize
			n.mu.RUnlock()
			if count >= majority {
				//log.Printf("Node %d: NewView accepted by majority (%d/%d), safe to proceed", n.ID, count, majority)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		n.mu.RLock()
		if n.acceptedFromNewViewCount < n.MajoritySize {
			// log.Printf("Node %d: Proceeding with less than majority NewView confirmations (%d/%d)",n.ID, n.acceptedFromNewViewCount, n.MajoritySize)
		}
		n.mu.RUnlock()

		for _, entry := range acceptLog {
			n.recoverEntryWithNewBallot(entry, ballot)
		}

		log.Printf("Node %d: leader election success with ballot %s", n.ID, ballot)
		return true
	}

	log.Printf("Node %d: leader election failed (promises=%d need=%d)", n.ID, promiseCount, requiredMajority)
	return false
}

// recoverEntryWithNewBallot ensures an entry is accepted by a majority under the
// current leader ballot, then issues a commit and applies it locally.
func (n *Node) recoverEntryWithNewBallot(entry datatypes.AcceptLogEntry, ballot datatypes.BallotNumber) {
	// Check current acceptance count
	n.mu.Lock()
	current := n.pendingAccepts[entry.SeqNum]
	acceptCount := 0
	if current != nil {
		acceptCount = len(current)
	}
	n.mu.Unlock()

	if acceptCount < config.MajoritySize {
		// Re-propose under this ballot
		acceptMsg := datatypes.AcceptMsg{
			Type:    "ACCEPT",
			Ballot:  ballot,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
		}

		var wg sync.WaitGroup
		for peerID := range n.Peers {
			if peerID == n.ID {
				continue
			}
			n.mu.RLock()
			targetActive := n.ActiveNodes[peerID]
			n.mu.RUnlock()
			if !targetActive {
				continue
			}
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				var ack datatypes.AcceptedMsg
				if err := n.callRPC(id, "Accept", acceptMsg, &ack); err == nil && ack.NodeID == id && ack.SeqNum == entry.SeqNum &&
					ack.Ballot.Number == acceptMsg.Ballot.Number && ack.Ballot.NodeID == acceptMsg.Ballot.NodeID {
					n.mu.Lock()
					if n.pendingAccepts[entry.SeqNum] == nil {
						n.pendingAccepts[entry.SeqNum] = make(map[int]datatypes.AcceptedMsg)
					}
					n.pendingAccepts[entry.SeqNum][id] = ack
					n.mu.Unlock()
				}
			}(peerID)
		}
		wg.Wait()
	}

	// Majority? then commit and learn
	n.mu.Lock()
	acceptCount = 0
	if n.pendingAccepts[entry.SeqNum] != nil {
		acceptCount = len(n.pendingAccepts[entry.SeqNum])
	}
	n.mu.Unlock()

	if acceptCount >= config.MajoritySize {
		commitMsg := datatypes.CommitMsg{
			Ballot:  ballot,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
		}
		for peerID := range n.Peers {
			if peerID == n.ID {
				continue
			}
			n.mu.RLock()
			targetActive := n.ActiveNodes[peerID]
			n.mu.RUnlock()
			if !targetActive {
				continue
			}
			go func(id int) {
				var applied bool
				n.callRPC(id, "Commit", commitMsg, &applied)
			}(peerID)
		}
		var applied bool
		_ = n.HandleCommit(commitMsg, &applied)
	}
}

// createNewViewFromPromises merges promise logs into a consolidated accept log.
func (n *Node) createNewViewFromPromises(promises map[int]datatypes.PromiseMsg) []datatypes.AcceptLogEntry {
	log.Printf("Node %d: creating new view from %d promises", n.ID, len(promises))
	allEntries := make(map[int]datatypes.AcceptLogEntry)
	maxSeq := 0

	for _, promise := range promises {
		for _, entry := range promise.AcceptLog {
			if entry.SeqNum > maxSeq {
				maxSeq = entry.SeqNum
			}
			existing, exists := allEntries[entry.SeqNum]
			if !exists || entry.AcceptNum.GreaterThan(existing.AcceptNum) {
				allEntries[entry.SeqNum] = entry
			} else if exists && entry.AcceptNum.Number == existing.AcceptNum.Number && entry.AcceptNum.NodeID == existing.AcceptNum.NodeID {

				if statusRank(entry.Status) > statusRank(existing.Status) {
					existing.Status = entry.Status
					allEntries[entry.SeqNum] = existing
				}
			}
		}
	}

	maxCommitted := 0
	for _, e := range allEntries {
		if e.Status == datatypes.StatusCommitted || e.Status == datatypes.StatusExecuted {
			if e.SeqNum > maxCommitted {
				maxCommitted = e.SeqNum
			}
		}
	}

	acceptLog := make([]datatypes.AcceptLogEntry, 0, maxSeq)
	for seq := 1; seq <= maxSeq; seq++ {
		if e, ok := allEntries[seq]; ok {

			if e.AcceptNum.LessThan(n.CurrentBallot) {
				e.AcceptNum = n.CurrentBallot
			}
			acceptLog = append(acceptLog, e)
			continue
		}

		if seq <= maxCommitted {
			noOpRequest := datatypes.ClientRequest{
				ClientID:  fmt.Sprintf("no-op-%d", seq),
				Timestamp: time.Now().UnixNano(),
				IsNoOp:    true,
				Transaction: datatypes.Txn{
					Sender:   "no-op",
					Receiver: "no-op",
					Amount:   0,
				},
			}
			noOp := datatypes.AcceptLogEntry{
				AcceptNum: n.CurrentBallot,
				SeqNum:    seq,
				Request:   noOpRequest,
				Status:    datatypes.StatusAccepted,
			}
			acceptLog = append(acceptLog, noOp)
		}
	}

	prevAccepted := n.AcceptedLog
	newAcceptedLog := make(map[int]datatypes.LogEntry, len(acceptLog))
	newRequestLog := make([]datatypes.LogEntry, 0, len(acceptLog))

	for _, e := range acceptLog {
		finalStatus := e.Status
		if prev, ok := prevAccepted[e.SeqNum]; ok {
			finalStatus = maxStatus(finalStatus, prev.Status)
		}

		logEntry := datatypes.LogEntry{
			Ballot:  e.AcceptNum,
			SeqNum:  e.SeqNum,
			Request: e.Request,
			Status:  finalStatus,
		}
		newAcceptedLog[e.SeqNum] = logEntry
		newRequestLog = append(newRequestLog, logEntry)
	}

	sort.Slice(newRequestLog, func(i, j int) bool {
		return newRequestLog[i].SeqNum < newRequestLog[j].SeqNum
	})

	n.AcceptedLog = newAcceptedLog
	n.RequestLog = newRequestLog
	if maxSeq == 0 {
		n.NextSeqNum = 1
	} else {
		n.NextSeqNum = maxSeq + 1
	}

	n.applyCommittedEntries(prevAccepted, maxSeq)

	keepSeq := make(map[int]struct{}, len(acceptLog))
	for _, e := range acceptLog {
		keepSeq[e.SeqNum] = struct{}{}
		if n.pendingAccepts[e.SeqNum] == nil {
			n.pendingAccepts[e.SeqNum] = make(map[int]datatypes.AcceptedMsg)
		} else {
			for k := range n.pendingAccepts[e.SeqNum] {
				delete(n.pendingAccepts[e.SeqNum], k)
			}
		}
		n.pendingAccepts[e.SeqNum][n.ID] = datatypes.AcceptedMsg{
			Ballot:  n.CurrentBallot,
			SeqNum:  e.SeqNum,
			Request: e.Request,
			NodeID:  n.ID,
		}
	}
	for seq := range n.pendingAccepts {
		if _, ok := keepSeq[seq]; !ok {
			delete(n.pendingAccepts, seq)
		}
	}

	log.Printf("Node %d: new view constructed with %d entries (maxSeq=%d)", n.ID, len(acceptLog), maxSeq)
	return acceptLog
}

// sendNewViewMessages distributes the leader's view-change log to peers.
func (n *Node) sendNewViewMessages(msg datatypes.NewViewMsg) {
	log.Printf("Node %d: broadcasting NewView ballot=%s entries=%d", n.ID, msg.Ballot.String(), len(msg.AcceptLog))
	for nodeID := range n.Peers {
		if nodeID == n.ID {
			continue
		}
		n.mu.RLock()
		targetActive := n.ActiveNodes[nodeID]
		n.mu.RUnlock()
		if !targetActive {
			continue
		}
		go func(id int) {
			var reply bool
			n.callRPC(id, "NewView", msg, &reply)
		}(nodeID)
	}
}

// buildStateSnapshot captures the current accepted log as a NewViewMsg.
func (n *Node) buildStateSnapshot() datatypes.NewViewMsg {
	n.mu.RLock()
	defer n.mu.RUnlock()
	log.Printf("Node %d: buildStateSnapshot generating from %d entries", n.ID, len(n.AcceptedLog))

	seqs := make([]int, 0, len(n.AcceptedLog))
	for seq := range n.AcceptedLog {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)

	acceptLog := make([]datatypes.AcceptLogEntry, 0, len(seqs))
	for _, seq := range seqs {
		entry := n.AcceptedLog[seq]
		acceptLog = append(acceptLog, datatypes.AcceptLogEntry{
			AcceptNum: entry.Ballot,
			SeqNum:    seq,
			Request:   entry.Request,
			Status:    entry.Status,
		})
	}

	snapshot := datatypes.NewViewMsg{
		Ballot:    n.CurrentBallot,
		AcceptLog: acceptLog,
	}
	log.Printf("Node %d: snapshot ready entries=%d ballot=%s", n.ID, len(acceptLog), snapshot.Ballot.String())
	return snapshot
}

// sendStateSnapshot pushes the latest snapshot to a specific follower.
func (n *Node) sendStateSnapshot(targetID int) {
	if targetID == n.ID {
		return
	}

	snapshot := n.buildStateSnapshot()
	log.Printf("Node %d: sending snapshot entries=%d to node %d", n.ID, len(snapshot.AcceptLog), targetID)
	var reply bool
	if err := n.callRPC(targetID, "NewView", snapshot, &reply); err != nil {
		log.Printf("Node %d: state snapshot to %d failed: %v", n.ID, targetID, err)
	}
}

// requestStateSync asks the leader for a snapshot to catch up after downtime.
func (n *Node) requestStateSync() {
	log.Printf("Node %d: initiating state sync attempts", n.ID)
	for attempt := 0; attempt < 5; attempt++ {
		n.mu.RLock()
		leaderID := n.CurrentBallot.NodeID
		isLeader := n.IsLeader && n.ActiveNodes[n.ID]
		leaderActive := leaderID != 0 && n.ActiveNodes[leaderID]
		n.mu.RUnlock()

		if isLeader {
			log.Printf("Node %d: aborting state sync because self is leader", n.ID)
			return
		}

		if !leaderActive || leaderID == n.ID {
			log.Printf("Node %d: state sync attempt %d waiting for active leader", n.ID, attempt+1)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		args := datatypes.StateTransferArgs{RequesterID: n.ID}
		var reply datatypes.StateTransferReply
		if err := n.callRPC(leaderID, "RequestStateTransfer", args, &reply); err != nil || !reply.Success {
			log.Printf("Node %d: state transfer request attempt %d failed: %v success=%v", n.ID, attempt+1, err, reply.Success)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		var applied bool
		if err := n.HandleNewView(reply.Snapshot, &applied); err != nil || !applied {
			log.Printf("Node %d: apply snapshot attempt %d failed: %v applied=%v", n.ID, attempt+1, err, applied)
			time.Sleep(300 * time.Millisecond)
			continue
		}

		log.Printf("Node %d: state sync completed", n.ID)
		return
	}

	//log.Printf("Node %d: failed to synchronize state after retries", n.ID)
	log.Printf("Node %d: failed to synchronize state after retries", n.ID)
}

// PrintLog dumps the node's request log via RPC.
func (s *NodeService) PrintLog(_ bool, reply *string) error {
	log.Printf("Node %d: PrintLog RPC invoked", s.node.ID)
	s.node.mu.RLock()
	defer s.node.mu.RUnlock()

	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("===== Node %d Log =====\n", s.node.ID))
	for _, entry := range s.node.RequestLog {
		status := entry.Status
		builder.WriteString(fmt.Sprintf("Seq %d | Ballot (%d,%d) | %s -> %s | Amount %d | Status %v\n",
			entry.SeqNum,
			entry.Ballot.Number,
			entry.Ballot.NodeID,
			entry.Request.Transaction.Sender,
			entry.Request.Transaction.Receiver,
			entry.Request.Transaction.Amount,
			status))
	}
	*reply = builder.String()
	return nil
}

// PrintStatus reports the consensus status of a sequence number.
func (s *NodeService) PrintStatus(seqNum int, reply *string) error {
	log.Printf("Node %d: PrintStatus RPC for seq=%d", s.node.ID, seqNum)
	s.node.mu.RLock()
	defer s.node.mu.RUnlock()

	if !s.node.ActiveNodes[s.node.ID] {
		*reply = fmt.Sprintf("Node %d inactive (Status: %s)", s.node.ID, datatypes.StatusNoStatus)
		return nil
	}

	if entry, exists := s.node.AcceptedLog[seqNum]; exists {
		switch entry.Status {
		case datatypes.StatusAccepted:
			*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Accepted)", s.node.ID, seqNum, datatypes.StatusAccepted)
			return nil
		case datatypes.StatusCommitted:
			*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Committed)", s.node.ID, seqNum, datatypes.StatusCommitted)
			return nil
		case datatypes.StatusExecuted:
			*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Executed)", s.node.ID, seqNum, datatypes.StatusExecuted)
			return nil
		default:
			*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Unknown)", s.node.ID, seqNum, datatypes.StatusNoStatus)
			return nil
		}
	}

	for _, entry := range s.node.RequestLog {
		if entry.SeqNum == seqNum {
			*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Pending/Accepted)", s.node.ID, seqNum, datatypes.StatusAccepted)
			return nil
		}
	}

	*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Not Found)", s.node.ID, seqNum, datatypes.StatusNoStatus)
	return nil
}

// PrintView lists processed new-view messages for inspection.
func (s *NodeService) PrintView(_ bool, reply *string) error {
	log.Printf("Node %d: PrintView RPC invoked", s.node.ID)
	s.node.mu.RLock()
	defer s.node.mu.RUnlock()

	var b strings.Builder
	fmt.Fprintf(&b, "===== Node %d: View Changes =====\n", s.node.ID)
	for i, msg := range s.node.NewViewMsgs {
		fmt.Fprintf(&b, "View %d: Ballot (%d,%d) | Leader: %d\n",
			i+1, msg.Ballot.Number, msg.Ballot.NodeID, msg.Ballot.NodeID)
		for _, acc := range msg.AcceptLog {
			req := acc.Request.Transaction
			fmt.Fprintf(&b, "  ⟨ACCEPT, (%d,%d), %d, (%s,%s,%d) | Status %s⟩\n",
				acc.AcceptNum.Number, acc.AcceptNum.NodeID,
				acc.SeqNum, req.Sender, req.Receiver, req.Amount, acc.Status)
		}
	}
	*reply = b.String()
	return nil
}

// PrintDB returns the node's database contents over RPC.
func (ns *NodeService) PrintDB(args datatypes.PrintDBArgs, reply *datatypes.PrintDBReply) error {
	//log.Printf("Node %d: Received RPC request to print DB.\n", ns.node.ID)
	log.Printf("Node %d: PrintDB RPC invoked", ns.node.ID)

	dbContents := ns.node.Database.PrintDB(ns.node.ID)

	reply.DBContents = dbContents

	return nil
}
