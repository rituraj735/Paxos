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

	//checkForLastHeartbeat kind of variable
	lastLeaderMsg time.Time

	//Heartbeat
	//lastHeartBeat    time.Time
	electionCoolDown time.Time
	// Paxos state
	CurrentBallot   datatypes.BallotNumber
	HighestPromised datatypes.BallotNumber
	NextSeqNum      int

	// Logs
	AcceptedLog map[int]datatypes.LogEntry
	RequestLog  []datatypes.LogEntry
	NewViewMsgs []datatypes.NewViewMsg

	// Client state
	LastReply map[string]datatypes.ReplyMsg
	Database  *database.Database

	// RPC server
	rpcServer *rpc.Server
	listener  net.Listener

	// Synchronization
	mu sync.RWMutex

	// For consensus tracking
	pendingAccepts map[int]map[int]datatypes.AcceptedMsg

	// Network partition simulation
	ActiveNodes  map[int]bool
	MajoritySize int

	// Shutdown
	shutdown chan bool
}

type NodeService struct {
	node *Node
}

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

func maxStatus(a, b datatypes.RequestStatus) datatypes.RequestStatus {
	if statusRank(b) > statusRank(a) {
		return b
	}
	return a
}

func NewNode(id int, address string, peers map[int]string) *Node {
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
	}
	go node.monitorLeaderTimeout()
	// Initialize all clients with initial balance
	for _, clientID := range config.ClientIDs {
		node.Database.InitializeClient(clientID, config.InitialBalance)
	}

	// Initially all nodes are active
	for nodeID := range peers {
		node.ActiveNodes[nodeID] = true
	}
	node.ActiveNodes[id] = true

	return node
}

func (n *Node) monitorLeaderTimeout() {
	for {
		time.Sleep(200 * time.Millisecond)
		n.mu.RLock()
		isLeader := n.IsLeader
		last := n.lastLeaderMsg
		cooldown := n.electionCoolDown
		n.mu.RUnlock()

		if isLeader {
			n.mu.Lock()
			n.lastLeaderMsg = time.Now() // keep its own timer fresh
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
			return
		default:
		}
	}
}

func (n *Node) StartRPCServer() error {
	n.rpcServer = rpc.NewServer()
	service := &NodeService{node: n}
	n.rpcServer.Register(service)
	fmt.Println("Listening RPC server at", n.Address)
	listener, err := net.Listen("tcp", n.Address)
	if err != nil {
		return err
	}
	n.listener = listener

	go func() {
		for {
			select {
			case <-n.shutdown:
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					continue
				}
				go n.rpcServer.ServeConn(conn)
			}
		}
	}()

	return nil
}

func (n *Node) Stop() {
	close(n.shutdown)
	if n.listener != nil {
		n.listener.Close()
	}
}

func (n *Node) callRPC(nodeID int, method string, args interface{}, reply interface{}) error {
	n.mu.RLock()
	if !n.ActiveNodes[nodeID] {
		n.mu.RUnlock()
		return fmt.Errorf("node %d is not active", nodeID)
	}
	n.mu.RUnlock()

	address, exists := n.Peers[nodeID]
	if !exists {
		return fmt.Errorf("unknown node %d", nodeID)
	}

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()

	done := make(chan error, 1)
	go func() {
		done <- client.Call("NodeService."+method, args, reply)
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(1 * time.Second):
		return fmt.Errorf("RPC timeout")
	}
}

func (s *NodeService) GetLeader(_ bool, reply *datatypes.LeaderInfo) error {
	s.node.mu.RLock()
	defer s.node.mu.RUnlock()

	now := time.Now()
	leaderID := 0

	// If *this* node is leader and active, report it confidently.
	if s.node.IsLeader && s.node.ActiveNodes[s.node.ID] {
		leaderID = s.node.ID
	} else {
		// Otherwise, only report the remembered leader if:
		// 1) that leader is still marked active, and
		// 2) its last heartbeat/new-view was recent (not timed out).
		if s.node.ActiveNodes[s.node.CurrentBallot.NodeID] &&
			now.Sub(s.node.lastLeaderMsg) <= time.Duration(config.LeaderTimeout)*time.Millisecond {
			leaderID = s.node.CurrentBallot.NodeID
		} else {
			leaderID = 0 // unknown / stale
		}
	}

	*reply = datatypes.LeaderInfo{
		LeaderID: leaderID,
		Ballot:   s.node.CurrentBallot,
		IsLeader: s.node.IsLeader && s.node.ActiveNodes[s.node.ID],
	}
	return nil
}

// RPC Service Methods
func (ns *NodeService) HandleClientRequest(args datatypes.ClientRequestRPC, reply *datatypes.ClientReplyRPC) error {
	fmt.Println("something reached handleClientRequest", args)
	replyMsg := ns.node.ProcessClientRequest(args.Request)
	reply.Reply = replyMsg
	return nil
}

func (ns *NodeService) Prepare(args datatypes.PrepareMsg, reply *datatypes.PromiseMsg) error {
	return ns.node.HandlePrepare(args, reply)
}

func (ns *NodeService) Accept(args datatypes.AcceptMsg, reply *datatypes.AcceptedMsg) error {
	return ns.node.HandleAccept(args, reply)
}

func (ns *NodeService) Commit(args datatypes.CommitMsg, reply *bool) error {
	return ns.node.HandleCommit(args, reply)
}

func (ns *NodeService) NewView(args datatypes.NewViewMsg, reply *bool) error {
	return ns.node.HandleNewView(args, reply)
}

func (ns *NodeService) AcceptedFromNewView(args datatypes.AcceptedMsg, reply *datatypes.AcceptedMsg) error {
	ns.node.mu.Lock()
	defer ns.node.mu.Unlock()

	if args.Ballot.Number != ns.node.CurrentBallot.Number || args.Ballot.NodeID != ns.node.CurrentBallot.NodeID {
		*reply = args
		return nil
	}

	if ns.node.pendingAccepts[args.SeqNum] == nil {
		ns.node.pendingAccepts[args.SeqNum] = make(map[int]datatypes.AcceptedMsg)
	}
	ns.node.pendingAccepts[args.SeqNum][args.NodeID] = args

	*reply = args
	return nil
}

func (ns *NodeService) RequestStateTransfer(args datatypes.StateTransferArgs, reply *datatypes.StateTransferReply) error {
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

func (s *NodeService) UpdateActiveStatus(args datatypes.UpdateNodeArgs, reply *bool) error {
	s.node.mu.Lock()
	leaderDemoted, activated := s.node.setNodeLiveness(args.NodeID, args.IsLive)
	isLeaderActive := s.node.IsLeader && s.node.ActiveNodes[s.node.ID]
	activeSnapshot := fmt.Sprintf("%v", s.node.ActiveNodes)
	selfID := s.node.ID
	s.node.mu.Unlock()

	*reply = true
	log.Printf("Node %d: Active status set to %v", s.node.ID, args.IsLive)
	log.Printf("Node %d: Active status updated -> %v", s.node.ID, activeSnapshot)

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

func (s *NodeService) UpdateActiveStatusForBulk(args datatypes.UpdateClusterStatusArgs, reply *bool) error {
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
func (n *Node) SetActiveNodes(activeNodeIDs []int) {
	n.mu.Lock()
	defer n.mu.Unlock()

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
	return maj
}

func (n *Node) majorityThresholdLocked() int {
	activeCount := 0
	for _, live := range n.ActiveNodes {
		if live {
			activeCount++
		}
	}
	return n.calcMajorityFromActive(activeCount)
}

func (n *Node) majorityThreshold() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.majorityThresholdLocked()
}

func (n *Node) setNodeLiveness(nodeID int, isLive bool) (bool, bool) {
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

func (n *Node) GetCurrentBallot() datatypes.BallotNumber {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.CurrentBallot
}

func (n *Node) GetIsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsLeader
}

func (n *NodeService) HandleHeartbeat(msg datatypes.HeartbeatMsg, reply *bool) error {
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

func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

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
						log.Printf("Leader %d: heartbeat to %d failed: %v", n.ID, pid, err)
					}
				}(peerID)
			}
		case <-n.shutdown:
			return
		}
	}
}

func (n *Node) ProcessClientRequest(request datatypes.ClientRequest) datatypes.ReplyMsg {
	n.mu.Lock()
	fmt.Println("Inside processClientRequest")
	// Check for duplicate request (exactly-once semantics)
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

	activeCount := 0
	for _, active := range n.ActiveNodes {
		if active {
			activeCount++
		}
	}

	fmt.Println("number of activeCount:", activeCount)
	requiredMajority := n.majorityThresholdLocked()

	if activeCount < requiredMajority {
		log.Printf("Node %d: insufficient active nodes %d (needed %d) so skipping transactions", n.ID, activeCount, requiredMajority)
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

	// Send accept messages to all peers
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

	// Wait for majority accepts
	maxWait := 50
	for i := 0; i < maxWait; i++ {
		time.Sleep(10 * time.Millisecond)
		n.mu.RLock()
		acceptCount := len(n.pendingAccepts[seqNum])
		n.mu.RUnlock()

		if acceptCount >= n.majorityThreshold() {
			break
		}
	}

	n.mu.Lock()
	acceptCount := len(n.pendingAccepts[seqNum])
	n.mu.Unlock()

	if acceptCount >= n.majorityThreshold() {
		commitMsg := datatypes.CommitMsg{
			Ballot:  n.CurrentBallot,
			SeqNum:  seqNum,
			Request: request,
		}

		// Send commit messages
		for nodeID := range n.Peers {
			if nodeID != n.ID {
				go func(id int) {
					var reply bool
					n.callRPC(id, "Commit", commitMsg, &reply)
				}(nodeID)
			}
		}

		// Execute locally
		n.mu.Lock()
		success, message := n.executeRequest(seqNum, request)
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

		return reply
	}

	return datatypes.ReplyMsg{
		Ballot:    n.CurrentBallot,
		Timestamp: request.Timestamp,
		ClientID:  request.ClientID,
		Success:   false,
		Message:   "consensus failed",
	}
}

func (n *Node) HandlePrepare(args datatypes.PrepareMsg, reply *datatypes.PromiseMsg) error {
	n.mu.Lock()
	//n.lastHeartBeat = time.Now()
	n.lastLeaderMsg = time.Now()

	defer n.mu.Unlock()

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

func (n *Node) HandleAccept(args datatypes.AcceptMsg, reply *datatypes.AcceptedMsg) error {
	n.mu.Lock()
	//n.lastHeartBeat = time.Now()
	n.lastLeaderMsg = time.Now()

	defer n.mu.Unlock()

	if args.Ballot.GreaterThanOrEqual(n.HighestPromised) {
		logEntry := datatypes.LogEntry{
			Ballot:  args.Ballot,
			SeqNum:  args.SeqNum,
			Request: args.Request,
			Status:  datatypes.StatusAccepted,
		}

		n.AcceptedLog[args.SeqNum] = logEntry
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

func (n *Node) HandleCommit(args datatypes.CommitMsg, reply *bool) error {
	n.mu.Lock()
	//n.lastHeartBeat = time.Now()
	n.lastLeaderMsg = time.Now()

	defer n.mu.Unlock()

	activeCount := 0
	for _, active := range n.ActiveNodes {
		if active {
			activeCount++
		}
	}

	requiredMajority := n.majorityThresholdLocked()
	if activeCount < requiredMajority {
		log.Printf("Node %d: ignoring commit seq %d (active nodes %d, needed %d)", n.ID, args.SeqNum, activeCount, requiredMajority)
		*reply = false
		return nil
	}

	if entry, exists := n.AcceptedLog[args.SeqNum]; exists {
		entry.Status = datatypes.StatusCommitted
		n.AcceptedLog[args.SeqNum] = entry

		for i := range n.RequestLog {
			if n.RequestLog[i].SeqNum == args.SeqNum && n.RequestLog[i].Ballot == args.Ballot {
				n.RequestLog[i].Status = datatypes.StatusCommitted
				break
			}
		}
	}

	n.executeRequestsInOrder()
	*reply = true

	log.Printf("Node %d: Committed seq %d\n", n.ID, args.SeqNum)
	return nil
}

func (n *Node) HandleNewView(args datatypes.NewViewMsg, reply *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Ballot.LessThan(n.HighestPromised) {
		log.Printf("Node %d: Ignoring outdated New-View for ballot %v (current promise %v)", n.ID, args.Ballot, n.HighestPromised)
		*reply = false
		return nil
	}

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

	// New view supersedes any pending accepts from a previous leadership attempt.
	n.pendingAccepts = make(map[int]map[int]datatypes.AcceptedMsg)

	if maxSeq == 0 {
		n.NextSeqNum = 1
	} else {
		n.NextSeqNum = maxSeq + 1
	}

	n.applyCommittedEntries(prevAccepted, maxSeq)

	// Send accepted confirmation back to leader for each entry we adopt
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

	*reply = true
	log.Printf("Node %d: Processed New-View from ballot %s with %d entries\n",
		n.ID, args.Ballot, len(args.AcceptLog))
	return nil
}

func (n *Node) executeRequest(seqNum int, request datatypes.ClientRequest) (bool, string) {
	if request.IsNoOp {
		if entry, exists := n.AcceptedLog[seqNum]; exists {
			entry.Status = datatypes.StatusExecuted
			n.AcceptedLog[seqNum] = entry
		}
		return true, "no-op executed"
	}

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

	return success, message
}

func (n *Node) executeRequestsInOrder() {
	for seqNum := 1; ; seqNum++ {
		entry, exists := n.AcceptedLog[seqNum]
		if !exists {
			break
		}

		if entry.Status == datatypes.StatusCommitted {
			n.executeRequest(seqNum, entry.Request)
		}
	}
}

func (n *Node) applyCommittedEntries(prevAccepted map[int]datatypes.LogEntry, maxSeq int) {
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

func (n *Node) StartLeaderElection() bool {
	n.mu.Lock()
	if !n.ActiveNodes[n.ID] {
		n.mu.Unlock()
		return false
	}
	if n.IsLeader {
		n.mu.Unlock()
		return false
	}
	n.CurrentBallot.Number++
	n.CurrentBallot.NodeID = n.ID
	ballot := n.CurrentBallot
	n.mu.Unlock()

	log.Printf("Node %d: Starting leader election with ballot %s\n", n.ID, ballot)

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

	requiredMajority := n.majorityThreshold()

	if promiseCount >= requiredMajority {
		n.mu.Lock()
		if !n.ActiveNodes[n.ID] {
			n.mu.Unlock()
			return false
		}
		n.IsLeader = true
		n.CurrentBallot = ballot
		go n.sendHeartbeats()
		log.Printf("Node %d: Became leader with ballot %s (promises: %d)\n",
			n.ID, ballot, promiseCount)

		acceptLog := n.createNewViewFromPromises(promises)

		newViewMsg := datatypes.NewViewMsg{
			Ballot:    ballot,
			AcceptLog: acceptLog,
		}
		n.NewViewMsgs = append(n.NewViewMsgs, newViewMsg)
		n.mu.Unlock()

		n.sendNewViewMessages(newViewMsg)

		if len(acceptLog) == 0 {
			return true
		}

		// Wait for accepted responses and commit
		time.Sleep(500 * time.Millisecond)

		for _, entry := range acceptLog {
			n.mu.Lock()
			acceptCount := 0
			if n.pendingAccepts[entry.SeqNum] != nil {
				acceptCount = len(n.pendingAccepts[entry.SeqNum])
			}
			n.mu.Unlock()

			if acceptCount >= n.majorityThreshold() {
				commitMsg := datatypes.CommitMsg{
					Ballot:  ballot,
					SeqNum:  entry.SeqNum,
					Request: entry.Request,
				}

				for nodeID := range n.Peers {
					if nodeID != n.ID {
						go func(id int) {
							var reply bool
							n.callRPC(id, "Commit", commitMsg, &reply)
						}(nodeID)
					}
				}

				n.mu.Lock()
				n.executeRequest(entry.SeqNum, entry.Request)
				n.mu.Unlock()
			}
		}

		return true
	}

	log.Printf("Node %d: Leader election failed (promises: %d, needed: %d)\n",
		n.ID, promiseCount, requiredMajority)
	return false
}

func (n *Node) createNewViewFromPromises(promises map[int]datatypes.PromiseMsg) []datatypes.AcceptLogEntry {
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

	acceptLog := make([]datatypes.AcceptLogEntry, 0, maxSeq)
	for seqNum := 1; seqNum <= maxSeq; seqNum++ {
		if entry, exists := allEntries[seqNum]; exists {
			entry.AcceptNum = n.CurrentBallot
			acceptLog = append(acceptLog, entry)
			continue
		}

		noOpRequest := datatypes.ClientRequest{
			ClientID:  fmt.Sprintf("no-op-%d", seqNum),
			Timestamp: time.Now().UnixNano(),
			IsNoOp:    true,
			Transaction: datatypes.Txn{
				Sender:   "no-op",
				Receiver: "no-op",
				Amount:   0,
			},
		}
		noOpEntry := datatypes.AcceptLogEntry{
			AcceptNum: n.CurrentBallot,
			SeqNum:    seqNum,
			Request:   noOpRequest,
			Status:    datatypes.StatusAccepted,
		}
		acceptLog = append(acceptLog, noOpEntry)
	}

	prevAccepted := n.AcceptedLog

	// Rebuild local canonical log based on acceptLog
	if maxSeq == 0 {
		n.AcceptedLog = make(map[int]datatypes.LogEntry)
		n.RequestLog = make([]datatypes.LogEntry, 0)
		if n.NextSeqNum < 1 {
			n.NextSeqNum = 1
		}
	} else {
		newAcceptedLog := make(map[int]datatypes.LogEntry, len(acceptLog))
		newRequestLog := make([]datatypes.LogEntry, 0, len(acceptLog))

		for _, entry := range acceptLog {
			status := entry.Status
			if existing, ok := prevAccepted[entry.SeqNum]; ok {
				status = maxStatus(status, existing.Status)
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
		n.NextSeqNum = maxSeq + 1
	}

	n.applyCommittedEntries(prevAccepted, maxSeq)

	// Reset pending accepts to reflect new view
	keepSeq := make(map[int]struct{}, len(acceptLog))
	for _, entry := range acceptLog {
		keepSeq[entry.SeqNum] = struct{}{}
		if n.pendingAccepts[entry.SeqNum] == nil {
			n.pendingAccepts[entry.SeqNum] = make(map[int]datatypes.AcceptedMsg)
		} else {
			for k := range n.pendingAccepts[entry.SeqNum] {
				delete(n.pendingAccepts[entry.SeqNum], k)
			}
		}
		n.pendingAccepts[entry.SeqNum][n.ID] = datatypes.AcceptedMsg{
			Ballot:  n.CurrentBallot,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
			NodeID:  n.ID,
		}
	}
	for seq := range n.pendingAccepts {
		if _, ok := keepSeq[seq]; !ok {
			delete(n.pendingAccepts, seq)
		}
	}

	return acceptLog
}

func (n *Node) sendNewViewMessages(msg datatypes.NewViewMsg) {
	for nodeID := range n.Peers {
		if nodeID != n.ID {
			go func(id int) {
				var reply bool
				n.callRPC(id, "NewView", msg, &reply)
			}(nodeID)
		}
	}
}

func (n *Node) buildStateSnapshot() datatypes.NewViewMsg {
	n.mu.RLock()
	defer n.mu.RUnlock()

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

	return datatypes.NewViewMsg{
		Ballot:    n.CurrentBallot,
		AcceptLog: acceptLog,
	}
}

func (n *Node) sendStateSnapshot(targetID int) {
	if targetID == n.ID {
		return
	}

	snapshot := n.buildStateSnapshot()
	var reply bool
	if err := n.callRPC(targetID, "NewView", snapshot, &reply); err != nil {
		log.Printf("Node %d: state snapshot to %d failed: %v", n.ID, targetID, err)
	}
}

func (n *Node) requestStateSync() {
	for attempt := 0; attempt < 5; attempt++ {
		n.mu.RLock()
		leaderID := n.CurrentBallot.NodeID
		isLeader := n.IsLeader && n.ActiveNodes[n.ID]
		leaderActive := leaderID != 0 && n.ActiveNodes[leaderID]
		n.mu.RUnlock()

		if isLeader {
			return
		}

		if !leaderActive || leaderID == n.ID {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		args := datatypes.StateTransferArgs{RequesterID: n.ID}
		var reply datatypes.StateTransferReply
		if err := n.callRPC(leaderID, "RequestStateTransfer", args, &reply); err != nil || !reply.Success {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		var applied bool
		if err := n.HandleNewView(reply.Snapshot, &applied); err != nil || !applied {
			time.Sleep(300 * time.Millisecond)
			continue
		}

		log.Printf("Node %d: synchronized state from leader %d (%d entries)", n.ID, leaderID, len(reply.Snapshot.AcceptLog))
		return
	}

	log.Printf("Node %d: failed to synchronize state after retries", n.ID)
}

// Print Functions (Required by project specification)

func (s *NodeService) PrintLog(_ bool, reply *string) error {
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

// PrintStatus returns the status (A, C, E, or X) of the transaction at a given sequence number.
// PrintStatus returns the status (A, C, E, or X) of the transaction at a given sequence number.
func (s *NodeService) PrintStatus(seqNum int, reply *string) error {
	s.node.mu.RLock()
	defer s.node.mu.RUnlock()

	// Self inactive? → return X (StatusNoStatus)
	if !s.node.ActiveNodes[s.node.ID] {
		*reply = fmt.Sprintf("Node %d inactive (Status: %s)", s.node.ID, datatypes.StatusNoStatus)
		return nil
	}

	// 1️⃣ Check if sequence exists in AcceptedLog
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

	// 2️⃣ If not in AcceptedLog, check if in RequestLog (leader may have appended but not yet accepted)
	for _, entry := range s.node.RequestLog {
		if entry.SeqNum == seqNum {
			*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Pending/Accepted)", s.node.ID, seqNum, datatypes.StatusAccepted)
			return nil
		}
	}

	// 3️⃣ If still not found
	*reply = fmt.Sprintf("Node %d: Seq %d | Status: %s (Not Found)", s.node.ID, seqNum, datatypes.StatusNoStatus)
	return nil
}

func (s *NodeService) PrintView(_ bool, reply *string) error {
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

//  1. Define the argument and reply structs for the new RPC call.
//     (You would add these in your datatypes package)
//
// In datatypes/types.go:

// 2. Add the new method to your NodeService.
// This is the public-facing "menu item".
func (ns *NodeService) PrintDB(args datatypes.PrintDBArgs, reply *datatypes.PrintDBReply) error {
	log.Printf("Node %d: Received RPC request to print DB.\n", ns.node.ID)

	// It calls the internal function to get the data.
	dbContents := ns.node.Database.PrintDB(ns.node.ID)

	// It populates the reply struct to send the data back to the client.
	reply.DBContents = dbContents

	return nil
}
