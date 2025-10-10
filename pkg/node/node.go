package node

import (
	"fmt"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"multipaxos/rituraj735/pkg/database"
	"net"
	"net/rpc"
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
	lastHeartBeat    time.Time
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
		lastHeartBeat:   time.Now(),
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
		last := n.lastHeartBeat
		cooldown := n.electionCoolDown
		n.mu.RUnlock()

		if isLeader {
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

	if ns.node.pendingAccepts[args.SeqNum] == nil {
		ns.node.pendingAccepts[args.SeqNum] = make(map[int]datatypes.AcceptedMsg)
	}
	ns.node.pendingAccepts[args.SeqNum][args.NodeID] = args

	*reply = args
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

	n.node.lastHeartBeat = time.Now()

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
	n.RequestLog = append(n.RequestLog, logEntry)

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
		n.RequestLog = append(n.RequestLog, logEntry)

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
	n.lastLeaderMsg = time.Now()
	defer n.mu.Unlock()

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
	n.lastLeaderMsg = time.Now()
	defer n.mu.Unlock()

	n.NewViewMsgs = append(n.NewViewMsgs, args)
	n.CurrentBallot = args.Ballot
	n.HighestPromised = args.Ballot

	// Update next sequence number
	maxSeq := n.NextSeqNum - 1
	for _, entry := range args.AcceptLog {
		if entry.SeqNum > maxSeq {
			maxSeq = entry.SeqNum
		}

		logEntry := datatypes.LogEntry{
			Ballot:  entry.AcceptNum,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
			Status:  datatypes.StatusAccepted,
		}
		n.AcceptedLog[entry.SeqNum] = logEntry
		n.RequestLog = append(n.RequestLog, logEntry)

		// Send accepted back to leader
		go func(e datatypes.AcceptLogEntry) {
			var acceptedReply datatypes.AcceptedMsg
			acceptedMsg := datatypes.AcceptedMsg{
				Ballot:  args.Ballot,
				SeqNum:  e.SeqNum,
				Request: e.Request,
				NodeID:  n.ID,
			}
			// Find leader and send
			for nodeID := range n.Peers {
				if nodeID == args.Ballot.NodeID {
					n.callRPC(nodeID, "AcceptedFromNewView", acceptedMsg, &acceptedReply)
					break
				}
			}
		}(entry)
	}

	if maxSeq >= n.NextSeqNum {
		n.NextSeqNum = maxSeq + 1
	}

	*reply = true
	log.Printf("Node %d: Processed new-view from ballot %s with %d entries\n",
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

func (n *Node) StartLeaderElection() bool {
	n.mu.Lock()
	n.CurrentBallot.Number++
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

	if promiseCount >= n.MajoritySize {
		n.mu.Lock()
		n.IsLeader = true
		n.CurrentBallot = ballot
		go n.sendHeartbeats()
		log.Printf("Node %d: Became leader with ballot %s (promises: %d)\n",
			n.ID, ballot, promiseCount)

		acceptLog := n.createNewViewFromPromises(promises)
		n.mu.Unlock()

		if len(acceptLog) > 0 {
			newViewMsg := datatypes.NewViewMsg{
				Ballot:    ballot,
				AcceptLog: acceptLog,
			}

			n.mu.Lock()
			n.NewViewMsgs = append(n.NewViewMsgs, newViewMsg)
			n.mu.Unlock()

			n.sendNewViewMessages(newViewMsg)

			// Wait for accepted responses and commit
			time.Sleep(500 * time.Millisecond)

			for _, entry := range acceptLog {
				n.mu.Lock()
				acceptCount := 0
				if n.pendingAccepts[entry.SeqNum] != nil {
					acceptCount = len(n.pendingAccepts[entry.SeqNum])
				}
				n.mu.Unlock()

				if acceptCount >= n.MajoritySize {
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
		}

		return true
	}

	log.Printf("Node %d: Leader election failed (promises: %d, needed: %d)\n",
		n.ID, promiseCount, n.MajoritySize)
	return false
}

func (n *Node) createNewViewFromPromises(promises map[int]datatypes.PromiseMsg) []datatypes.AcceptLogEntry {
	allEntries := make(map[int]datatypes.AcceptLogEntry)

	for _, promise := range promises {
		for _, entry := range promise.AcceptLog {
			if existing, exists := allEntries[entry.SeqNum]; !exists || entry.AcceptNum.GreaterThan(existing.AcceptNum) {
				allEntries[entry.SeqNum] = entry
			}
		}
	}

	if len(allEntries) == 0 {
		return nil
	}

	maxSeq := 0
	for seqNum := range allEntries {
		if seqNum > maxSeq {
			maxSeq = seqNum
		}
	}

	acceptLog := make([]datatypes.AcceptLogEntry, 0)
	for seqNum := 1; seqNum <= maxSeq; seqNum++ {
		if entry, exists := allEntries[seqNum]; exists {
			entry.AcceptNum = n.CurrentBallot
			acceptLog = append(acceptLog, entry)
		} else {
			noOpEntry := datatypes.AcceptLogEntry{
				AcceptNum: n.CurrentBallot,
				SeqNum:    seqNum,
				Request:   datatypes.ClientRequest{IsNoOp: true},
			}
			acceptLog = append(acceptLog, noOpEntry)
		}
	}

	// Initialize pending accepts for these entries
	for _, entry := range acceptLog {
		if n.pendingAccepts[entry.SeqNum] == nil {
			n.pendingAccepts[entry.SeqNum] = make(map[int]datatypes.AcceptedMsg)
		}
		// Count self as accepted
		n.pendingAccepts[entry.SeqNum][n.ID] = datatypes.AcceptedMsg{
			Ballot:  n.CurrentBallot,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
			NodeID:  n.ID,
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

// Print Functions (Required by project specification)

func (n *Node) PrintLog() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Printf("=== Node %d Log ===\n", n.ID)
	for _, entry := range n.RequestLog {
		fmt.Printf("Seq %d: %s Ballot %s Status %s\n",
			entry.SeqNum, entry.Request, entry.Ballot, entry.Status)
	}
	fmt.Println()
}

func (n *Node) PrintStatus(seqNum int) datatypes.RequestStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if entry, exists := n.AcceptedLog[seqNum]; exists {
		return entry.Status
	}
	return datatypes.StatusNoStatus
}

func (n *Node) PrintView() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	fmt.Printf("=== Node %d New-View Messages ===\n", n.ID)
	for i, msg := range n.NewViewMsgs {
		fmt.Printf("New-View %d: Ballot %s\n", i+1, msg.Ballot)
		fmt.Printf("  AcceptLog entries: %d\n", len(msg.AcceptLog))
		for _, entry := range msg.AcceptLog {
			fmt.Printf("    Seq %d: %s Ballot %s\n", entry.SeqNum, entry.Request, entry.AcceptNum)
		}
	}
	fmt.Println()
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
