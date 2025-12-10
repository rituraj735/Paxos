package node

import (
	"fmt"
	"log"
	"multipaxos/rituraj735/config"
	"multipaxos/rituraj735/datatypes"
	"multipaxos/rituraj735/pkg/database"
	"multipaxos/rituraj735/pkg/shard"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Node struct {
	ID       int
	Address  string
	Peers    map[int]string
	IsLeader bool

	ClusterID int

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

	Locks map[int]LockInfo

	TxnStates map[string]*TxnState

	Pending2PCAcks map[string]*PendingDecision

	execRunning bool
}

type PendingDecision struct {
	TxnID       string
	Decision    datatypes.TwoPCDecision
	DestCID     int
	Attempts    int
	LastAttempt time.Time
}

type NodeService struct {
	node *Node
}

type LockInfo struct {
	TxnID     string
	Ballot    datatypes.BallotNumber
	CreatedAt time.Time
}

type TxnPhase string

const (
	TxnPhaseNone      TxnPhase = ""
	TxnPhasePrepared  TxnPhase = "PREPARED"
	TxnPhaseCommitted TxnPhase = "COMMITTED"
	TxnPhaseAborted   TxnPhase = "ABORTED"
)

type TxnRole string

const (
	TxnRoleNone  TxnRole = ""
	TxnRoleCoord TxnRole = "COORD"
	TxnRolePart  TxnRole = "PART"
)

type TxnState struct {
	TxnID       string
	Role        TxnRole
	Phase       TxnPhase
	SeqPrepare  int
	SeqDecision int
	Decision    datatypes.TwoPCDecision
	Shards      []int

	S           int
	R           int
	Amount      int
	SourceCID   int
	DestCID     int
	LockHeldOnR bool
}

func (n *Node) getOrCreateTxnStateLocked(txnID string) *TxnState {
	ts, ok := n.TxnStates[txnID]
	if !ok {
		ts = &TxnState{TxnID: txnID, Phase: TxnPhaseNone, Role: TxnRoleNone}
		n.TxnStates[txnID] = ts
	}
	return ts
}

func (n *Node) setTxnPhaseLocked(txnID string, phase TxnPhase) {
	ts := n.getOrCreateTxnStateLocked(txnID)
	ts.Phase = phase
}

func (n *Node) getOrCreateParticipantTxnStateLocked(txnID string, args datatypes.TwoPCPrepareArgs) *TxnState {
	ts, ok := n.TxnStates[txnID]
	if !ok {
		ts = &TxnState{TxnID: txnID}
		n.TxnStates[txnID] = ts
	}
	ts.Role = TxnRolePart
	ts.S = args.S
	ts.R = args.R
	ts.Amount = args.Amount
	ts.SourceCID = args.SourceCID
	ts.DestCID = args.DestCID
	return ts
}

func (n *Node) clusterPeerIDs() []int {
	ids := make([]int, 0)
	members, ok := config.ClusterMembers[n.ClusterID]
	if ok && len(members) > 0 {
		for _, id := range members {
			if _, exists := n.Peers[id]; exists {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			return ids
		}
	}

	for id := range n.Peers {
		ids = append(ids, id)
	}
	return ids
}

func (n *Node) clusterMajorityLocked() int {
	peers := n.clusterPeerIDs()
	size := len(peers)
	if size <= 0 {
		return 1
	}
	maj := size/2 + 1
	// if maj < 1 {
	//     maj = 1
	// }
	return maj
}

func (n *Node) recordPrepareSeqLocked(txnID string, seq int) {
	ts := n.getOrCreateTxnStateLocked(txnID)
	ts.SeqPrepare = seq
}

func (n *Node) recordDecisionSeqLocked(txnID string, seq int, decision datatypes.TwoPCDecision) {
	ts := n.getOrCreateTxnStateLocked(txnID)
	ts.SeqDecision = seq
	ts.Decision = decision
}

func (n *Node) tryLockLocked(txnID string, ids ...int) bool {
	sort.Ints(ids)
	currBallot := n.CurrentBallot

	for _, id := range ids {
		if info, ok := n.Locks[id]; ok {
			if info.Ballot.LessThan(currBallot) {
				log.Printf("Node %d [LOCK]: dropping stale lock id=%d tx=%s ballot=%s (current=%s)",
					n.ID, id, info.TxnID, info.Ballot.String(), currBallot.String())
				delete(n.Locks, id)
			}
		}
	}

	for _, id := range ids {
		if info, ok := n.Locks[id]; ok && info.TxnID != txnID {
			log.Printf("Node %d [LOCK]: tryLock DENIED tx=%s id=%d heldBy=%s ballot=%s",
				n.ID, txnID, id, info.TxnID, info.Ballot.String())
			return false
		}
	}

	now := time.Now()

	for _, id := range ids {
		if curr, ok := n.Locks[id]; !ok || curr.TxnID != txnID {
			n.Locks[id] = LockInfo{TxnID: txnID, Ballot: currBallot, CreatedAt: now}
			log.Printf("Node %d [LOCK]: acquired tx=%s id=%d ballot=%s", n.ID, txnID, id, currBallot.String())
		}
	}
	return true
}

func (n *Node) unlockLocked(txnID string, ids ...int) {
	sort.Ints(ids)
	for _, id := range ids {
		if info, ok := n.Locks[id]; ok && info.TxnID == txnID {
			delete(n.Locks, id)
			log.Printf("Node %d [LOCK]: released tx=%s id=%d", n.ID, txnID, id)
		}
	}
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
	log.Printf("Node %d: initializing at %s with %d peers", id, address, len(peers))
	node := &Node{
		ID:              id,
		Address:         address,
		Peers:           peers,
		IsLeader:        false,
		ClusterID:       0,
		CurrentBallot:   datatypes.BallotNumber{Number: 0, NodeID: id},
		HighestPromised: datatypes.BallotNumber{Number: 0, NodeID: 0},
		NextSeqNum:      1,
		AcceptedLog:     make(map[int]datatypes.LogEntry),
		RequestLog:      make([]datatypes.LogEntry, 0),
		NewViewMsgs:     make([]datatypes.NewViewMsg, 0),
		LastReply:       make(map[string]datatypes.ReplyMsg),
		Database:        nil,
		pendingAccepts:  make(map[int]map[int]datatypes.AcceptedMsg),
		ActiveNodes:     make(map[int]bool),
		MajoritySize:    config.MajoritySize,
		shutdown:        make(chan bool),
		lastLeaderMsg:   time.Now(),
		ackFromNewView:  make(map[int]bool),
		Locks:           make(map[int]LockInfo),
		TxnStates:       make(map[string]*TxnState),
		Pending2PCAcks:  make(map[string]*PendingDecision),
	}

	dataDir := "data"
	_ = os.MkdirAll(dataDir, 0o755)
	dbPath := filepath.Join(dataDir, fmt.Sprintf("node-%d.db", id))
	if config.WipeDataOnBoot {
		_ = os.Remove(dbPath)
	}
	boltDB, err := database.NewBoltDatabase(dbPath)
	if err != nil {
		log.Fatalf("Node %d: failed to open BoltDB at %s: %v", id, dbPath, err)
	}
	node.Database = boltDB
	log.Printf("Node %d: BoltDB initialized at %s", id, dbPath)

	if err := shard.LoadOverridesFromFile(); err != nil {
		log.Printf("Node %d: no shard overrides loaded (%v)", id, err)
	}
	go node.monitorLeaderTimeout()

	for cid, members := range config.ClusterMembers {
		for _, mid := range members {
			if mid == id {
				node.ClusterID = cid
				break
			}
		}
		if node.ClusterID != 0 {
			break
		}
	}

	if node.ClusterID != 0 {
		rng := config.ClusterRanges[node.ClusterID]
		for acc := rng.Min; acc <= rng.Max; acc++ {
			node.Database.InitializeClient(strconv.Itoa(acc), config.InitialBalance)
		}
	} else {

		for acc := config.MinAccountID; acc <= config.MaxAccountID; acc++ {
			node.Database.InitializeClient(strconv.Itoa(acc), config.InitialBalance)
		}
	}

	for nodeID := range peers {
		node.ActiveNodes[nodeID] = true
	}
	node.ActiveNodes[id] = true

	log.Printf("Node %d: initialization complete (maj=%d)", id, node.MajoritySize)
	return node
}

func (n *Node) TryLock(txnID string, ids ...int) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.tryLockLocked(txnID, ids...)
}

func (n *Node) Unlock(txnID string, ids ...int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.unlockLocked(txnID, ids...)
}

func (n *Node) monitorLeaderTimeout() {
	log.Printf("Node %d: leader timeout monitor running", n.ID)
	for {
		time.Sleep(200 * time.Millisecond)
		n.mu.RLock()
		selfActive := n.ActiveNodes[n.ID]
		isLeader := n.IsLeader
		last := n.lastLeaderMsg
		cooldown := n.electionCoolDown
		n.mu.RUnlock()

		if !selfActive {

			continue
		}

		if isLeader {
			n.mu.Lock()
			n.lastLeaderMsg = time.Now()
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

func (n *Node) Stop() {
	log.Printf("Node %d: stopping node", n.ID)
	close(n.shutdown)
	if n.listener != nil {
		n.listener.Close()
	}
	if n.Database != nil {
		_ = n.Database.Close()
	}
}

func (n *Node) callRPC(nodeID int, method string, args interface{}, reply interface{}) error {
	n.mu.RLock()
	if !n.ActiveNodes[nodeID] {
		n.mu.RUnlock()
		log.Printf("Node %d: skipping RPC %s to inactive node %d", n.ID, method, nodeID)
		return fmt.Errorf("node %d is not active", nodeID)
	}
	n.mu.RUnlock()
	if method != "HandleHeartbeat" {
		log.Printf("Node %d: RPC %s->node %d", n.ID, method, nodeID)
	}

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

func (ns *NodeService) HandleClientRequest(args datatypes.ClientRequestRPC, reply *datatypes.ClientReplyRPC) error {
	log.Printf("something reached handleClientRequest", args)
	log.Printf("Node %d: HandleClientRequest type=%s client=%s ts=%d", ns.node.ID, args.Request.MessageType, args.Request.ClientID, args.Request.Timestamp)
	replyMsg := ns.node.ProcessClientRequest(args.Request)
	reply.Reply = replyMsg
	return nil
}

func (s *NodeService) TwoPCPrepare(args datatypes.TwoPCPrepareArgs, reply *datatypes.TwoPCPrepareReply) error {
	n := s.node
	n.mu.Lock()
	defer n.mu.Unlock()

	*reply = datatypes.TwoPCPrepareReply{TxnID: args.TxnID}

	if !n.IsLeader || !n.ActiveNodes[n.ID] || (n.ClusterID != args.DestCID && args.DestCID != 0) {
		reply.Success = false
		reply.Message = "not leader"
		return nil
	}

	st := n.getOrCreateParticipantTxnStateLocked(args.TxnID, args)

	if st.Phase == TxnPhasePrepared || st.Phase == TxnPhaseCommitted {
		reply.Success = true
		reply.Message = "already prepared"
		return nil
	}
	if st.Phase == TxnPhaseAborted {
		reply.Success = false
		reply.Message = "already aborted"
		return nil
	}

	if !n.tryLockLocked(args.TxnID, args.R) {
		reply.Success = false
		reply.Message = "receiver locked"
		return nil
	}
	st.LockHeldOnR = true

	seqReq := datatypes.ClientRequest{
		MessageType: "BANK_TXN",
		ClientID:    args.ClientID,
		Timestamp:   args.ClientTS,
		Transaction: datatypes.Txn{Sender: strconv.Itoa(args.S), Receiver: strconv.Itoa(args.R), Amount: args.Amount},
		TxnID:       args.TxnID,
		TwoPCPhase:  datatypes.TwoPCPhasePrepare,
		IsCross:     true,
	}
	n.mu.Unlock()
	seqNum, ok := n.proposeAndWait(seqReq)
	n.mu.Lock()
	if !ok {
		if st.LockHeldOnR {
			n.unlockLocked(args.TxnID, args.R)
			st.LockHeldOnR = false
		}
		st.Phase = TxnPhaseAborted
		reply.Success = false
		reply.Message = "prepare paxos failed"
		return nil
	}
	st.SeqPrepare = seqNum
	reply.Success = true
	reply.Message = "prepared-logged"
	return nil
}

func (s *NodeService) TwoPCDecision(args datatypes.TwoPCDecisionArgs, reply *datatypes.TwoPCDecisionReply) error {
	n := s.node
	n.mu.Lock()
	defer n.mu.Unlock()

	*reply = datatypes.TwoPCDecisionReply{TxnID: args.TxnID}
	if !n.IsLeader || !n.ActiveNodes[n.ID] {
		reply.Acked = false
		reply.Message = "not leader"
		return nil
	}
	st, ok := n.TxnStates[args.TxnID]
	if !ok {
		st = &TxnState{TxnID: args.TxnID, Role: TxnRolePart}
		n.TxnStates[args.TxnID] = st
	}

	if st.Phase == TxnPhaseCommitted && args.Decision == datatypes.TwoPCDecisionCommit {
		reply.Acked = true
		reply.Message = "already committed"
		return nil
	}
	if st.Phase == TxnPhaseAborted && args.Decision == datatypes.TwoPCDecisionAbort {
		reply.Acked = true
		reply.Message = "already aborted"
		return nil
	}

	req := datatypes.ClientRequest{
		MessageType: "BANK_TXN",
		ClientID:    "2pc-decision-" + args.TxnID,
		Timestamp:   time.Now().UnixNano(),
		Transaction: datatypes.Txn{Sender: strconv.Itoa(st.S), Receiver: strconv.Itoa(st.R), Amount: st.Amount},
		TxnID:       args.TxnID,
		TwoPCPhase:  datatypes.TwoPCPhase(args.Decision),
		IsCross:     true,
	}
	n.mu.Unlock()
	seqNum, ok := n.proposeAndWait(req)
	n.mu.Lock()
	if !ok {
		reply.Acked = false
		reply.Message = "decision paxos failed"
		return nil
	}
	st.SeqDecision = seqNum
	reply.Acked = true
	reply.Message = "decision-logged"
	return nil
}

func (ns *NodeService) Prepare(args datatypes.PrepareMsg, reply *datatypes.PromiseMsg) error {
	log.Printf("Node %d: Prepare RPC from ballot (%d,%d)", ns.node.ID, args.Ballot.Number, args.Ballot.NodeID)
	return ns.node.HandlePrepare(args, reply)
}

func (ns *NodeService) Accept(args datatypes.AcceptMsg, reply *datatypes.AcceptedMsg) error {
	log.Printf("Node %d: Accept RPC seq=%d ballot=(%d,%d)", ns.node.ID, args.SeqNum, args.Ballot.Number, args.Ballot.NodeID)
	return ns.node.HandleAccept(args, reply)
}

func (ns *NodeService) Commit(args datatypes.CommitMsg, reply *bool) error {
	log.Printf("Node %d: Commit RPC seq=%d ballot=(%d,%d)", ns.node.ID, args.SeqNum, args.Ballot.Number, args.Ballot.NodeID)
	return ns.node.HandleCommit(args, reply)
}

func (ns *NodeService) NewView(args datatypes.NewViewMsg, reply *bool) error {
	log.Printf("Node %d: NewView RPC ballot=(%d,%d) entries=%d", ns.node.ID, args.Ballot.Number, args.Ballot.NodeID, len(args.AcceptLog))
	return ns.node.HandleNewView(args, reply)
}

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

			s.node.RecoverFromCrash()
			s.node.requestStateSync()
		} else if isLeaderActive {

			go s.node.sendStateSnapshot(args.NodeID)
			go s.node.sendCommitReplay(args.NodeID)
		}
	}
	return nil
}

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

			s.node.RecoverFromCrash()
			s.node.requestStateSync()
		} else if isLeaderActive {
			go s.node.sendStateSnapshot(id)
			go s.node.sendCommitReplay(id)
		}
	}

	return nil
}

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

func (n *Node) majorityThreshold() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.majorityThresholdLocked()
}

func (n *Node) setNodeLiveness(nodeID int, isLive bool) (bool, bool) {
	log.Printf("Node %d: setNodeLiveness node=%d live=%v", n.ID, nodeID, isLive)
	prev, existed := n.ActiveNodes[nodeID]
	n.ActiveNodes[nodeID] = isLive

	becameActive := (!prev || !existed) && isLive
	leaderDemoted := false

	if nodeID == n.ID && !isLive {
		if n.IsLeader {
			n.clearAllLocksLocked("self marked inactive")
		} else {
			n.clearAllLocksLocked("self marked inactive")
		}
		n.IsLeader = false
	}

	if !isLive && n.CurrentBallot.NodeID == nodeID {
		n.CurrentBallot.NodeID = 0
		n.lastLeaderMsg = time.Now().Add(-2 * time.Duration(config.LeaderTimeout) * time.Millisecond)
		n.electionCoolDown = time.Time{}
		leaderDemoted = true
	}

	if isLive && nodeID == n.ID {

		leaderID := n.CurrentBallot.NodeID
		if leaderID != 0 && n.ActiveNodes[leaderID] {

			n.lastLeaderMsg = time.Now()
		} else {

			n.lastLeaderMsg = time.Now().Add(-2 * time.Duration(config.LeaderTimeout) * time.Millisecond)
			n.electionCoolDown = time.Time{}
		}
	}

	return leaderDemoted, becameActive
}

func (n *Node) clearAllLocksLocked(reason string) {
	if len(n.Locks) > 0 {
		log.Printf("Node %d [LOCK]: clearing %d locks (%s)", n.ID, len(n.Locks), reason)
	}
	n.Locks = make(map[int]LockInfo)
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
		if n.node.IsLeader {
			n.node.IsLeader = false
		}
		n.node.clearAllLocksLocked("ballot updated from heartbeat")
	}

	*reply = true
	return nil
}

func (n *Node) sendHeartbeats() {
	ticker := time.NewTicker(200 * time.Millisecond)
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

			for _, peerID := range n.clusterPeerIDs() {
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

func (n *Node) ProcessClientRequest(request datatypes.ClientRequest) datatypes.ReplyMsg {
	n.mu.Lock()
	log.Printf("Node %d: ProcessClientRequest client=%s ts=%d no-op=%v", n.ID, request.ClientID, request.Timestamp, request.IsNoOp)
	log.Printf("Inside processClientRequest")

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
	for _, id := range n.clusterPeerIDs() {
		if n.ActiveNodes[id] {
			activeCount++
		}
	}
	majNeeded := n.clusterMajorityLocked()
	if activeCount < majNeeded {
		log.Printf("Node %d: insufficient active nodes in cluster %d/%d", n.ID, activeCount, majNeeded)
		n.mu.Unlock()
		return datatypes.ReplyMsg{
			Ballot:    n.CurrentBallot,
			Timestamp: request.Timestamp,
			ClientID:  request.ClientID,
			Success:   false,
			Message:   "insufficient active nodes",
		}
	}

	isCross, xsID, xrID, xSrcCID, xDstCID := n.detectCrossShardBankTxn(request)
	if isCross {
		log.Printf("Node %d: detected cross-shard BANK_TXN (s=%d r=%d srcCID=%d dstCID=%d)", n.ID, xsID, xrID, xSrcCID, xDstCID)

		bal := n.Database.GetBalanceInt(xsID)
		if bal < request.Transaction.Amount {
			n.mu.Unlock()
			return datatypes.ReplyMsg{Ballot: n.CurrentBallot, Timestamp: request.Timestamp, ClientID: request.ClientID, Success: false, Message: "insufficient funds"}
		}
		n.mu.Unlock()
		return n.handleCrossShardCoordinator(request, xsID, xrID, xSrcCID, xDstCID)
	}

	isIntra, sID, rID, shardID := n.isIntraShardBankTxn(request)
	var txnID string
	var hasLocks bool
	if isIntra {
		if shardID != 0 && shardID != n.ClusterID {
			log.Printf("Node %d [WARN]: intra-shard BANK_TXN routed to wrong cluster (txn shard=%d, node shard=%d)", n.ID, shardID, n.ClusterID)
		}
		txnID = fmt.Sprintf("%s-%d", request.ClientID, request.Timestamp)
		if !n.tryLockLocked(txnID, sID, rID) {

			log.Printf("Node %d: intra-shard txn locked (s=%d r=%d) — rejecting", n.ID, sID, rID)
			n.mu.Unlock()
			return datatypes.ReplyMsg{
				Ballot:    n.CurrentBallot,
				Timestamp: request.Timestamp,
				ClientID:  request.ClientID,
				Success:   false,
				Message:   "locked",
			}
		}
		hasLocks = true
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

	if isIntra && hasLocks {
		defer n.Unlock(txnID, sID, rID)
	}

	for _, nodeID := range n.clusterPeerIDs() {
		if nodeID == n.ID {
			continue
		}
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

	maxWait := 50
	for i := 0; i < maxWait; i++ {
		time.Sleep(10 * time.Millisecond)
		n.mu.RLock()
		acceptCount := len(n.pendingAccepts[seqNum])
		maj := n.clusterMajorityLocked()
		n.mu.RUnlock()
		if acceptCount >= maj {
			break
		}
	}

	n.mu.Lock()
	acceptCount := len(n.pendingAccepts[seqNum])
	maj := n.clusterMajorityLocked()
	n.mu.Unlock()

	if acceptCount >= maj {
		commitMsg := datatypes.CommitMsg{
			Ballot:  n.CurrentBallot,
			SeqNum:  seqNum,
			Request: request,
		}

		for _, nodeID := range n.clusterPeerIDs() {
			if nodeID == n.ID {
				continue
			}
			n.mu.RLock()
			targetActive := n.ActiveNodes[nodeID]
			n.mu.RUnlock()
			if !targetActive {
				continue
			}
			go func(id int) { var reply bool; n.callRPC(id, "Commit", commitMsg, &reply) }(nodeID)
		}

		var applied bool
		_ = n.HandleCommit(commitMsg, &applied)

		//log.Printf("Node %d: COMMITTED seq=%d (%s→%s, %d)",n.ID,seqNum,request.Transaction.Sender,request.Transaction.Receiver,request.Transaction.Amount)

		//log.Printf("Node %d: EXECUTING seq=%d (%s→%s,%d)",n.ID,seqNum,request.Transaction.Sender,request.Transaction.Receiver,request.Transaction.Amount)

		success, message := n.executeRequest(seqNum, request)

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

func (n *Node) detectCrossShardBankTxn(req datatypes.ClientRequest) (bool, int, int, int, int) {

	if req.IsNoOp || strings.TrimSpace(req.Transaction.Receiver) == "" || req.Transaction.Amount <= 0 {
		return false, 0, 0, 0, 0
	}
	sID, err1 := strconv.Atoi(req.Transaction.Sender)
	rID, err2 := strconv.Atoi(req.Transaction.Receiver)
	if err1 != nil || err2 != nil {
		return false, 0, 0, 0, 0
	}
	cs := shard.ClusterOfItem(sID)
	cr := shard.ClusterOfItem(rID)
	if cs == 0 || cr == 0 || cs == cr {
		return false, sID, rID, cs, cr
	}
	return true, sID, rID, cs, cr
}

func (n *Node) handleCrossShardCoordinator(request datatypes.ClientRequest, sID, rID int, srcCID, dstCID int) datatypes.ReplyMsg {
	n.mu.Lock()

	if !n.IsLeader {
		n.mu.Unlock()
		return datatypes.ReplyMsg{Ballot: n.CurrentBallot, Timestamp: request.Timestamp, ClientID: request.ClientID, Success: false, Message: "not leader"}
	}
	active := 0
	for _, id := range n.clusterPeerIDs() {
		if n.ActiveNodes[id] {
			active++
		}
	}
	if active < n.clusterMajorityLocked() {
		n.mu.Unlock()
		return datatypes.ReplyMsg{Ballot: n.CurrentBallot, Timestamp: request.Timestamp, ClientID: request.ClientID, Success: false, Message: "insufficient active nodes"}
	}

	txnID := fmt.Sprintf("txn-%s-%d", request.ClientID, request.Timestamp)
	st := n.getOrCreateTxnStateLocked(txnID)
	st.TxnID = txnID
	st.Shards = []int{srcCID, dstCID}
	st.Role = TxnRoleCoord
	st.S, st.R, st.Amount = sID, rID, request.Transaction.Amount
	st.SourceCID, st.DestCID = srcCID, dstCID

	if !n.tryLockLocked(txnID, sID) {
		n.mu.Unlock()
		return datatypes.ReplyMsg{Ballot: n.CurrentBallot, Timestamp: request.Timestamp, ClientID: request.ClientID, Success: false, Message: "locked"}
	}

	prepReq := request
	prepReq.TxnID = txnID
	prepReq.IsCross = true
	prepReq.TwoPCPhase = datatypes.TwoPCPhasePrepare
	n.mu.Unlock()

	seqP, ok := n.proposeAndWait(prepReq)

	n.mu.Lock()
	if !ok {
		n.unlockLocked(txnID, sID)
		n.mu.Unlock()
		return datatypes.ReplyMsg{Ballot: n.CurrentBallot, Timestamp: request.Timestamp, ClientID: request.ClientID, Success: false, Message: "prepare-consensus-failed"}
	}
	st = n.getOrCreateTxnStateLocked(txnID)
	st.SeqPrepare = seqP
	st.Phase = TxnPhasePrepared

	args := datatypes.TwoPCPrepareArgs{
		TxnID: txnID, S: sID, R: rID, Amount: request.Transaction.Amount,
		SourceCID: srcCID, DestCID: dstCID, ClientID: request.ClientID, ClientTS: request.Timestamp,
	}
	var prepReply datatypes.TwoPCPrepareReply
	n.mu.Unlock()
	_ = n.call2PCPrepare(dstCID, args, &prepReply)
	n.mu.Lock()

	phase := datatypes.TwoPCPhaseCommit
	decision := datatypes.TwoPCDecisionCommit
	msg := "commit"
	if !prepReply.Success {
		phase = datatypes.TwoPCPhaseAbort
		decision = datatypes.TwoPCDecisionAbort
		msg = "abort"
	}

	decReq := request
	decReq.TxnID = txnID
	decReq.IsCross = true
	decReq.TwoPCPhase = phase
	n.mu.Unlock()
	seqD, ok := n.proposeAndWait(decReq)
	n.mu.Lock()
	if !ok {

		n.mu.Unlock()
		return datatypes.ReplyMsg{Ballot: n.CurrentBallot, Timestamp: request.Timestamp, ClientID: request.ClientID, Success: false, Message: "decision-consensus-failed-" + msg}
	}
	st = n.getOrCreateTxnStateLocked(txnID)
	st.SeqDecision = seqD
	st.Decision = decision
	if decision == datatypes.TwoPCDecisionCommit {
		st.Phase = TxnPhaseCommitted
	} else {
		st.Phase = TxnPhaseAborted
	}

	n.mu.Unlock()
	var decReply datatypes.TwoPCDecisionReply
	_ = n.call2PCDecision(dstCID, datatypes.TwoPCDecisionArgs{TxnID: txnID, Decision: datatypes.TwoPCDecision(phase)}, &decReply)

	n.mu.Lock()
	n.unlockLocked(txnID, sID)
	if !decReply.Acked {

		n.Pending2PCAcks[txnID] = &PendingDecision{TxnID: txnID, Decision: datatypes.TwoPCDecision(phase), DestCID: dstCID}
	} else {

		go n.Database.ClearWAL(txnID)
	}
	n.mu.Unlock()

	return datatypes.ReplyMsg{Ballot: n.CurrentBallot, Timestamp: request.Timestamp, ClientID: request.ClientID, Success: decision == datatypes.TwoPCDecisionCommit, Message: msg}
}

func (n *Node) call2PCPrepare(destCID int, args datatypes.TwoPCPrepareArgs, reply *datatypes.TwoPCPrepareReply) error {
	reply.TxnID = args.TxnID
	leaderID, err := n.findClusterLeader(destCID)
	if err != nil {
		reply.Success = false
		reply.Message = "no dest leader"
		return nil
	}

	address, ok := n.Peers[leaderID]
	if !ok {
		reply.Success = false
		reply.Message = "addr missing"
		return nil
	}
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		reply.Success = false
		reply.Message = "dial failed"
		return nil
	}
	defer client.Close()
	done := make(chan error, 1)
	go func() { done <- client.Call("NodeService.TwoPCPrepare", args, reply) }()
	select {
	case <-time.After(500 * time.Millisecond):
		reply.Success = false
		reply.Message = "timeout"
		return nil
	case err := <-done:
		if err != nil {
			reply.Success = false
			reply.Message = err.Error()
		}
		return nil
	}
}

func (n *Node) call2PCDecision(destCID int, args datatypes.TwoPCDecisionArgs, reply *datatypes.TwoPCDecisionReply) error {
	leaderID, err := n.findClusterLeader(destCID)
	if err != nil {
		reply.Acked = false
		reply.Message = "no dest leader"
		return nil
	}
	address, ok := n.Peers[leaderID]
	if !ok {
		reply.Acked = false
		reply.Message = "addr missing"
		return nil
	}
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		reply.Acked = false
		reply.Message = "dial failed"
		return nil
	}
	defer client.Close()
	done := make(chan error, 1)
	go func() { done <- client.Call("NodeService.TwoPCDecision", args, reply) }()
	select {
	case <-time.After(500 * time.Millisecond):
		reply.Acked = false
		reply.Message = "timeout"
		return nil
	case err := <-done:
		if err != nil {
			reply.Acked = false
			reply.Message = err.Error()
		}
		return nil
	}
}

func (n *Node) findClusterLeader(clusterID int) (int, error) {
	log.Printf("reached findClusterLeader for cluster %d", clusterID)
	members, ok := config.ClusterMembers[clusterID]
	if !ok {
		return 0, fmt.Errorf("unknown cluster %d", clusterID)
	}
	for _, id := range members {
		address, exists := n.Peers[id]
		if !exists {
			continue
		}
		client, err := rpc.Dial("tcp", address)
		if err != nil {
			continue
		}
		var info datatypes.LeaderInfo
		_ = client.Call("NodeService.GetLeader", true, &info)
		client.Close()
		if info.IsLeader && info.LeaderID == id {
			return id, nil
		}
	}
	return 0, fmt.Errorf("no leader for cluster %d", clusterID)
}

func (n *Node) proposeAndWait(req datatypes.ClientRequest) (int, bool) {

	n.mu.Lock()
	if !n.IsLeader {
		n.mu.Unlock()
		return 0, false
	}

	maj := n.clusterMajorityLocked()
	active := 0
	for _, id := range n.clusterPeerIDs() {
		if n.ActiveNodes[id] {
			active++
		}
	}
	if active < maj {
		n.mu.Unlock()
		return 0, false
	}
	seqNum := n.NextSeqNum
	n.NextSeqNum++
	acceptMsg := datatypes.AcceptMsg{Type: "ACCEPT", Ballot: n.CurrentBallot, SeqNum: seqNum, Request: req}
	logEntry := datatypes.LogEntry{Ballot: n.CurrentBallot, SeqNum: seqNum, Request: req, Status: datatypes.StatusAccepted}
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
	n.pendingAccepts[seqNum][n.ID] = datatypes.AcceptedMsg{Ballot: n.CurrentBallot, SeqNum: seqNum, Request: req, NodeID: n.ID}
	n.mu.Unlock()

	for _, nodeID := range n.clusterPeerIDs() {
		if nodeID == n.ID {
			continue
		}
		go func(id int) {
			var reply datatypes.AcceptedMsg
			if err := n.callRPC(id, "Accept", acceptMsg, &reply); err == nil {
				n.mu.Lock()
				if n.pendingAccepts[seqNum] == nil {
					n.pendingAccepts[seqNum] = make(map[int]datatypes.AcceptedMsg)
				}
				n.pendingAccepts[seqNum][id] = reply
				n.mu.Unlock()
			}
		}(nodeID)
	}

	for i := 0; i < 50; i++ {
		time.Sleep(10 * time.Millisecond)
		n.mu.RLock()
		acceptCount := len(n.pendingAccepts[seqNum])
		n.mu.RUnlock()
		if acceptCount >= n.clusterMajorityLocked() {
			break
		}
	}
	n.mu.Lock()
	acceptCount := len(n.pendingAccepts[seqNum])
	log.Printf("Node %d: proposeAndWait seq=%d acceptCount=%d needed=%d", n.ID, seqNum, acceptCount, n.clusterMajorityLocked())
	n.mu.Unlock()
	if acceptCount < n.clusterMajorityLocked() {
		return 0, false
	}

	commitMsg := datatypes.CommitMsg{Ballot: n.CurrentBallot, SeqNum: seqNum, Request: req}
	for _, nodeID := range n.clusterPeerIDs() {
		if nodeID == n.ID {
			continue
		}
		n.mu.RLock()
		targetActive := n.ActiveNodes[nodeID]
		n.mu.RUnlock()
		if !targetActive {
			continue
		}
		log.Printf("Node %d: proposeAndWait sending COMMIT seq=%d to node %d", n.ID, seqNum, nodeID)
		go func(id int) { var reply bool; n.callRPC(id, "Commit", commitMsg, &reply) }(nodeID)
	}

	var applied bool
	_ = n.HandleCommit(commitMsg, &applied)

	success, _ := n.executeRequest(seqNum, req)
	return seqNum, success
}

func (n *Node) isIntraShardBankTxn(req datatypes.ClientRequest) (bool, int, int, int) {

	if req.IsNoOp || strings.TrimSpace(req.Transaction.Receiver) == "" || req.Transaction.Amount <= 0 {
		return false, 0, 0, 0
	}
	sID, err1 := strconv.Atoi(req.Transaction.Sender)
	rID, err2 := strconv.Atoi(req.Transaction.Receiver)
	if err1 != nil || err2 != nil {
		return false, 0, 0, 0
	}
	cs := shard.ClusterOfItem(sID)
	cr := shard.ClusterOfItem(rID)
	if cs == 0 || cr == 0 || cs != cr {
		return false, sID, rID, 0
	}
	return true, sID, rID, cs
}

func (n *Node) HandlePrepare(args datatypes.PrepareMsg, reply *datatypes.PromiseMsg) error {
	n.mu.Lock()

	defer n.mu.Unlock()

	recent := time.Since(n.lastLeaderMsg) <= time.Duration(config.LeaderTimeout)*time.Millisecond
	leaderActive := n.CurrentBallot.NodeID != 0 && n.ActiveNodes[n.CurrentBallot.NodeID]
	if recent && leaderActive && n.CurrentBallot.NodeID != 0 && args.Ballot.LessThan(n.CurrentBallot) {

		higher := n.HighestPromised
		if n.CurrentBallot.GreaterThan(higher) {
			higher = n.CurrentBallot
		}
		*reply = datatypes.PromiseMsg{Ballot: higher, Success: false}
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

		higher := n.HighestPromised
		if n.CurrentBallot.GreaterThan(higher) {
			higher = n.CurrentBallot
		}
		*reply = datatypes.PromiseMsg{Ballot: higher, Success: false}
	}

	return nil
}

func (n *Node) HandleAccept(args datatypes.AcceptMsg, reply *datatypes.AcceptedMsg) error {
	n.mu.Lock()

	n.lastLeaderMsg = time.Now()
	defer n.mu.Unlock()

	if !n.ActiveNodes[n.ID] {
		return nil
	}

	if args.Ballot.GreaterThanOrEqual(n.HighestPromised) {

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

func (n *Node) HandleCommit(args datatypes.CommitMsg, reply *bool) error {
	n.mu.Lock()

	n.lastLeaderMsg = time.Now()

	if !n.ActiveNodes[n.ID] {
		*reply = false
		return nil
	}

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

	followerShouldExec := args.Ballot.NodeID != n.ID
	n.mu.Unlock()

	if followerShouldExec {
		n.triggerExecutor()
	}

	*reply = true
	return nil
}

func (n *Node) HandleNewView(args datatypes.NewViewMsg, reply *bool) error {

	n.mu.Lock()

	if members, ok := config.ClusterMembers[n.ClusterID]; ok {
		inCluster := false
		for _, id := range members {
			if id == args.Ballot.NodeID {
				inCluster = true
				break
			}
		}
		if !inCluster {
			n.mu.Unlock()
			*reply = false
			return nil
		}
	}
	if args.Ballot.LessThan(n.HighestPromised) {
		n.mu.Unlock()
		*reply = false
		return nil
	}

	if len(args.AcceptLog) == 0 && len(n.AcceptedLog) > 0 {
		n.lastProcessedView = args.Ballot
		n.lastLeaderMsg = time.Now()
		n.HighestPromised = args.Ballot
		n.CurrentBallot = args.Ballot
		n.IsLeader = args.Ballot.NodeID == n.ID
		n.NewViewMsgs = append(n.NewViewMsgs, args)
		n.mu.Unlock()
		*reply = true
		return nil
	}
	if args.Ballot.Number == n.lastProcessedView.Number && args.Ballot.NodeID == n.lastProcessedView.NodeID {
		n.mu.Unlock()
		*reply = true
		return nil
	}
	n.lastProcessedView = args.Ballot
	n.lastLeaderMsg = time.Now()
	n.HighestPromised = args.Ballot
	n.CurrentBallot = args.Ballot
	n.IsLeader = args.Ballot.NodeID == n.ID
	n.NewViewMsgs = append(n.NewViewMsgs, args)

	prevAcceptedCopy := make(map[int]datatypes.LogEntry, len(n.AcceptedLog))
	for seq, e := range n.AcceptedLog {
		prevAcceptedCopy[seq] = e
	}
	n.mu.Unlock()

	newAcceptedLog := make(map[int]datatypes.LogEntry, len(args.AcceptLog))
	newRequestLog := make([]datatypes.LogEntry, 0, len(args.AcceptLog))
	maxSeq := 0
	for _, entry := range args.AcceptLog {
		if entry.SeqNum > maxSeq {
			maxSeq = entry.SeqNum
		}
		status := entry.Status
		if prev, ok := prevAcceptedCopy[entry.SeqNum]; ok {
			status = maxStatus(status, prev.Status)
		}
		logEntry := datatypes.LogEntry{Ballot: entry.AcceptNum, SeqNum: entry.SeqNum, Request: entry.Request, Status: status}
		newAcceptedLog[entry.SeqNum] = logEntry
		newRequestLog = append(newRequestLog, logEntry)
	}
	sort.Slice(newRequestLog, func(i, j int) bool { return newRequestLog[i].SeqNum < newRequestLog[j].SeqNum })

	n.mu.Lock()
	n.AcceptedLog = newAcceptedLog
	n.RequestLog = newRequestLog
	n.pendingAccepts = make(map[int]map[int]datatypes.AcceptedMsg)
	if maxSeq == 0 {
		n.NextSeqNum = 1
	} else {
		n.NextSeqNum = maxSeq + 1
	}
	n.mu.Unlock()

	n.applyCommittedEntries(prevAcceptedCopy, maxSeq)
	if len(args.AcceptLog) > 0 && args.Ballot.NodeID != n.ID {
		for _, entry := range args.AcceptLog {
			go func(e datatypes.AcceptLogEntry) {
				var acceptedReply datatypes.AcceptedMsg
				acceptedMsg := datatypes.AcceptedMsg{Ballot: args.Ballot, SeqNum: e.SeqNum, Request: e.Request, NodeID: n.ID}
				_ = n.callRPC(args.Ballot.NodeID, "AcceptedFromNewView", acceptedMsg, &acceptedReply)
			}(entry)
		}
	}
	log.Printf("Triggering executor from NewView on Node %d inside HandleNewView line 1859", n.ID)
	n.triggerExecutor()
	*reply = true
	return nil
}

func (n *Node) executeRequest(seqNum int, request datatypes.ClientRequest) (bool, string) {
	log.Printf("Kind of circular Node %d: executeRequest seq=%d (%s→%s,%d) 2PCPhase=%d IsCross=%v IsNoOp=%v", n.ID, seqNum, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount, request.TwoPCPhase, request.IsCross, request.IsNoOp)
	if request.IsNoOp {

		n.markExecuted(seqNum)
		return true, "no-op executed"
	}

	if request.IsCross && request.TwoPCPhase != datatypes.TwoPCPhaseNone {

		n.mu.RLock()
		st, ok := n.TxnStates[request.TxnID]
		n.mu.RUnlock()
		if !ok {

			sID, errS := strconv.Atoi(request.Transaction.Sender)
			rID, errR := strconv.Atoi(request.Transaction.Receiver)
			if errS != nil || errR != nil {
				log.Printf("Node %d [2PC]: bad account ids in request for txn=%s", n.ID, request.TxnID)
				return false, "bad-ids"
			}
			sCID := shard.ClusterOfItem(sID)
			rCID := shard.ClusterOfItem(rID)
			role := TxnRoleNone
			if n.ClusterID == sCID {
				role = TxnRoleCoord
			} else if n.ClusterID == rCID {
				role = TxnRolePart
			}
			inferred := &TxnState{
				TxnID:     request.TxnID,
				Role:      role,
				S:         sID,
				R:         rID,
				Amount:    request.Transaction.Amount,
				SourceCID: sCID,
				DestCID:   rCID,
			}

			n.mu.Lock()

			if existing, ok2 := n.TxnStates[request.TxnID]; ok2 {
				st = existing
			} else {
				n.TxnStates[request.TxnID] = inferred
				st = inferred
			}
			n.mu.Unlock()
		}
		if st.Role == TxnRoleCoord {
			return n.execute2PCCoordinator(seqNum, request, st)
		}
		if st.Role == TxnRolePart {
			return n.execute2PCParticipant(seqNum, request, st)
		}
		return false, "unknown-txn-role"
	}

	success, message := n.Database.ExecuteTransaction(request.Transaction)
	n.markExecuted(seqNum)

	// if success {
	// 	log.Printf("Node %d: EXECUTED seq=%d SUCCESS (%s→%s,%d)",n.ID, seqNum,request.Transaction.Sender,request.Transaction.Receiver,request.Transaction.Amount)
	// } else {
	// 	log.Printf("Node %d: EXECUTION FAILED seq=%d (%s→%s,%d) — %s",n.ID, seqNum, request.Transaction.Sender, request.Transaction.Receiver, request.Transaction.Amount, message)
	// }

	return success, message
}

func (n *Node) executeRequestsInOrder() {
	log.Printf("Node %d: executeRequestsInOrder triggered", n.ID)
	didSync := false
	for seqNum := 1; ; seqNum++ {

		n.mu.RLock()
		entry, exists := n.AcceptedLog[seqNum]
		n.mu.RUnlock()
		if !exists {

			if !didSync {
				log.Printf("Node %d: gap at seq=%d — requesting state transfer from leader", n.ID, seqNum)
				if n.tryStateTransferFromLeader() {
					didSync = true

					seqNum = 0
					continue
				}
			}
			log.Printf("Node %d: executeRequestsInOrder stopping at gap seq=%d (syncAttempted=%v)", n.ID, seqNum, didSync)
			break
		}

		if entry.Status == datatypes.StatusExecuted {
			continue
		}

		if entry.Status == datatypes.StatusCommitted {
			log.Printf("Node %d: EXECUTING seq=%d (%s→%s,%d)", n.ID, seqNum, entry.Request.Transaction.Sender, entry.Request.Transaction.Receiver, entry.Request.Transaction.Amount)
			n.executeRequest(seqNum, entry.Request)
		}
	}
}

func (n *Node) tryStateTransferFromLeader() bool {
	leaderID, err := n.findClusterLeader(n.ClusterID)
	if err != nil || leaderID == 0 {
		return false
	}

	if leaderID == n.ID {
		return false
	}
	args := datatypes.StateTransferArgs{RequesterID: n.ID}
	var rep datatypes.StateTransferReply
	if err := n.callRPC(leaderID, "RequestStateTransfer", args, &rep); err != nil || !rep.Success {
		return false
	}
	var applied bool

	if err := n.HandleNewView(rep.Snapshot, &applied); err != nil {
		return false
	}
	return applied
}

func (n *Node) triggerExecutor() {
	n.mu.Lock()
	if n.execRunning {
		n.mu.Unlock()
		return
	}
	n.execRunning = true
	n.mu.Unlock()
	go func() {
		defer func() {
			n.mu.Lock()
			n.execRunning = false
			n.mu.Unlock()
		}()
		n.executeRequestsInOrder()
	}()
}

func (n *Node) markExecuted(seq int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if entry, ok := n.AcceptedLog[seq]; ok {
		if entry.Status != datatypes.StatusExecuted {
			entry.Status = datatypes.StatusExecuted
			n.AcceptedLog[seq] = entry
		}
	}
	for i := range n.RequestLog {
		if n.RequestLog[i].SeqNum == seq {
			if n.RequestLog[i].Status != datatypes.StatusExecuted {
				n.RequestLog[i].Status = datatypes.StatusExecuted
			}
			break
		}
	}
}

func (n *Node) execute2PCCoordinator(seqNum int, req datatypes.ClientRequest, st *TxnState) (bool, string) {
	log.Printf("the phase value is %v", req.TwoPCPhase)
	switch req.TwoPCPhase {
	case datatypes.TwoPCPhasePrepare:

		if st.Phase == TxnPhasePrepared || st.Phase == TxnPhaseCommitted {
			n.markExecuted(seqNum)
			return true, "coord-prepared"
		}

		sID, amt := st.S, st.Amount
		if err := n.Database.DebitWithWALMeta(req.TxnID, sID, amt, st.DestCID, st.R); err != nil {
			st.Phase = TxnPhaseAborted
			return false, "coord-debit-failed"
		}
		st.Phase = TxnPhasePrepared
		n.markExecuted(seqNum)
		return true, "coord-prepared"
	case datatypes.TwoPCPhaseCommit:

		_ = n.Database.PromoteWALPrepareToCommit(req.TxnID)
		_ = n.Database.ApplyWALCommit(req.TxnID)

		st.Phase = TxnPhaseCommitted

		n.markExecuted(seqNum)
		return true, "coord-committed"
	case datatypes.TwoPCPhaseAbort:
		_ = n.Database.UndoWAL(req.TxnID)

		st.Phase = TxnPhaseAborted

		n.markExecuted(seqNum)
		return true, "coord-aborted"
	default:
		return false, "coord-unknown-phase"
	}
}

func (n *Node) execute2PCParticipant(seqNum int, req datatypes.ClientRequest, st *TxnState) (bool, string) {
	switch req.TwoPCPhase {
	case datatypes.TwoPCPhasePrepare:
		if st.Phase == TxnPhasePrepared || st.Phase == TxnPhaseCommitted {
			n.markExecuted(seqNum)
			return true, "part-prepared"
		}
		if err := n.Database.CreditWithWAL(req.TxnID, st.R, st.Amount); err != nil {
			st.Phase = TxnPhaseAborted
			if st.LockHeldOnR {
				n.unlockLocked(req.TxnID, st.R)
				st.LockHeldOnR = false
			}
			return false, "part-credit-failed"
		}
		st.Phase = TxnPhasePrepared
		n.markExecuted(seqNum)
		return true, "part-prepared"
	case datatypes.TwoPCPhaseCommit:
		_ = n.Database.PromoteWALPrepareToCommit(req.TxnID)
		_ = n.Database.ApplyWALCommit(req.TxnID)
		_ = n.Database.ClearWAL(req.TxnID)
		if st.LockHeldOnR {
			n.unlockLocked(req.TxnID, st.R)
			st.LockHeldOnR = false
		}
		st.Phase = TxnPhaseCommitted
		n.markExecuted(seqNum)
		return true, "part-committed"
	case datatypes.TwoPCPhaseAbort:
		_ = n.Database.UndoWAL(req.TxnID)
		_ = n.Database.ClearWAL(req.TxnID)
		if st.LockHeldOnR {
			n.unlockLocked(req.TxnID, st.R)
			st.LockHeldOnR = false
		}
		st.Phase = TxnPhaseAborted
		n.markExecuted(seqNum)
		return true, "part-aborted"
	default:
		return false, "part-unknown-phase"
	}
}

func (n *Node) applyCommittedEntries(prevAccepted map[int]datatypes.LogEntry, maxSeq int) {
	log.Printf("Node %d: applyCommittedEntries up to seq=%d", n.ID, maxSeq)
	for seqNum := 1; seqNum <= maxSeq; seqNum++ {

		n.mu.RLock()
		entry, exists := n.AcceptedLog[seqNum]
		n.mu.RUnlock()
		if !exists {
			continue
		}

		if entry.Status == datatypes.StatusCommitted || entry.Status == datatypes.StatusExecuted {
			if prevEntry, ok := prevAccepted[seqNum]; ok && prevEntry.Status == datatypes.StatusExecuted {
				if entry.Status == datatypes.StatusCommitted {

					n.mu.Lock()
					cur := n.AcceptedLog[seqNum]
					if cur.Status == datatypes.StatusCommitted {
						cur.Status = datatypes.StatusExecuted
						n.AcceptedLog[seqNum] = cur
						for i := range n.RequestLog {
							if n.RequestLog[i].SeqNum == seqNum {
								n.RequestLog[i].Status = datatypes.StatusExecuted
								break
							}
						}
					}
					n.mu.Unlock()
				}
				continue
			}

			n.executeRequest(seqNum, entry.Request)
		}
	}
}

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
	for _, nodeID := range n.clusterPeerIDs() {
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

	n.mu.Lock()
	requiredMajority := n.clusterMajorityLocked()
	n.mu.Unlock()
	if promiseCount >= requiredMajority {
		n.mu.Lock()
		if !n.ActiveNodes[n.ID] {
			n.mu.Unlock()
			return false
		}

		if n.HighestPromised.GreaterThan(ballot) || (n.IsLeader && n.CurrentBallot.GreaterThanOrEqual(ballot)) {
			log.Printf("Node %d: ignoring stale election completion (attempt=%s highestPromised=%s current=%s)",
				n.ID, ballot.String(), n.HighestPromised.String(), n.CurrentBallot.String())
			n.mu.Unlock()
			return false
		}
		n.IsLeader = true
		n.acceptedFromNewViewCount = 0
		n.ackFromNewView = make(map[int]bool)

		n.clearAllLocksLocked("became leader with new ballot")

		n.CurrentBallot = ballot

		n.lastLeaderMsg = time.Now()
		n.electionCoolDown = time.Now()
		go n.sendHeartbeats()
		go n.runDecisionRetryLoop()
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
			majority := n.clusterMajorityLocked()
			n.mu.RUnlock()
			if count >= majority {
				//log.Printf("Node %d: NewView accepted by majority (%d/%d), safe to proceed", n.ID, count, majority)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		n.mu.RLock()
		if n.acceptedFromNewViewCount < n.clusterMajorityLocked() {
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

func (n *Node) ForceLeader() bool {

	n.mu.RLock()
	if !n.ActiveNodes[n.ID] {
		n.mu.RUnlock()
		return false
	}

	startNum := n.CurrentBallot.Number
	if n.HighestPromised.Number > startNum {
		startNum = n.HighestPromised.Number
	}
	ballot := datatypes.BallotNumber{Number: startNum + 1, NodeID: n.ID}
	n.mu.RUnlock()

	for attempt := 0; attempt < 3; attempt++ {

		promises := make(map[int]datatypes.PromiseMsg)
		var mu sync.Mutex
		var wg sync.WaitGroup
		highestNack := ballot

		for _, pid := range n.clusterPeerIDs() {
			if pid == n.ID {
				continue
			}
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				req := datatypes.PrepareMsg{Ballot: ballot}
				var rep datatypes.PromiseMsg
				if err := n.callRPC(id, "Prepare", req, &rep); err == nil {
					mu.Lock()
					if rep.Success {
						promises[id] = rep
					} else {

						if rep.Ballot.Number > highestNack.Number || (rep.Ballot.Number == highestNack.Number && rep.Ballot.NodeID > highestNack.NodeID) {
							highestNack = rep.Ballot
						}
					}
					mu.Unlock()
				}
			}(pid)
		}

		n.mu.RLock()
		selfLog := make([]datatypes.AcceptLogEntry, 0)
		for seqNum, entry := range n.AcceptedLog {
			selfLog = append(selfLog, datatypes.AcceptLogEntry{AcceptNum: entry.Ballot, SeqNum: seqNum, Request: entry.Request, Status: entry.Status})
		}
		n.mu.RUnlock()
		mu.Lock()
		promises[n.ID] = datatypes.PromiseMsg{Ballot: ballot, AcceptLog: selfLog, Success: true}
		mu.Unlock()

		wg.Wait()

		n.mu.RLock()
		maj := n.clusterMajorityLocked()
		n.mu.RUnlock()
		if len(promises) >= maj {

			n.mu.Lock()
			if !n.ActiveNodes[n.ID] {
				n.mu.Unlock()
				return false
			}

			if n.HighestPromised.GreaterThan(ballot) || (n.IsLeader && n.CurrentBallot.GreaterThanOrEqual(ballot)) {
				log.Printf("Node %d: ignoring stale force-leader completion (attempt=%s highestPromised=%s current=%s)",
					n.ID, ballot.String(), n.HighestPromised.String(), n.CurrentBallot.String())
				n.mu.Unlock()
				return false
			}
			n.IsLeader = true
			n.clearAllLocksLocked("force leader with new ballot")
			n.CurrentBallot = ballot

			n.lastLeaderMsg = time.Now()
			n.electionCoolDown = time.Now()
			acceptLog := n.createNewViewFromPromises(promises)
			newViewMsg := datatypes.NewViewMsg{Ballot: ballot, AcceptLog: acceptLog}
			n.NewViewMsgs = append(n.NewViewMsgs, newViewMsg)
			n.mu.Unlock()

			n.sendNewViewMessages(newViewMsg)
			go n.sendHeartbeats()
			go n.runDecisionRetryLoop()
			return true
		}

		if highestNack.Number > ballot.Number || (highestNack.Number == ballot.Number && highestNack.NodeID > ballot.NodeID) {
			ballot.Number = highestNack.Number + 1
			ballot.NodeID = n.ID
			continue
		}

		return false
	}
	return false
}

func (n *Node) recoverEntryWithNewBallot(entry datatypes.AcceptLogEntry, ballot datatypes.BallotNumber) {

	n.mu.Lock()
	current := n.pendingAccepts[entry.SeqNum]
	acceptCount := 0
	if current != nil {
		acceptCount = len(current)
	}
	n.mu.Unlock()

	if acceptCount < n.clusterMajorityLocked() {

		acceptMsg := datatypes.AcceptMsg{
			Type:    "ACCEPT",
			Ballot:  ballot,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
		}

		var wg sync.WaitGroup
		for _, peerID := range n.clusterPeerIDs() {
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

	n.mu.Lock()
	acceptCount = 0
	if n.pendingAccepts[entry.SeqNum] != nil {
		acceptCount = len(n.pendingAccepts[entry.SeqNum])
	}
	n.mu.Unlock()

	if acceptCount >= n.clusterMajorityLocked() {
		commitMsg := datatypes.CommitMsg{
			Ballot:  ballot,
			SeqNum:  entry.SeqNum,
			Request: entry.Request,
		}
		for _, peerID := range n.clusterPeerIDs() {
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

		n.triggerExecutor()
	}
}

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

		if seq <= maxSeq {
			noOpRequest := datatypes.ClientRequest{
				ClientID:  fmt.Sprintf("no-op-%d", seq),
				Timestamp: time.Now().UnixNano(),
				IsNoOp:    true,
				Transaction: datatypes.Txn{
					Sender:   "no-op",
					Receiver: "no-op",
					Amount:   0,
				},
				TxnID:      "",
				TwoPCPhase: datatypes.TwoPCPhaseNone,
				IsCross:    false,
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

func (n *Node) sendNewViewMessages(msg datatypes.NewViewMsg) {
	log.Printf("Node %d: broadcasting NewView ballot=%s entries=%d", n.ID, msg.Ballot.String(), len(msg.AcceptLog))
	for _, nodeID := range n.clusterPeerIDs() {
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

	for _, entry := range acceptLog {
		log.Printf("Node %d: snapshot entry seq=%d ballot=%s request=%s status=%v",
			n.ID, entry.SeqNum, entry.AcceptNum.String(), entry.Request.String(), entry.Status)
	}
	return snapshot
}

func (n *Node) runDecisionRetryLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !n.GetIsLeader() {
			return
		}
		<-ticker.C

		n.mu.RLock()
		pending := make([]*PendingDecision, 0, len(n.Pending2PCAcks))
		for _, pd := range n.Pending2PCAcks {
			pending = append(pending, &PendingDecision{TxnID: pd.TxnID, Decision: pd.Decision, DestCID: pd.DestCID})
		}
		n.mu.RUnlock()
		for _, pd := range pending {
			var reply datatypes.TwoPCDecisionReply
			args := datatypes.TwoPCDecisionArgs{TxnID: pd.TxnID, Decision: datatypes.TwoPCDecision(pd.Decision)}
			_ = n.call2PCDecision(pd.DestCID, args, &reply)
			if reply.Acked {

				go n.Database.ClearWAL(pd.TxnID)
				n.mu.Lock()
				delete(n.Pending2PCAcks, pd.TxnID)
				n.mu.Unlock()
			} else {
				log.Printf("[RECOVERY][RETRY] txn=%s destCID=%d decision=%s no-ack-yet", pd.TxnID, pd.DestCID, pd.Decision)
			}
		}
	}
}

func (n *Node) RecoverFromCrash() {

	n.mu.Lock()
	n.clearAllLocksLocked("hard recovery")
	n.TxnStates = make(map[string]*TxnState)
	n.pendingAccepts = make(map[int]map[int]datatypes.AcceptedMsg)
	n.mu.Unlock()

	recs, err := n.Database.LoadWAL()
	if err != nil {
		log.Printf("Node %d: RecoverFromCrash LoadWAL error: %v", n.ID, err)
		return
	}
	type group struct {
		prep      *datatypes.WALRecord
		hasCommit bool
		hasAbort  bool
	}
	groups := make(map[string]*group)
	for i := range recs {
		r := &recs[i]
		g := groups[r.TxnID]
		if g == nil {
			g = &group{}
			groups[r.TxnID] = g
		}
		switch r.Phase {
		case datatypes.WALPrepare:
			g.prep = r
		case datatypes.WALCommit:
			g.hasCommit = true
		case datatypes.WALAbort:
			g.hasAbort = true
		}
	}

	for txnID, g := range groups {

		if g.hasCommit || g.hasAbort {

			if g.prep != nil && g.prep.DestCID > 0 {

				decision := datatypes.TwoPCDecisionAbort
				if g.hasCommit {
					decision = datatypes.TwoPCDecisionCommit
				}

				sID := 0
				amount := 0
				if g.prep != nil && len(g.prep.Items) > 0 {
					sID = g.prep.Items[0].ID
					amount = g.prep.Items[0].OldBalance - g.prep.Items[0].NewBalance
					if amount < 0 {
						amount = -amount
					}
				}
				st := &TxnState{TxnID: txnID, Role: TxnRoleCoord, S: sID, R: g.prep.R, Amount: amount, SourceCID: shard.ClusterOfItem(sID), DestCID: g.prep.DestCID}
				if g.hasCommit {
					st.Phase = TxnPhaseCommitted
				} else {
					st.Phase = TxnPhaseAborted
				}
				n.mu.Lock()
				n.TxnStates[txnID] = st
				n.Pending2PCAcks[txnID] = &PendingDecision{TxnID: txnID, Decision: decision, DestCID: g.prep.DestCID}
				n.mu.Unlock()
				log.Printf("[RECOVERY] txn=%s role=coord action=pending-%s destCID=%d", txnID, decision, g.prep.DestCID)
			} else {

				if g.hasCommit {
					_ = n.Database.ApplyWALCommit(txnID)
					log.Printf("[RECOVERY] txn=%s role=part action=apply-commit+clear", txnID)
				} else if g.hasAbort {
					_ = n.Database.UndoWAL(txnID)
					log.Printf("[RECOVERY] txn=%s role=part action=apply-abort+clear", txnID)
				}
				_ = n.Database.ClearWAL(txnID)

				rID := 0
				amount := 0
				if g.prep != nil && len(g.prep.Items) > 0 {
					rID = g.prep.Items[0].ID
					amount = g.prep.Items[0].NewBalance - g.prep.Items[0].OldBalance
					if amount < 0 {
						amount = -amount
					}
				}
				st := &TxnState{TxnID: txnID, Role: TxnRolePart, R: rID, Amount: amount, DestCID: shard.ClusterOfItem(rID)}
				if g.hasCommit {
					st.Phase = TxnPhaseCommitted
				} else {
					st.Phase = TxnPhaseAborted
				}
				n.mu.Lock()
				n.TxnStates[txnID] = st
				n.mu.Unlock()
			}
			continue
		}

		if g.prep == nil {
			continue
		}
		prep := g.prep

		if prep.DestCID > 0 {

			_ = n.Database.UndoWAL(txnID)
			if n.GetIsLeader() {

				sID := 0
				rID := prep.R
				amount := 0
				if len(prep.Items) > 0 {
					sID = prep.Items[0].ID
					amount = prep.Items[0].OldBalance - prep.Items[0].NewBalance
					if amount < 0 {
						amount = -amount
					}
				}

				st := &TxnState{TxnID: txnID, Role: TxnRoleCoord, S: sID, R: rID, Amount: amount, SourceCID: shard.ClusterOfItem(sID), DestCID: prep.DestCID, Phase: TxnPhasePrepared}
				n.mu.Lock()
				n.TxnStates[txnID] = st
				n.mu.Unlock()
				decReq := datatypes.ClientRequest{
					MessageType: "BANK_TXN",
					ClientID:    "recovery",
					Timestamp:   time.Now().UnixNano(),
					Transaction: datatypes.Txn{Sender: strconv.Itoa(sID), Receiver: strconv.Itoa(rID), Amount: amount},
					TxnID:       txnID,
					TwoPCPhase:  datatypes.TwoPCPhaseAbort,
					IsCross:     true,
				}
				_, _ = n.proposeAndWait(decReq)

				var rep datatypes.TwoPCDecisionReply
				_ = n.call2PCDecision(prep.DestCID, datatypes.TwoPCDecisionArgs{TxnID: txnID, Decision: datatypes.TwoPCDecisionAbort}, &rep)
				if rep.Acked {
					_ = n.Database.ClearWAL(txnID)
					n.mu.Lock()
					st.Phase = TxnPhaseAborted
					n.TxnStates[txnID] = st
					n.mu.Unlock()
					log.Printf("[RECOVERY] txn=%s role=coord action=presumed-abort+acked destCID=%d", txnID, prep.DestCID)
				} else {
					n.mu.Lock()
					n.Pending2PCAcks[txnID] = &PendingDecision{TxnID: txnID, Decision: datatypes.TwoPCDecisionAbort, DestCID: prep.DestCID}
					n.mu.Unlock()
					log.Printf("[RECOVERY] txn=%s role=coord action=presumed-abort+pended destCID=%d", txnID, prep.DestCID)
				}
			}
		} else {

			if len(prep.Items) > 0 {
				rID := prep.Items[0].ID
				n.mu.Lock()
				_ = n.tryLockLocked(txnID, rID)
				n.mu.Unlock()

				amount := prep.Items[0].NewBalance - prep.Items[0].OldBalance
				if amount < 0 {
					amount = -amount
				}
				st := &TxnState{TxnID: txnID, Role: TxnRolePart, R: rID, Amount: amount, DestCID: shard.ClusterOfItem(rID), Phase: TxnPhasePrepared}
				n.mu.Lock()
				n.TxnStates[txnID] = st
				n.mu.Unlock()
				log.Printf("[RECOVERY] txn=%s role=part action=prepared+relock r=%d", txnID, rID)
			}
		}
	}
	log.Printf("[RECOVERY] Node %d: recovery complete (txns=%d)", n.ID, len(groups))
}

func (n *Node) sendStateSnapshot(targetID int) {
	if targetID == n.ID {
		return
	}

	n.mu.RLock()
	empty := len(n.AcceptedLog) == 0
	n.mu.RUnlock()
	if empty {
		log.Printf("Node %d: skip sending snapshot to %d (no entries)", n.ID, targetID)
		return
	}

	snapshot := n.buildStateSnapshot()
	log.Printf("Node %d: sending snapshot entries=%d to node %d", n.ID, len(snapshot.AcceptLog), targetID)
	var reply bool
	if err := n.callRPC(targetID, "NewView", snapshot, &reply); err != nil {
		log.Printf("Node %d: state snapshot to %d failed: %v", n.ID, targetID, err)
	}
}

func (n *Node) sendCommitReplay(targetID int) {
	if targetID == n.ID {
		return
	}

	inCluster := false
	for _, pid := range n.clusterPeerIDs() {
		if pid == targetID {
			inCluster = true
			break
		}
	}
	if !inCluster {
		return
	}

	n.mu.RLock()
	if !n.ActiveNodes[n.ID] || !n.IsLeader {
		n.mu.RUnlock()
		return
	}
	seqs := make([]int, 0, len(n.AcceptedLog))
	for seq := range n.AcceptedLog {
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	ballot := n.CurrentBallot

	entries := make([]datatypes.LogEntry, 0, len(seqs))
	for _, s := range seqs {
		e := n.AcceptedLog[s]
		if e.Status == datatypes.StatusCommitted || e.Status == datatypes.StatusExecuted {
			entries = append(entries, e)
		}
	}
	n.mu.RUnlock()

	for _, e := range entries {
		n.mu.RLock()
		active := n.ActiveNodes[targetID]
		n.mu.RUnlock()
		if !active {
			return
		}
		commitMsg := datatypes.CommitMsg{Ballot: ballot, SeqNum: e.SeqNum, Request: e.Request}
		var applied bool
		_ = n.callRPC(targetID, "Commit", commitMsg, &applied)
	}
}

func (n *Node) requestStateSync() {
	log.Printf("Node %d: initiating state sync attempts", n.ID)
	for attempt := 0; attempt < 5; attempt++ {

		n.mu.RLock()
		isLeader := n.IsLeader && n.ActiveNodes[n.ID]
		clusterPeers := n.clusterPeerIDs()
		active := make([]int, 0, len(clusterPeers))
		for _, pid := range clusterPeers {
			if n.ActiveNodes[pid] {
				active = append(active, pid)
			}
		}
		n.mu.RUnlock()

		if isLeader {
			log.Printf("Node %d: aborting state sync because self is leader", n.ID)
			return
		}

		leaderID := 0
		for _, pid := range active {
			var li datatypes.LeaderInfo
			if err := n.callRPC(pid, "GetLeader", true, &li); err == nil {
				if li.IsLeader && li.LeaderID != 0 {
					leaderID = li.LeaderID
					break
				}
			}
		}

		candidates := make([]int, 0, len(active))
		if leaderID != 0 {
			candidates = append(candidates, leaderID)
		}
		for _, pid := range active {
			if pid != leaderID {
				candidates = append(candidates, pid)
			}
		}

		synced := false
		for _, pid := range candidates {
			args := datatypes.StateTransferArgs{RequesterID: n.ID}
			var reply datatypes.StateTransferReply
			if err := n.callRPC(pid, "RequestStateTransfer", args, &reply); err != nil || !reply.Success {
				continue
			}
			var applied bool
			if err := n.HandleNewView(reply.Snapshot, &applied); err == nil && applied {
				log.Printf("Node %d: state sync completed via node %d", n.ID, pid)
				synced = true
				break
			}
		}
		if synced {
			return
		}

		log.Printf("Node %d: state sync attempt %d had no usable leader/peers; retrying", n.ID, attempt+1)
		time.Sleep(300 * time.Millisecond)
	}

	//log.Printf("Node %d: failed to synchronize state after retries", n.ID)
	log.Printf("Node %d: failed to synchronize state after retries", n.ID)
}

func (s *NodeService) PrintLog(_ bool, reply *string) error {
	log.Printf("Node %d: PrintLog RPC invoked", s.node.ID)
	s.node.mu.RLock()
	defer s.node.mu.RUnlock()

	var b strings.Builder
	b.WriteString(fmt.Sprintf("===== Node %d Log =====\n", s.node.ID))
	for _, e := range s.node.RequestLog {

		seq := e.SeqNum
		bn, bi := e.Ballot.Number, e.Ballot.NodeID
		sID, rID, amt := e.Request.Transaction.Sender, e.Request.Transaction.Receiver, e.Request.Transaction.Amount
		stat := e.Status

		phase := string(e.Request.TwoPCPhase)
		isCross := e.Request.IsCross
		tid := e.Request.TxnID
		isNoOp := e.Request.IsNoOp

		sInt, _ := strconv.Atoi(sID)
		rInt, _ := strconv.Atoi(rID)
		sCID := shard.ClusterOfItem(sInt)
		rCID := shard.ClusterOfItem(rInt)

		if isNoOp {
			fmt.Fprintf(&b, "Seq %d | Ballot (%d,%d) | NO-OP | Status %s\n", seq, bn, bi, stat)
			continue
		}

		if tid != "" || phase != "" || isCross {
			fmt.Fprintf(&b,
				"Seq %d | Ballot (%d,%d) | Txn %s -> %s amt=%d | Status %s | 2PC{txn=%s phase=%s cross=%v} | Clusters %d->%d\n",
				seq, bn, bi, sID, rID, amt, stat, tid, phase, isCross, sCID, rCID)
		} else {
			fmt.Fprintf(&b,
				"Seq %d | Ballot (%d,%d) | Txn %s -> %s amt=%d | Status %s | Clusters %d->%d\n",
				seq, bn, bi, sID, rID, amt, stat, sCID, rCID)
		}
	}
	*reply = b.String()
	return nil
}

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

func (ns *NodeService) PrintDB(args datatypes.PrintDBArgs, reply *datatypes.PrintDBReply) error {
	//log.Printf("Node %d: Received RPC request to print DB.\n", ns.node.ID)
	log.Printf("Node %d: PrintDB RPC invoked", ns.node.ID)

	dbContents := ns.node.Database.PrintDB(ns.node.ID)

	reply.DBContents = dbContents

	return nil
}

func (ns *NodeService) GetBalance(args datatypes.GetBalanceArgs, reply *datatypes.GetBalanceReply) error {
	log.Printf("Node %d: GetBalance RPC account=%s", ns.node.ID, args.AccountID)
	reply.Balance = ns.node.Database.GetBalance(args.AccountID)
	return nil
}

func (ns *NodeService) AdminGetBalance(args datatypes.AdminGetBalanceArgs, reply *datatypes.AdminGetBalanceReply) error {
	reply.Balance = ns.node.Database.GetBalance(args.AccountID)
	reply.Ok = true
	return nil
}

func (ns *NodeService) AdminSetBalance(args datatypes.AdminSetBalanceArgs, reply *datatypes.AdminSetBalanceReply) error {
	if err := ns.node.Database.SetBalance(args.AccountID, args.Balance); err != nil {
		reply.Ok = false
		return nil
	}
	reply.Ok = true
	return nil
}

func (ns *NodeService) AdminDeleteAccount(args datatypes.AdminDeleteAccountArgs, reply *datatypes.AdminDeleteAccountReply) error {
	if err := ns.node.Database.DeleteAccount(args.AccountID); err != nil {
		reply.Ok = false
		return nil
	}
	reply.Ok = true
	return nil
}

func (ns *NodeService) AdminReloadOverrides(_ datatypes.AdminReloadOverridesArgs, reply *datatypes.AdminReloadOverridesReply) error {
	if err := shard.LoadOverridesFromFile(); err != nil {
		log.Printf("Node %d: AdminReloadOverrides load error: %v", ns.node.ID, err)

		reply.Ok = false
		return nil
	}
	reply.Ok = true
	return nil
}

func (s *NodeService) TriggerElection(_ datatypes.TriggerElectionArgs, reply *datatypes.TriggerElectionReply) error {
	s.node.mu.RLock()
	active := s.node.ActiveNodes[s.node.ID]
	s.node.mu.RUnlock()
	started := false
	if active {
		started = s.node.StartLeaderElection()
	}
	*reply = datatypes.TriggerElectionReply{Started: started}
	return nil
}

func (s *NodeService) ForceLeader(_ datatypes.TriggerElectionArgs, reply *datatypes.TriggerElectionReply) error {
	s.node.mu.RLock()
	selfActive := s.node.ActiveNodes[s.node.ID]
	s.node.mu.RUnlock()
	ok := false
	if selfActive {
		ok = s.node.ForceLeader()
	}
	*reply = datatypes.TriggerElectionReply{Started: ok}
	return nil
}

func (s *NodeService) GetBalancesFor(args datatypes.GetBalancesForArgs, reply *datatypes.GetBalancesForReply) error {
	out := make(map[int]int)
	for _, id := range args.AccountIDs {
		out[id] = s.node.Database.GetBalanceInt(id)
	}
	reply.Balances = out
	return nil
}

func (s *NodeService) FlushState(args datatypes.FlushStateArgs, reply *datatypes.FlushStateReply) error {
	s.node.mu.Lock()

	s.node.NewViewMsgs = nil

	if args.ResetConsensus {

		s.node.AcceptedLog = make(map[int]datatypes.LogEntry)
		s.node.RequestLog = nil
		s.node.pendingAccepts = make(map[int]map[int]datatypes.AcceptedMsg)
		s.node.NextSeqNum = 1
		s.node.LastReply = make(map[string]datatypes.ReplyMsg)
		s.node.HighestPromised = datatypes.BallotNumber{Number: 0, NodeID: 0}
		s.node.CurrentBallot = datatypes.BallotNumber{Number: 0, NodeID: 0}
		s.node.IsLeader = false
		s.node.ackFromNewView = make(map[int]bool)
		s.node.acceptedFromNewViewCount = 0
		s.node.Locks = make(map[int]LockInfo)
		s.node.TxnStates = make(map[string]*TxnState)
		s.node.Pending2PCAcks = make(map[string]*PendingDecision)

		s.node.lastLeaderMsg = time.Now()
		s.node.electionCoolDown = time.Now()
	}

	s.node.mu.Unlock()

	if args.ResetWAL {
		_ = s.node.Database.ClearAllWAL()
	}
	if args.ResetDB {
		rng := config.ClusterRanges[s.node.ClusterID]
		_ = s.node.Database.ResetBalances(rng.Min, rng.Max, config.InitialBalance)
	}
	*reply = datatypes.FlushStateReply{Ok: true}
	return nil
}
