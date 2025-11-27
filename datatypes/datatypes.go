// =======================================
// File: datatypes/datatypes.go
// Description: All RPC payloads, consensus structs, and helper types shared by nodes and clients.
// =======================================
package datatypes

import (
    "fmt"
)

type ClientRequest struct {
	MessageType string
	Transaction Txn
	Timestamp   int64
	ClientID    string
	IsNoOp      bool
}

type ReplyMsg struct {
	Message   string
	Ballot    BallotNumber
	Timestamp int64
	Success   bool
	ClientID  string
	SeqNum    int
}

type BallotNumber struct {
	Number int
	NodeID int
}

type UpdateNodeArgs struct {
	NodeID int
	IsLive bool
}

type LeaderInfo struct {
	LeaderID int
	Ballot   BallotNumber
	IsLeader bool
}

type UpdateClusterStatusArgs struct {
	Active map[int]bool
}

type ClientRequestRPC struct {
	Request ClientRequest
}

type ClientReplyRPC struct {
	Reply ReplyMsg
}

type LogEntry struct {
	Ballot  BallotNumber
	SeqNum  int
	Request ClientRequest
	Status  RequestStatus
}

type NewViewMsg struct {
	Ballot    BallotNumber
	AcceptLog []AcceptLogEntry
}

type AcceptedMsg struct {
	Ballot  BallotNumber
	SeqNum  int
	Request ClientRequest
	NodeID  int
}

type AcceptLogEntry struct {
	AcceptNum BallotNumber
	SeqNum    int
	Request   ClientRequest
	Status    RequestStatus
}

type RequestStatus string

type PrepareMsg struct {
	Ballot BallotNumber
}

type AcceptMsg struct {
	Type    string
	Ballot  BallotNumber
	SeqNum  int
	Request ClientRequest
}

type CommitMsg struct {
	Ballot  BallotNumber
	SeqNum  int
	Request ClientRequest
}

// GreaterThan compares ballots lexicographically by number then node ID.
func (b BallotNumber) GreaterThan(other BallotNumber) bool {
	if b.Number != other.Number {
		return b.Number > other.Number
	}
	return b.NodeID > other.NodeID
}

// GreaterThanOrEqual checks whether the ballot is >= another ballot.
func (b BallotNumber) GreaterThanOrEqual(other BallotNumber) bool {
	return b.GreaterThan(other) || (b.Number == other.Number && b.NodeID == other.NodeID)
}

// String renders the ballot for debug logging.
func (b BallotNumber) String() string {
	return fmt.Sprintf("(%d,%d)", b.Number, b.NodeID)
}

// LessThan reports whether the ballot precedes another ballot.
func (b BallotNumber) LessThan(other BallotNumber) bool {
	if b.Number != other.Number {
		return b.Number < other.Number
	}
	return b.NodeID < other.NodeID
}

// Txn is a tuple to represent (c, c', amt)
type Txn struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Amount   int    `json:"amount"`
}

// String formats a transaction tuple.
func (t Txn) String() string {
	return fmt.Sprintf("(%s,%s,%d)", t.Sender, t.Receiver, t.Amount)
}

// String summarizes the request, showing "no-op" or the txn payload.
func (cr ClientRequest) String() string {
	if cr.IsNoOp {
		return "no-op"
	}
	return cr.Transaction.String()
}

type PromiseMsg struct {
	Ballot    BallotNumber     `json:"ballot"`
	AcceptLog []AcceptLogEntry `json:"acceptLog"`
	Success   bool             `json:"success"`
}

const (
	StatusAccepted  RequestStatus = "A"
	StatusCommitted RequestStatus = "C"
	StatusExecuted  RequestStatus = "E"
	StatusNoStatus  RequestStatus = "X"
)

type PrintDBArgs struct {
	NodeID int
}

type PrintDBReply struct {
	DBContents string
}

type HeartbeatMsg struct {
	Ballot    BallotNumber
	LeaderID  int
	Timestamp int64
}

type StateTransferArgs struct {
	RequesterID int
}

type StateTransferReply struct {
    Snapshot NewViewMsg
    Success  bool
}

// =======================================
// WAL types for 2PC/plumbing (Phase 2)
// Filepath: datatypes/datatypes.go
// Description: Shared write-ahead log (WAL) types used by participants and
// the coordinator to reason about prepare/commit/abort phases and recovery.
// =======================================

// WALPhase encodes the phase of a transaction for WAL persistence.
// P = Prepare, C = Commit, A = Abort
type WALPhase string

const (
    WALPrepare WALPhase = "P"
    WALCommit  WALPhase = "C"
    WALAbort   WALPhase = "A"
)

// WALItem captures the before/after values for a single account touched by a txn.
// ID is the numeric account identifier.
type WALItem struct {
    ID         int `json:"id"`
    OldBalance int `json:"oldBalance"`
    NewBalance int `json:"newBalance"`
}

// WALRecord is the durable record for a transaction phase on a node.
// TxnID is globally unique; Phase is one of P/C/A; Items describe the changes.
// Timestamp is for ordering/debug; recovery logic does not depend on it.
type WALRecord struct {
    TxnID     string    `json:"txnId"`
    Phase     WALPhase  `json:"phase"`
    Items     []WALItem `json:"items"`
    Timestamp int64     `json:"ts"`
}
