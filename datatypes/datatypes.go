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
}

type RequestStatus string

type PrepareMsg struct {
	Ballot BallotNumber
}

type AcceptMsg struct {
	Ballot  BallotNumber
	SeqNum  int
	Request ClientRequest
}

type CommitMsg struct {
	Ballot  BallotNumber
	SeqNum  int
	Request ClientRequest
}

func (b BallotNumber) GreaterThan(other BallotNumber) bool {
	if b.Number != other.Number {
		return b.Number > other.Number
	}
	return b.NodeID > other.NodeID
}

func (b BallotNumber) GreaterThanOrEqual(other BallotNumber) bool {
	return b.GreaterThan(other) || (b.Number == other.Number && b.NodeID == other.NodeID)
}

func (b BallotNumber) String() string {
	return fmt.Sprintf("(%d,%d)", b.Number, b.NodeID)
}

// Transaction represents a banking transaction
type Txn struct {
	Sender   string `json:"sender"`
	Receiver string `json:"receiver"`
	Amount   int    `json:"amount"`
}

func (t Txn) String() string {
	return fmt.Sprintf("(%s,%s,%d)", t.Sender, t.Receiver, t.Amount)
}

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

// RPC Request/Reply types for client-node communication
