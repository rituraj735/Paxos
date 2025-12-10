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

	TxnID      string
	TwoPCPhase TwoPCPhase
	IsCross    bool
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

func (b BallotNumber) LessThan(other BallotNumber) bool {
	if b.Number != other.Number {
		return b.Number < other.Number
	}
	return b.NodeID < other.NodeID
}

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

type GetBalanceArgs struct {
	AccountID string
}

type GetBalanceReply struct {
	Balance int
}

type GetBalancesForArgs struct {
	AccountIDs []int
}

type GetBalancesForReply struct {
	Balances map[int]int
}

type TriggerElectionArgs struct{}
type TriggerElectionReply struct {
	Started bool
}

type FlushStateArgs struct {
	ResetDB        bool
	ResetConsensus bool
	ResetWAL       bool
}
type FlushStateReply struct {
	Ok bool
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

type WALPhase string

const (
	WALPrepare WALPhase = "P"
	WALCommit  WALPhase = "C"
	WALAbort   WALPhase = "A"
)

type WALItem struct {
	ID         int `json:"id"`
	OldBalance int `json:"oldBalance"`
	NewBalance int `json:"newBalance"`
}

type WALRecord struct {
	TxnID     string    `json:"txnId"`
	Phase     WALPhase  `json:"phase"`
	Items     []WALItem `json:"items"`
	Timestamp int64     `json:"ts"`

	DestCID int `json:"destCid,omitempty"`
	R       int `json:"r,omitempty"`
}

type TwoPCPhase string

const (
	TwoPCPhaseNone    TwoPCPhase = ""
	TwoPCPhasePrepare TwoPCPhase = "PREPARE"
	TwoPCPhaseCommit  TwoPCPhase = "COMMIT"
	TwoPCPhaseAbort   TwoPCPhase = "ABORT"
)

type TwoPCDecision string

const (
	TwoPCDecisionNone   TwoPCDecision = ""
	TwoPCDecisionCommit TwoPCDecision = "COMMIT"
	TwoPCDecisionAbort  TwoPCDecision = "ABORT"
)

type CrossShardTxn struct {
	TxnID    string
	Sender   string
	Receiver string
	Amount   int
	Shards   []int
}

type TwoPCPrepareArgs struct {
	TxnID     string
	S         int
	R         int
	Amount    int
	SourceCID int
	DestCID   int
	ClientID  string
	ClientTS  int64
}

type TwoPCPrepareReply struct {
	TxnID   string
	Success bool
	Message string
}

type TwoPCDecisionArgs struct {
	TxnID    string
	Decision TwoPCDecision
}

type TwoPCDecisionReply struct {
	TxnID   string
	Acked   bool
	Message string
}

type AdminGetBalanceArgs struct{ AccountID string }
type AdminGetBalanceReply struct {
	Balance int
	Ok      bool
}

type AdminSetBalanceArgs struct {
	AccountID string
	Balance   int
}
type AdminSetBalanceReply struct{ Ok bool }

type AdminDeleteAccountArgs struct{ AccountID string }
type AdminDeleteAccountReply struct{ Ok bool }

type AdminReloadOverridesArgs struct{}
type AdminReloadOverridesReply struct{ Ok bool }
