package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	ERR_TIMEOUT                 = 0
	ERR_NODE_NOT_FOUND          = 1
	ERR_NOT_SUPPORTED           = 10
	ERR_TEMPORARILY_UNAVAILABLE = 11
	ERR_MALFORMED_REQUEST       = 12
	ERR_CRASH                   = 13
	ERR_ABORT                   = 14
	ERR_KEY_DOES_NOT_EXIST      = 20
	ERR_KEY_ALREADY_EXISTS      = 21
	ERR_PRECONDITION_FAILED     = 22
	ERR_TXN_CONFLICT            = 30

	ROLE_FOLLOWER  = 0
	ROLE_CANDIDATE = 1
	ROLE_LEADER    = 2
)

var epoch = time.Now().UnixMilli()

type StateMachine interface {
	Apply(any) (any, error)
}

type Raft struct {
	mux  sync.RWMutex
	wait sync.WaitGroup

	// components
	stateMachine StateMachine // TODO Map might need its own lock ?
	logs         *Logs
	node         *maelstrom.Node

	// Persistent state
	votedFor string
	leader   string
	role     int
	term     int

	// Tickers for ongoing processes
	checkElectionTicker   time.Duration
	stepDownTicker        time.Duration
	leaderHeartbeatTicker time.Duration
	replicationTicker     time.Duration

	// Timeouts
	stepDownTimeout time.Duration

	// Deadlines
	electionDeadline time.Time
	stepDownDeadline time.Time
	lastReplication  time.Time

	//////////////////////
	//// Leader State ////
	//////////////////////
	// index of highest log entry known to be committed (initialized to 0)
	commitIndex int
	// index of the highest log entry applied to state machine -- Do we need this? Since our state machine is just a local map
	lastApplied int
	// Map of nodes to the next index to replicate (initialized to leader last log index + 1)
	nextIndex map[string]int
	// Map of other nodes to the highest log entry known to be replicated on that node (initialized to 0)
	matchIndex map[string]int

	sufficientAcks  bool
	sufficientVotes bool
	sufficientRepls bool
}

type VoteRequest struct {
	Type         string `json:"type"`
	Candidate    string `json:"candidate"`
	Term         int    `json:"term"`
	LastLogTerm  int    `json:"last_log_term"`
	LastLogIndex int    `json:"last_log_index"`
}

type VoteResponse struct {
	Type        string `json:"type"`
	Term        int    `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntryRequest struct {
	Type         string `json:"type"`
	Term         int    `json:"term"`
	Leader       string `json:"leader"`
	PrevLogIndex int    `json:"prev_log_index"`
	PrevLogTerm  int    `json:"prev_log_term"`
	LeaderCommit int    `json:"leader_commit"`
	Entries      []*Log `json:"entries"`
	//Size         int    `json:"size"` //TODO: REMOVE
}

type AppendEntryResponse struct {
	Type               string `json:"type"`
	Source             string `json:"src"`
	Term               int    `json:"term"`
	Ack                bool   `json:"ack"`
	ReplicationSuccess bool   `json:"success"`
}

func InitRaft(node *maelstrom.Node, state StateMachine) *Raft {
	r := &Raft{
		mux: sync.RWMutex{},
		//stateMachine:          Map{Data: map[int]int{}},
		stateMachine:          state,
		logs:                  InitLogs(),
		node:                  node,
		votedFor:              "",
		leader:                "",
		role:                  ROLE_FOLLOWER,
		term:                  0,
		checkElectionTicker:   10,
		stepDownTicker:        100,
		leaderHeartbeatTicker: 100,
		replicationTicker:     0,
		stepDownTimeout:       2000,
		electionDeadline:      time.Now().Add(generateRandomTimeout(150, 300)),
		stepDownDeadline:      time.Now(),
		lastReplication:       time.Time{},
		commitIndex:           0,
		lastApplied:           0,
		nextIndex:             nil,
		matchIndex:            nil,
		sufficientAcks:        false,
		sufficientVotes:       false,
	}
	go r.ScheduleCandidate()
	go r.ScheduleAppendEntries()
	go r.ScheduleStepDownAsLeader()
	go r.PrintLeader()
	return r
}

// In milliseconds
func generateRandomTimeout(min int, max int) time.Duration {
	rand.Seed(time.Now().UnixNano())

	max = max * 10
	min = min * 10
	return time.Millisecond * time.Duration(rand.Intn(max-min+1)+min)
}

func (r *Raft) Logf(message string, params ...any) {
	log.Println(fmt.Sprintf("%v: %s", time.Now().UnixMilli()-epoch, fmt.Sprintf(message, params...)))
}

func (r *Raft) Lock(i int) {
	//r.Logf("locking %d", i)
	r.mux.Lock()
}

func (r *Raft) Unlock(i int) {
	//r.Logf("unlocking %d", i)
	r.mux.Unlock()
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func Int(m any) (int, error) {
	var res int
	switch v := m.(type) {
	case int:
		res = v
	case float64:
		res = int(v)
	default:
		return res, fmt.Errorf("unsupported type %T", v)
	}
	return res, nil
}
