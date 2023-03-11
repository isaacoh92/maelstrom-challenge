package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	roleFollower  = 0
	roleCandidate = 1
	roleLeader    = 2
)

var epoch = time.Now().UnixMilli()

type Map struct {
	Data map[int]int
}

func (m *Map) Apply(operation []any) ([]any, error) {
	//operation := op.([]any)
	opType, ok := operation[0].(string)
	if !ok {
		return nil, errors.New("first element in operation should be string")
	}
	opKey, err := Int(operation[1])
	if err != nil {
		return []any{}, err
	}

	switch opType {
	case "r":
		if val, ok := m.Data[opKey]; ok {
			return []any{"r", opKey, val}, nil
		}
		return []any{"r", opKey, 0}, nil
	case "w":
		val, err := Int(operation[2])
		if err != nil {
			return []any{}, err
		}
		m.Data[opKey] = val
		return []any{"w", opKey, val}, nil
	default:
		return []any{}, fmt.Errorf("%s not supported", opType)
	}
}

type Raft struct {
	// components
	stateMachine Map
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
	// Map of nodes to the next index to replicate (initialized to leader last log index + 1)
	nextIndex map[string]int
	// Map of other nodes to the highest log entry known to be replicated on that node (initialized to 0)
	matchIndex map[string]int
	// index of highest log entry known to be committed (initialized to 0)
	commitIndex int
	// index of the highest log entry applied to state machine -- Do we need this? Since our state machine is just a local map
	lastApplied int

	mux  sync.RWMutex
	wait sync.WaitGroup

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
	Leader       string `json:"leader"`
	Entries      []*Log `json:"entries"`
	Term         int    `json:"term"`
	PrevLogIndex int    `json:"prev_log_index"`
	PrevLogTerm  int    `json:"prev_log_term"`
	LeaderCommit int    `json:"leader_commit"`
}

type AppendEntryResponse struct {
	Type               string `json:"type"`
	Source             string `json:"src"`
	Term               int    `json:"term"`
	Ack                bool   `json:"ack"`
	ReplicationSuccess bool   `json:"success"`
}

func InitRaft(node *maelstrom.Node) *Raft {
	r := &Raft{
		mux:                   sync.RWMutex{},
		stateMachine:          Map{Data: map[int]int{}},
		logs:                  InitLogs(),
		node:                  node,
		votedFor:              "",
		leader:                "",
		role:                  roleFollower,
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
	log.Printf("%v: %s", time.Now().UnixMilli()-epoch, fmt.Sprintf(message, params...))
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

func (r *Raft) HandleClientRequest(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	switch {
	case r.IsLeader(true):
		r.Lock(0)
		defer r.Unlock(0)
		r.logs.Append(&Log{
			Term:      r.term,
			Operation: req["txn"].([]any),
		})

		txs := []any{}
		for _, txn := range req["txn"].([]any) {
			res, err := r.stateMachine.Apply(txn.([]any))
			if err != nil {
				return err
			}
			txs = append(txs, res)
		}
		r.commitIndex++

		return r.node.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  txs,
		})
	case r.Leader(true) != "":
		responseChannel := make(chan maelstrom.Message)
		// Async RPC request
		if err := r.node.RPC(r.Leader(true), req, func(m maelstrom.Message) error {
			responseChannel <- m
			return nil
		}); err != nil {
			return err
		}
		ctx, close := context.WithTimeout(context.Background(), time.Millisecond*1000)
		defer close()
		select {
		case res := <-responseChannel:
			var resp map[string]any
			if err := json.Unmarshal(res.Body, &resp); err != nil {
				return err
			}
			return r.node.Reply(msg, resp)
		case <-ctx.Done():
			return errors.New("client request unsuccessful, timed out")
		}
	default:
		return errors.New("temporarily unavailable")
	}
}
