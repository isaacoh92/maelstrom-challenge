package main

import (
	"context"
	"encoding/json"
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

type Map struct {
	Data map[int]int
}

var epoch = time.Now().UnixMilli()

func (m *Map) Apply(operation []any) ([]any, error) {
	opType := operation[0].(string)
	opKey, err := Int(operation[1])
	if err != nil {
		return []any{}, err
	}

	switch opType {
	case "r":
		if val, ok := m.Data[opKey]; ok {
			return []any{"r", opKey, val}, nil
		} else {
			return []any{"r", opKey, 0}, nil
		}
	case "w":
		val, err := Int(operation[2])
		if err != nil {
			return []any{}, err
		}
		m.Data[opKey] = val
		return []any{"w", opKey, val}, nil
	default:
		return []any{}, maelstrom.NewRPCError(ERR_NOT_SUPPORTED, fmt.Sprintf("%s not supported", opType))
	}
}

// structs
type Raft struct {
	mux      sync.RWMutex
	state    Map // TODO Map might need its own lock ?
	logs     *Logs
	node     *maelstrom.Node
	votedFor string
	leader   string

	role  int
	term  int
	votes StringSet

	// Tickers for ongoing processes
	checkElectionTicker   time.Duration
	stepDownTicker        time.Duration
	leaderHeartbeatTicker time.Duration

	// Timeouts
	stepDownTimeout time.Duration

	// Deadlines
	electionDeadline time.Time
	stepDownDeadline time.Time

	sufficientAcks  bool
	sufficientVotes bool
}

type VoteMessage struct {
	Type         string `json:"type"`
	Candidate    string `json:"candidate,omitempty"`
	Term         int    `json:"term"`
	LastLogTerm  int    `json:"last_log_term"`
	LastLogIndex int    `json:"last_log_index"`
	VoteGranted  bool   `json:"vote_granted,omitempty"`
}

type AckMessage struct {
	Type   string `json:"type"`
	Source string `json:"src,omitempty"`
	Term   int    `json:"term"`
	Ack    bool   `json:"ack"`
}

/*
Request:
Received {c4 n0 {"txn":[["r",9,null],["r",8,null],["w",9,1]],"type":"txn","msg_id":1}}

Response:
Sent {"src":"n0","dest":"c7","body":{"in_reply_to":1,"txn":[["w",9,3]],"type":"txn_ok"}}
*/
func (r *Raft) HandleClientRequest(msg maelstrom.Message) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	r.Lock(0)
	defer r.Unlock(0)
	txs := []any{}
	for _, txn := range req["txn"].([]any) {
		res, err := r.state.Apply(txn.([]any))
		if err != nil {
			return err
		}
		txs = append(txs, res)
	}

	return r.node.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  txs,
	})
}

func InitRaft(node *maelstrom.Node) *Raft {
	r := &Raft{
		mux:                   sync.RWMutex{},
		state:                 Map{Data: map[int]int{}},
		logs:                  InitLogs(),
		node:                  node,
		votedFor:              "",
		leader:                "",
		role:                  ROLE_FOLLOWER,
		term:                  0,
		votes:                 StringSet{Items: map[string]bool{}},
		checkElectionTicker:   10,
		stepDownTicker:        100,
		leaderHeartbeatTicker: 500,
		stepDownTimeout:       2000,
		electionDeadline:      time.Now().Add(generateRandomTimeout(150, 300)),
		stepDownDeadline:      time.Now(),
		sufficientAcks:        false,
		sufficientVotes:       false,
	}
	go r.ScheduleCandidate()
	go r.ScheduleAppendEntries()
	go r.ScheduleStepDownAsLeader()
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
	//log.Println(fmt.Sprintf("%v: %s", time.Now().UnixMilli()-epoch, fmt.Sprintf(message, params...)))
	log.Println(fmt.Sprintf("%v OR %v: %s", time.Now().UnixMilli(), time.Now().UnixMilli()-epoch, fmt.Sprintf(message, params...)))
}

func (r *Raft) Lock(i int) {
	r.Logf("locking %d", i)
	r.mux.Lock()
}

func (r *Raft) Unlock(i int) {
	r.Logf("unlocking %d", i)
	r.mux.Unlock()
}

// When a node becomes a candidate, a new election term is started and the node votes for itself
// TODO: send out Request Vote message to other nodes
func (r *Raft) BecomeCandidate() {
	r.Lock(1)
	r.ResetElectionDeadline()
	// r.ResetStepDownDeadline()
	r.ClearVotes()
	r.votedFor = r.node.ID()
	r.ResetVotesFlag()
	r.ElectLeader("") // remove anyone we may have been following
	r.AssignRole(ROLE_CANDIDATE)
	r.CollectVote(r.node.ID())
	r.AdvanceTerm(r.term + 1)
	r.Logf("Became candidate. Starting new term %d", r.term)
	r.Unlock(1)

	r.RequestVotes()
}

func (r *Raft) BecomeFollower(leader string, term int, lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(2)
		defer r.Unlock(2)
	}
	r.ElectLeader(leader)
	r.AdvanceTerm(term)
	r.ClearVotes()
	r.votedFor = ""
	r.AssignRole(ROLE_FOLLOWER)
	r.ResetElectionDeadline()

	r.Logf("became follower to %s for term %d", leader, r.term)
}

func (r *Raft) BecomeLeader() {
	r.Lock(3)
	defer r.Unlock(3)

	r.Logf("starting become leader process")
	if r.IsFollower() {
		r.Logf("We decided to follow someone else on this term... abort becoming a leader")
		return
	}
	r.ResetStepDownDeadline()
	r.ResetElectionDeadline()
	r.ElectLeader(r.node.ID()) // TODO: empty string or self?
	r.ClearVotes()
	// r.ClearAcks()
	r.votedFor = ""
	r.AssignRole(ROLE_LEADER)

	r.Logf("became leader on term %d", r.term)
}

// Request votes by making RPC calls to other nodes
func (r *Raft) RequestVotes() {
	done := make(chan bool)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()

	for _, node := range r.node.NodeIDs() {
		if node == r.node.ID() {
			continue
		}
		go r.RequestVote(ctx, node, done)
	}

	select {
	case <-done:
		r.Logf("done! got enough votes")
		r.BecomeLeader()
		return

	case <-ctx.Done(): // Didn't receive enough acks in time
		r.Logf("failed to collect votes in time")
	}
}

func (r *Raft) MaybeStepDown(term int, lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(30)
		defer r.Unlock(30)
	}
	if r.Term() < term {
		r.Logf("Stepping down because we see a larger term %d than our term %d", term, r.Term())
		r.BecomeFollower("", term)
	}
}

func (r *Raft) RequestVote(ctx context.Context, peer string, done chan bool) error {
	responseChannel := make(chan maelstrom.Message)
	r.mux.RLock()
	request := map[string]any{
		"type":           "request_vote",
		"term":           r.term,
		"candidate":      r.node.ID(),
		"last_log_term":  r.logs.LastLogTerm(),
		"last_log_index": r.logs.Size(),
	}
	r.mux.RUnlock()

	// Async RPC request
	if err := r.node.RPC(peer, request, func(m maelstrom.Message) error {
		responseChannel <- m
		return nil
	}); err != nil {
		r.Logf("Request vote to peer %s errored: %v", peer, err)
		return err
	}

	select {
	case msg := <-responseChannel:
		var response VoteMessage
		if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
			return jsonErr
		}
		r.Logf("Received vote response from peer %s: %v", peer, response)

		r.Lock(5)
		defer r.Unlock(5)
		r.MaybeStepDown(response.Term)

		// peer voted for us!
		if response.VoteGranted && r.IsCandidate() && !r.HasSufficientVotes() {
			r.CollectVote(peer)
			r.ResetStepDownDeadline()
			if r.HasMajorityVotes() {
				r.Logf("Has majority votes")
				r.sufficientVotes = true
				done <- true
			}
		}

	case <-ctx.Done():
		r.Logf("closing vote request due to parent context reaching deadline...")
	}
	return nil
}

// If this node hasn't voted yet in this term, then it votes for the candidate
// After voting, resets election timeout
func (r *Raft) SubmitVote(msg maelstrom.Message) error {
	var request VoteMessage
	if jsonErr := json.Unmarshal(msg.Body, &request); jsonErr != nil {
		return nil
	}

	r.Logf("Received a request to vote for %s", request.Candidate)
	r.Lock(6)
	defer r.Unlock(6)
	r.MaybeStepDown(request.Term)

	var grant bool
	// TODO: should we check if we have a leader already?
	if request.Term < r.term {
		r.Logf("Not voting for %s because term %d is less than ours %d", request.Candidate, request.Term, r.term)
	} else if r.votedFor != "" {
		r.Logf("Not voting for %s because already voted for %s on term %d", request.Candidate, r.votedFor, r.term)
	} else if request.LastLogTerm < r.logs.LastLogTerm() {
		r.Logf(
			"Not voting for %s because our logs have entries from term %d, which is newer than remote term %d",
			request.Candidate,
			r.logs.LastLogTerm(),
			request.LastLogTerm,
		)
	} else if request.LastLogTerm == r.logs.LastLogTerm() && request.LastLogIndex < r.logs.Size() {
		r.Logf(
			"Not voting for %s because even though last log terms are the same, our log size of %d is bigger than remotes of %d",
			request.Candidate,
			r.logs.Size(),
			request.LastLogIndex,
		)
	} else {
		grant = true
		r.votedFor = request.Candidate
		r.term = request.Term
		r.ResetElectionDeadline()
		r.Logf("Granting vote to %s", request.Candidate)
	}

	response := VoteMessage{
		Type:        "request_vote_response",
		Term:        r.term,
		VoteGranted: grant,
	}
	//r.Unlock(6)

	return r.node.Reply(msg, response)
}

func (r *Raft) AppendEntries() {
	r.ResetAcksFlag(true)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()
	done := make(chan bool)
	acks := []string{r.node.ID()}

	for _, node := range r.node.NodeIDs() {
		if node == r.node.ID() {
			continue
		}
		go r.AppendEntry(ctx, node, &acks, done)
	}

	select {
	case <-done:
		r.Logf("done! got majority acks")
		r.ResetStepDownDeadline(true)
	case <-ctx.Done():
		r.Logf("Didn't receive acks from followers in time... stepping down")
	}
}

func (r *Raft) AppendEntry(ctx context.Context, peer string, acks *[]string, done chan bool) error {
	if !r.IsLeader(true) {
		r.Logf("not going to append this entry because we're not a leader")
		return nil
	}

	responseChannel := make(chan maelstrom.Message)
	request := map[string]any{
		"type": "append_entries",
		"term": r.Term(true),
		"src":  r.node.ID(),
	}
	// Async RPC request
	r.Logf("sending append entry request to node %s", peer)
	if err := r.node.RPC(peer, request, func(m maelstrom.Message) error {
		responseChannel <- m
		return nil
	}); err != nil {
		r.Logf("Append entry to peer %s errored: %v", peer, err)
		return err
	}

	select {
	case msg := <-responseChannel:
		r.Lock(8)
		defer r.Unlock(8)
		var response AckMessage
		if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
			return jsonErr
		}
		r.MaybeStepDown(response.Term)

		// receive ack from peer
		if response.Ack && r.IsLeader() && !r.HasSufficientAcks() {
			*acks = append(*acks, response.Source)
			if r.HasMajorityAcks(acks) {
				r.sufficientAcks = true
				done <- true
			}
		}
	case <-ctx.Done():
		r.Logf("parent context for append entry timed out when sending to %s", peer)
	}

	return nil
}

func (r *Raft) ReceiveAppendEntries(msg maelstrom.Message) error {
	var request AckMessage
	if jsonErr := json.Unmarshal(msg.Body, &request); jsonErr != nil {
		r.Logf("malformed append entries request %s", msg.Body)
		return nil
	}
	var ack bool

	r.Lock(35)
	if request.Term >= r.term {
		r.Logf("append_entries request term %d >= our current term %d", request.Term, r.term)
		r.BecomeFollower(request.Source, request.Term)
		ack = true
	}

	response := AckMessage{
		Type: "append_entries_response",
		Term: r.term,
		Ack:  ack,
	}
	r.Unlock(35)

	// jsonResponse, jsonErr := json.Marshal(response)
	// if jsonErr != nil {
	// 	return jsonErr
	// }
	r.Logf("responding back to append_entries request")
	return r.node.Reply(msg, response)
}

// After the election timeout, a follower becomes a candidate and starts a new election term
func (r *Raft) ScheduleCandidate() {
	for range time.Tick(time.Millisecond * r.checkElectionTicker) {
		if time.Now().After(r.electionDeadline) {
			r.Logf("scheduling to become candidate...%v", r.IsLeader())

			if r.IsLeader() {
				r.Lock(9)
				r.ResetElectionDeadline()
				r.Unlock(9)
				return
			}

			// if we are not a leader, then become a candidate
			r.BecomeCandidate()
		}
	}
	r.Logf("schedule candidate ticker is finishing... wonder why?")
}

// As a leader, we must constantly ping our followers
func (r *Raft) ScheduleAppendEntries() {
	for range time.Tick(time.Millisecond * r.leaderHeartbeatTicker) {
		if r.IsLeader(true) {
			r.Logf("scheduling append entries")
			r.AppendEntries()
		}
	}
	r.Logf("append entries ticker is finishing... wonder why?")
}

func (r *Raft) ScheduleStepDownAsLeader() {
	for range time.Tick(time.Millisecond * r.stepDownTicker) {
		r.Lock(10)
		r.Logf("step down time check; now: %v; deadline: %v IS %v AND %v", time.Now(), r.stepDownDeadline, time.Now().After(r.stepDownDeadline), r.IsLeader())

		if time.Now().After(r.stepDownDeadline) && r.IsLeader() {
			r.Logf("Haven't received Acks recently, Stepping down as leader...")
			r.BecomeFollower("", r.term)
		}
		r.Unlock(10)
	}
	r.Logf("step down as leader ticker is finishing... wonder why?")
}
