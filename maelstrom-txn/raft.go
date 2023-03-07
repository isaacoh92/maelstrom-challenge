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

type Raft struct {
	mux      sync.RWMutex
	state    Map // TODO Map might need its own lock ?
	logs     *Logs
	node     *maelstrom.Node
	role     int
	term     int
	votes    StringSet
	votedFor string
	leader   string

	sufficientAcks  bool
	sufficientVotes bool

	// Tickers for ongoing processes
	checkElectionTicker   time.Duration
	stepDownTicker        time.Duration
	leaderHeartbeatTicker time.Duration

	// Timeouts
	stepDownTimeout time.Duration

	// Deadlines
	electionDeadline time.Time
	stepDownDeadline time.Time
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
		role:                  ROLE_FOLLOWER,
		term:                  0,
		votes:                 StringSet{Items: map[string]bool{}},
		votedFor:              "",
		leader:                "",
		sufficientAcks:        false,
		checkElectionTicker:   10,
		stepDownTicker:        100,
		leaderHeartbeatTicker: 500,
		stepDownTimeout:       2000,
		electionDeadline:      time.Now().Add(generateRandomTimeout(150, 300)),
		stepDownDeadline:      time.Now(),
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

func (r *Raft) ResetElectionDeadline() {
	r.electionDeadline = time.Now().Add(generateRandomTimeout(150, 300))
}

func (r *Raft) ResetStepDownDeadline() {
	r.Logf("resetting step down deadline")
	r.stepDownDeadline = time.Now().Add(r.stepDownTimeout * time.Millisecond)
}

func (r *Raft) Logf(message string, params ...any) {
	log.Println(fmt.Sprintf("%v: %s", time.Now().UnixMilli()-epoch, fmt.Sprintf(message, params...)))
}

func (r *Raft) Lock(i int) {
	r.Logf("locking %d", i)
	r.mux.Lock()
}

func (r *Raft) Unlock(i int) {
	r.Logf("unlocking %d", i)
	r.mux.Unlock()
}
func (r *Raft) AdvanceTerm(term int) {
	if term < r.term {
		r.Logf("Aborting advance term. Current term %d; previous term %d", r.term, term)
	}
	r.term = term
}

func (r *Raft) AssignRole(role int) {
	r.role = role
}

func (r *Raft) ElectLeader(leader string) {
	r.leader = leader
}

func (r *Raft) ClearVotes() {
	r.votes.Clear()
}

func (r *Raft) IsLeader() bool {
	return r.role == ROLE_LEADER
}

func (r *Raft) IsCandidate() bool {
	return r.role == ROLE_CANDIDATE
}

func (r *Raft) IsFollower() bool {
	return r.role == ROLE_FOLLOWER
}

// When a node becomes a candidate, a new election term is started and the node votes for itself
// TODO: send out Request Vote message to other nodes
func (r *Raft) BecomeCandidate() {
	r.Lock(1)
	r.ResetElectionDeadline()
	// r.ResetStepDownDeadline()
	r.ClearVotes()
	// r.ClearAcks()
	r.votedFor = r.node.ID()
	r.sufficientVotes = false
	r.ElectLeader("") // remove anyone we may have been following
	r.AssignRole(ROLE_CANDIDATE)
	r.CollectVote(r.node.ID())
	r.AdvanceTerm(r.term + 1)
	r.Logf("Became candidate. Starting new term %d", r.term)
	r.Unlock(1)

	r.RequestVotes()
}

func (r *Raft) BecomeFollower(leader string, term int) {
	r.Lock(2)
	defer r.Unlock(2)
	r.ElectLeader(leader)
	r.AdvanceTerm(term)
	r.ClearVotes()
	// r.ClearAcks()
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

// broadcasts to other nodes
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

func (r *Raft) MaybeStepDown(term int) {
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
			r.Logf("vote response from peer %s invalid: %v", peer, jsonErr)
			return jsonErr
		}
		r.Logf("Received vote response from peer %s: %v", peer, response)

		r.MaybeStepDown(response.Term)

		// peer voted for us!
		r.Lock(5)
		defer r.Unlock(5)
		if response.VoteGranted && r.IsCandidate() && !r.sufficientVotes {

			r.CollectVote(peer)
			r.ResetStepDownDeadline()
			if r.HasMajorityVotes() {
				r.Logf("Has majority votes")
				r.sufficientVotes = true
				done <- true
			}
			return nil
		}

	case <-ctx.Done():
		r.Logf("closing vote request due to parent context reaching deadline...")
	}

	return nil
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

// type VoteRequest struct {
// 	Type      string `json:"type"`
// 	Candidate string `json:"src,omitempty"`
// 	Term      int    `json:"term"`
// }

func (r *Raft) HasMajorityVotes() bool {
	// r.mux.RLock()
	majority := r.votes.Length() >= len(r.node.NodeIDs())/2+1
	// r.mux.RUnlock()
	return majority
}

func (r *Raft) HasMajorityAcks(acks *[]string) bool {
	//r.mux.RLock()
	//defer r.mux.RUnlock()
	majority := len(*acks) >= len(r.node.NodeIDs())/2+1
	r.Logf("Has majority %v", majority)

	return majority
}

func (r *Raft) CollectVote(node string) {
	switch {
	case r.IsLeader():
		r.Logf("Already role leader, ignoring AddVote request")
	case r.IsFollower():
		r.Logf("Not a candidate, ignoring AddVote request")
	case r.IsCandidate():
		r.votes.Add(node)
	}
}

// If this node hasn't voted yet in this term, then it votes for the candidate
// After voting, resets election timeout
func (r *Raft) SubmitVote(msg maelstrom.Message) error {

	var request VoteMessage
	if jsonErr := json.Unmarshal(msg.Body, &request); jsonErr != nil {
		r.Logf("malformed vote request %s", msg.Body)
		return nil
	}
	r.Logf("Received a request to vote for %s", request.Candidate)

	r.MaybeStepDown(request.Term)
	r.Logf("about to start")
	var grant bool
	r.Lock(6)
	// TODO: add log sizes and log terms to our conditions
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
	r.Unlock(6)

	return r.node.Reply(msg, response)
}

func (r *Raft) VoteForCandidate(candidate string, term int) {
	r.Lock(4)
	r.AdvanceTerm(term)
	r.ClearVotes()
	r.ElectLeader("")
	r.votedFor = candidate
	r.AssignRole(ROLE_FOLLOWER)
	r.ResetElectionDeadline()
	r.Unlock(4)

	r.Logf("Voting for candidate %s", candidate)
}

func (r *Raft) AppendEntries() {
	r.mux.Lock()
	isLeader := r.IsLeader()
	r.sufficientAcks = false
	r.mux.Unlock()

	r.Logf("isleader?")
	if !isLeader {
		r.Logf("not going to append entries because we're not a leader")
		return
	}
	r.Logf("create context?")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()
	done := make(chan bool)
	// acks := StringSet{
	// 	Items: map[string]bool{},
	// }
	acks := []string{r.node.ID()}
	for _, node := range r.node.NodeIDs() {
		if node == r.node.ID() {
			continue
		}
		r.Logf("going to append entry to node %s", node)
		go r.AppendEntry(ctx, node, &acks, done)
	}

	r.Logf("sent acks?")

	select {
	case <-done:
		r.Logf("done! got majority acks")
		r.Lock(7)
		r.ResetStepDownDeadline()
		r.Unlock(7)
		return
	case <-ctx.Done():
		r.Logf("Didn't receive acks from followers in time... stepping down")
	}
}

func (r *Raft) AppendEntry(ctx context.Context, peer string, acks *[]string, done chan bool) error {
	if !r.IsLeader() {
		r.Logf("not going to append this entry because we're not a leader")
		// done <- false
		return nil
	}

	responseChannel := make(chan maelstrom.Message)
	request := map[string]any{
		"type": "append_entries",
		"term": r.Term(),
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
		var response AckMessage
		if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
			r.Logf("vote response from peer %s invalid: %v", peer, jsonErr)
			return jsonErr
		}
		r.MaybeStepDown(response.Term)
		// receive ack from peer
		r.Lock(8)
		defer r.Unlock(8)
		if response.Ack && r.IsLeader() && !r.sufficientAcks {
			r.Logf("Received ack from peer %s", peer)
			//r.Lock(8)
			//r.Unlock(8)
			// acks.Add(response.Source)

			*acks = append(*acks, response.Source)
			r.Logf("ACK Length %d", len(*acks))
			if r.HasMajorityAcks(acks) {
				r.sufficientAcks = true
				done <- true
			}
		}
		//return nil
	case <-ctx.Done():
		r.Logf("parent context for append entry timed out when sending to %s", peer)
	}

	// var response map[string]any

	return nil
}

func (r *Raft) ReceiveAppendEntries(msg maelstrom.Message) error {
	var request AckMessage
	if jsonErr := json.Unmarshal(msg.Body, &request); jsonErr != nil {
		r.Logf("malformed append entries request %s", msg.Body)
		return nil
	}
	currentTerm := r.Term()
	r.Logf("received append entries request from %s", request.Source)
	// r.MaybeStepDown(request.Term)
	var ack bool
	if request.Term >= currentTerm {
		r.Logf("append_entries request term %d >= our current term %d", request.Term, currentTerm)
		r.BecomeFollower(request.Source, request.Term)
		ack = true
	}

	response := AckMessage{
		Type: "append_entries_response",
		Term: currentTerm,
		Ack:  ack,
	}

	// jsonResponse, jsonErr := json.Marshal(response)
	// if jsonErr != nil {
	// 	return jsonErr
	// }
	r.Logf("responding back to append_entries request")
	return r.node.Reply(msg, response)
}

// Use for when a read lock is required
func (r *Raft) Term() int {
	r.mux.RLock()
	defer r.mux.RUnlock()
	return r.term
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
		r.Logf("scheduling append entries")
		r.AppendEntries()
	}
	r.Logf("append entries ticker is finishing... wonder why?")
}

func (r *Raft) ScheduleStepDownAsLeader() {
	for range time.Tick(time.Millisecond * r.stepDownTicker) {
		r.Lock(10)
		r.Logf("step down time check; now: %v; deadline: %v IS %v AND %v", time.Now(), r.stepDownDeadline, time.Now().After(r.stepDownDeadline), r.IsLeader())
		cond := time.Now().After(r.stepDownDeadline) && r.IsLeader()
		r.Unlock(10)
		if cond {
			r.Logf("Haven't received Acks recently, Stepping down as leader...")
			r.BecomeFollower("", r.Term())
		}
	}
	r.Logf("step down as leader ticker is finishing... wonder why?")
}

// // peer did not vote for us :(
// switch {
// case response.Term == currentTerm && response.leader != "": // peer designated a leader already, then that leader reached majority vote before us...follow that node then
// 	r.Logf("term from peer is the same but peer has a leader already... following its leader")
// 	r.BecomeFollower(response.leader, response.Term)
// 	done <- false
// 	return nil
// 	// otherwise, our peer voted for someone else... continue with the voting process. Let's see if we can get leader before other candidates

// case response.Term > currentTerm && response.leader != "":
// 	r.Logf("term from peer %s is greater than current term: %d, and peer has leader.. so following it", peer, currentTerm)
// 	r.BecomeFollower(response.leader, response.Term)
// 	done <- false
// 	return nil

// case response.Term > currentTerm && response.leader == "":
// 	r.Logf("term from peer %s is greater than current term: %d, but peer does not have a leader, must be voting: %s", peer, currentTerm, response.votedFor)
// 	r.BecomeFollower("", currentTerm) // something went wrong, but let's step down as candidate, and no need to follow anyone yet
// 	// TODO: or should we also vote for this peer's voted_for? ... probably not
// 	done <- false
// 	return nil
// }
