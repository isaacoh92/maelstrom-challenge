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
	Mux   sync.RWMutex
	State Map // TODO Map might need its own lock ?
	Node  *maelstrom.Node
	Role  int
	term  int
	Votes StringSet
	// Acks                  StringSet
	VotedFor              string
	Leader                string
	CheckElectionTicker   time.Duration
	StepDownTicker        time.Duration
	LeaderHeartbeatTicker time.Duration
	StepDownTimeout       time.Duration
	ElectionDeadline      time.Time
	StepDownDeadline      time.Time
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
	r.Mux.Lock()
	txs := []any{}
	for _, txn := range req["txn"].([]any) {
		res, err := r.State.Apply(txn.([]any))
		if err != nil {
			return err
		}
		txs = append(txs, res)
	}
	r.Mux.Unlock()
	return r.Node.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  txs,
	})
}

func InitRaft(node *maelstrom.Node) *Raft {
	r := &Raft{
		Mux:                   sync.RWMutex{},
		State:                 Map{Data: map[int]int{}},
		Node:                  node,
		Role:                  ROLE_FOLLOWER,
		term:                  0,
		Votes:                 StringSet{Items: map[string]bool{}},
		VotedFor:              "",
		Leader:                "",
		CheckElectionTicker:   10,
		StepDownTicker:        100,
		LeaderHeartbeatTicker: 500,
		StepDownTimeout:       2000,
		ElectionDeadline:      time.Now().Add(generateRandomTimeout(150, 300)),
		StepDownDeadline:      time.Now(),
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
	// r.Mux.Lock()
	r.ElectionDeadline = time.Now().Add(generateRandomTimeout(150, 300))
	// r.Mux.Unlock()
}

func (r *Raft) ResetStepDownDeadline() {
	// r.Mux.Lock()
	r.Log("resetting step down deadline")
	r.StepDownDeadline = time.Now().Add(r.StepDownTimeout * time.Millisecond)
	// r.Mux.Unlock()
}

func (r *Raft) Log(message string, params ...any) {
	log.Println(fmt.Sprintf("%v: %s", time.Now().UnixMilli()-epoch, fmt.Sprintf(message, params...)))
}

func (r *Raft) AdvanceTerm(term int) {
	if term < r.term {
		r.Log("Aborting advance term. Current term %d; previous term %d", r.term, term)
	}
	r.term = term
}

func (r *Raft) AssignRole(role int) {
	r.Role = role
}

func (r *Raft) ElectLeader(leader string) {
	r.Leader = leader
}

func (r *Raft) ClearVotes() {
	r.Votes.Clear()
}

// func (r *Raft) ClearAcks() {
// 	r.Acks.Clear()
// }

func (r *Raft) IsLeader() bool {
	return r.Role == ROLE_LEADER
}

func (r *Raft) IsCandidate() bool {
	return r.Role == ROLE_CANDIDATE
}

func (r *Raft) IsFollower() bool {
	return r.Role == ROLE_FOLLOWER
}

// When a node becomes a candidate, a new election term is started and the node votes for itself
// TODO: send out Request Vote message to other nodes
func (r *Raft) BecomeCandidate() {
	r.Mux.Lock()
	r.ResetElectionDeadline()
	// r.ResetStepDownDeadline()
	r.ClearVotes()
	// r.ClearAcks()
	r.VotedFor = r.Node.ID()
	r.ElectLeader("") // remove anyone we may have been following
	r.AssignRole(ROLE_CANDIDATE)
	r.CollectVote(r.Node.ID())
	r.AdvanceTerm(r.term + 1)
	r.Log("Became candidate. Starting new term %d", r.term)
	r.Mux.Unlock()

	r.RequestVotes()
}

func (r *Raft) BecomeFollower(leader string, term int) {
	r.Mux.Lock()
	defer r.Mux.Unlock()
	r.ElectLeader(leader)
	r.AdvanceTerm(term)
	r.ClearVotes()
	// r.ClearAcks()
	r.VotedFor = ""
	r.AssignRole(ROLE_FOLLOWER)
	r.ResetElectionDeadline()

	r.Log("became follower to %s for term %d", leader, r.term)
}

func (r *Raft) BecomeLeader() {
	r.Mux.Lock()
	defer r.Mux.Unlock()

	r.Log("starting become leader process")
	if r.IsFollower() {
		r.Log("We decided to follow someone else on this term... abort becoming a leader")
		return
	}
	r.ResetStepDownDeadline()
	r.ResetElectionDeadline()
	r.ElectLeader(r.Node.ID()) // TODO: empty string or self?
	r.ClearVotes()
	// r.ClearAcks()
	r.VotedFor = ""
	r.AssignRole(ROLE_LEADER)

	r.Log("became leader on term %d", r.term)
}

// broadcasts to other nodes
func (r *Raft) RequestVotes() {
	done := make(chan bool)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()

	for _, node := range r.Node.NodeIDs() {
		if node == r.Node.ID() {
			continue
		}
		go r.RequestVote(ctx, node, done)
	}

	select {
	case <-done:
		r.Log("done! got enough votes")
		r.BecomeLeader()
		return

	case <-ctx.Done(): // Didn't receive enough acks in time
		r.Log("failed to collect votes in time")
	}
}

func (r *Raft) MaybeStepDown(term int) {
	if r.Term() < term {
		r.Log("Stepping down because we see a larger term %d than our term %d", term, r.Term())
		r.BecomeFollower("", term)
	}
}

func (r *Raft) RequestVote(ctx context.Context, peer string, done chan bool) error {
	responseChannel := make(chan maelstrom.Message)
	request := map[string]any{
		"type":      "request_vote",
		"term":      r.Term(),
		"candidate": r.Node.ID(),
	}

	// Async RPC request
	if err := r.Node.RPC(peer, request, func(m maelstrom.Message) error {
		responseChannel <- m
		return nil
	}); err != nil {
		r.Log("Request vote to peer %s errored: %v", peer, err)
		return err
	}

	select {
	case msg := <-responseChannel:

		var response VoteMessage
		if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
			r.Log("vote response from peer %s invalid: %v", peer, jsonErr)
			return jsonErr
		}
		r.Log("Received vote response from peer %s: %v", peer, response)

		r.MaybeStepDown(response.Term)

		// peer voted for us!
		if response.VoteGranted {
			r.Mux.Lock()
			defer r.Mux.Unlock()
			r.CollectVote(peer)
			r.ResetStepDownDeadline()
			if r.HasMajorityVotes() {
				r.Log("Has majority votes")
				done <- true
			}
			return nil
		}

	case <-ctx.Done():
		r.Log("closing vote request due to parent context reaching deadline...")
	}

	return nil
}

type VoteMessage struct {
	Type        string `json:"type"`
	Candidate   string `json:"candidate,omitempty"`
	Term        int    `json:"term"`
	VoteGranted bool   `json:"vote_granted,omitempty"`
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
	// r.Mux.RLock()
	majority := r.Votes.Length() >= len(r.Node.NodeIDs())/2+1
	// r.Mux.RUnlock()
	return majority
}

func (r *Raft) HasMajorityAcks(acks *[]string) bool {
	// r.Mux.RLock()
	majority := len(*acks) >= len(r.Node.NodeIDs())/2+1
	r.Log("Has majority %v", majority)
	// r.Mux.RUnlock()
	return majority
}

func (r *Raft) CollectVote(node string) {
	// r.Mux.Lock()
	switch {
	case r.IsLeader():
		r.Log("Already role leader, ignoring AddVote request")
	case r.IsFollower():
		r.Log("Not a candidate, ignoring AddVote request")
	case r.IsCandidate():
		r.Votes.Add(node)
	}
	// r.Mux.Unlock()
}

// If this node hasn't voted yet in this term, then it votes for the candidate
// After voting, resets election timeout
func (r *Raft) SubmitVote(msg maelstrom.Message) error {

	var request VoteMessage
	if jsonErr := json.Unmarshal(msg.Body, &request); jsonErr != nil {
		r.Log("malformed vote request %s", msg.Body)
		return nil
	}
	r.Log("Received a request to vote for %s", request.Candidate)

	r.MaybeStepDown(request.Term)
	r.Log("about to start")
	var grant bool
	r.Mux.Lock()
	// TODO: add log sizes and log terms to our conditions
	if request.Term < r.term {
		r.Log("Not voting for %s because term %d is less than ours %d", request.Candidate, request.Term, r.term)
	} else if r.VotedFor != "" {
		r.Log("Not voting for %s because already voted for %s on term %d", request.Candidate, r.VotedFor, r.term)
	} else {
		grant = true
		r.VotedFor = request.Candidate
		r.term = request.Term
		r.ResetElectionDeadline()
		r.Log("Granting vote to %s", request.Candidate)
	}

	response := VoteMessage{
		Type:        "request_vote_response",
		Term:        r.term,
		VoteGranted: grant,
	}
	r.Mux.Unlock()

	// jsonResponse, jsonErr := json.Marshal(response)
	// if jsonErr != nil {
	// 	r.Log("submite vote json marshal err %v", jsonErr)
	// 	return jsonErr
	// }
	return r.Node.Reply(msg, response)
}

func (r *Raft) VoteForCandidate(candidate string, term int) {
	r.Mux.Lock()
	r.AdvanceTerm(term)
	r.ClearVotes()
	// r.ClearAcks()
	r.ElectLeader("")
	r.VotedFor = candidate
	r.AssignRole(ROLE_FOLLOWER)
	r.ResetElectionDeadline()
	r.Mux.Unlock()

	r.Log("Voting for candidate %s", candidate)
}

var appendCounter int = 0
var appendLock sync.RWMutex = sync.RWMutex{}

func (r *Raft) AppendEntries() {
	appendLock.Lock()
	appendCounter++
	counter := appendCounter
	appendLock.Unlock()
	r.Log("goroutine %d", counter)
	r.Mux.RLock()
	isLeader := r.IsLeader()
	r.Mux.RUnlock()

	r.Log("isleader?")
	if !isLeader {
		r.Log("not going to append entries because we're not a leader")
		return
	}
	r.Log("create context?")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()
	done := make(chan bool)
	// acks := StringSet{
	// 	Items: map[string]bool{},
	// }
	acks := []string{r.Node.ID()}
	for _, node := range r.Node.NodeIDs() {
		if node == r.Node.ID() {
			continue
		}
		r.Log("going to append entry to node %s", node)
		go r.AppendEntry(ctx, node, &acks, done)
	}

	r.Log("sent acks?")

	select {
	case <-done:
		r.Log("done! got majority acks")
		r.Mux.Lock()
		r.ResetStepDownDeadline()
		r.Mux.Unlock()
		return
	case <-ctx.Done():
		r.Log("goroutine done %d", counter)
		r.Log("Didn't receive acks from followers in time... stepping down %d", counter)
	}
}

func (r *Raft) AppendEntry(ctx context.Context, peer string, acks *[]string, done chan bool) error {
	if !r.IsLeader() {
		r.Log("not going to append this entry because we're not a leader")
		// done <- false
		return nil
	}

	responseChannel := make(chan maelstrom.Message)
	request := map[string]any{
		"type": "append_entries",
		"term": r.Term(),
		"src":  r.Node.ID(),
	}
	// Async RPC request
	r.Log("sending append entry request to node %s", peer)
	if err := r.Node.RPC(peer, request, func(m maelstrom.Message) error {
		responseChannel <- m
		return nil
	}); err != nil {
		r.Log("Append entry to peer %s errored: %v", peer, err)
		return err
	}

	select {

	case msg := <-responseChannel:
		var response AckMessage
		if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
			r.Log("vote response from peer %s invalid: %v", peer, jsonErr)
			return jsonErr
		}
		r.MaybeStepDown(response.Term)
		// receive ack from peer
		if response.Ack {
			r.Log("Received ack from peer %s", peer)
			r.Mux.Lock()
			defer r.Mux.Unlock()
			// acks.Add(response.Source)

			*acks = append(*acks, response.Source)
			r.Log("ACK Length %d", len(*acks))
			if r.HasMajorityAcks(acks) {
				done <- true
			}
		}
		return nil
	case <-ctx.Done():
		r.Log("parent context for append entry timed out when sending to %s", peer)
	}

	// var response map[string]any

	return nil
}

func (r *Raft) ReceiveAppendEntries(msg maelstrom.Message) error {
	var request AckMessage
	if jsonErr := json.Unmarshal(msg.Body, &request); jsonErr != nil {
		r.Log("malformed append entries request %s", msg.Body)
		return nil
	}
	currentTerm := r.Term()
	r.Log("received append entries request from %s", request.Source)
	// r.MaybeStepDown(request.Term)
	var ack bool
	if request.Term >= currentTerm {
		r.Log("append_entries request term %d >= our current term %d", request.Term, currentTerm)
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
	r.Log("responding back to append_entries request")
	return r.Node.Reply(msg, response)
}

// Use for when a read lock is required
func (r *Raft) Term() int {
	r.Mux.RLock()
	defer r.Mux.RUnlock()
	return r.term
}

// After the election timeout, a follower becomes a candidate and starts a new election term
func (r *Raft) ScheduleCandidate() {
	for range time.Tick(time.Millisecond * r.CheckElectionTicker) {
		if time.Now().After(r.ElectionDeadline) {
			r.Log("scheduling to become candidate...%v", r.IsLeader())

			if r.IsLeader() {
				r.Mux.Lock()
				r.ResetElectionDeadline()
				r.Mux.Unlock()
				return
			}

			// if we are not a leader, then become a candidate
			r.BecomeCandidate()
		}
	}
	r.Log("schedule candidate ticker is finishing... wonder why?")
}

// As a leader, we must constantly ping our followers
func (r *Raft) ScheduleAppendEntries() {
	for range time.Tick(time.Millisecond * r.LeaderHeartbeatTicker) {
		r.Log("scheduling append entries")
		r.AppendEntries()
	}
	r.Log("append entries ticker is finishing... wonder why?")
}

func (r *Raft) ScheduleStepDownAsLeader() {
	for range time.Tick(time.Millisecond * r.StepDownTicker) {
		r.Mux.Lock()
		r.Log("step down time check; now: %v; deadline: %v IS %v AND %v", time.Now(), r.StepDownDeadline, time.Now().After(r.StepDownDeadline), r.IsLeader())
		cond := time.Now().After(r.StepDownDeadline) && r.IsLeader()
		r.Mux.Unlock()
		if cond {
			r.Log("Haven't received Acks recently, Stepping down as leader...")
			r.BecomeFollower("", r.Term())
		}
	}
	r.Log("step down as leader ticker is finishing... wonder why?")
}

// // peer did not vote for us :(
// switch {
// case response.Term == currentTerm && response.Leader != "": // peer designated a leader already, then that leader reached majority vote before us...follow that node then
// 	r.Log("term from peer is the same but peer has a leader already... following its leader")
// 	r.BecomeFollower(response.Leader, response.Term)
// 	done <- false
// 	return nil
// 	// otherwise, our peer voted for someone else... continue with the voting process. Let's see if we can get leader before other candidates

// case response.Term > currentTerm && response.Leader != "":
// 	r.Log("term from peer %s is greater than current term: %d, and peer has leader.. so following it", peer, currentTerm)
// 	r.BecomeFollower(response.Leader, response.Term)
// 	done <- false
// 	return nil

// case response.Term > currentTerm && response.Leader == "":
// 	r.Log("term from peer %s is greater than current term: %d, but peer does not have a leader, must be voting: %s", peer, currentTerm, response.VotedFor)
// 	r.BecomeFollower("", currentTerm) // something went wrong, but let's step down as candidate, and no need to follow anyone yet
// 	// TODO: or should we also vote for this peer's voted_for? ... probably not
// 	done <- false
// 	return nil
// }
