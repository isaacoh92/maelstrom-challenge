package raft

import (
	"context"
	"encoding/json"
	"errors"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"sort"
	"time"
)

// Main raft actions (e.g. Request/Submit votes, append entries, etc.)
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
	request := VoteRequest{
		Type:         "request_vote",
		Candidate:    r.node.ID(),
		Term:         r.term,
		LastLogTerm:  r.logs.LastLogTerm(),
		LastLogIndex: r.logs.Size(),
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
		var response VoteResponse
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
	var request VoteRequest
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

	response := VoteResponse{
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
		r.wait.Add(1)
		go r.AppendEntry(ctx, node, &acks, done)
	}

	select {
	case <-done:
		r.Logf("done! got majority acks")
		r.ResetStepDownDeadline(true)
	case <-ctx.Done():
		r.Logf("Didn't receive acks from followers in time... stepping down")
	}
	r.wait.Wait()
}

func (r *Raft) AppendEntry(ctx context.Context, peer string, acks *[]string, done chan bool) error {
	defer r.wait.Done()
	if !r.IsLeader(true) {
		r.Logf("not going to append this entry because we're not a leader")
		return nil
	}

	r.mux.RLock()
	r.Logf("current leader state machine %v", r.stateMachine.Data)
	nextIndex := r.nextIndex[peer]
	entries := r.logs.FromIndex(nextIndex)

	responseChannel := make(chan maelstrom.Message)

	request := AppendEntryRequest{
		Type:         "append_entries",
		Term:         r.term,
		Leader:       r.node.ID(),
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  r.logs.Get(nextIndex - 1).Term,
		LeaderCommit: r.commitIndex,
		Entries:      entries.Entries,
	}
	r.mux.RUnlock()
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
		var response AppendEntryResponse
		if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
			return jsonErr
		}
		r.MaybeStepDown(response.Term)

		if r.IsLeader() {
			r.ResetStepDownDeadline()

			if response.ReplicationSuccess {
				r.Logf("peer %s replication succeeded, updating next and match index", peer)
				r.nextIndex[peer] = nextIndex + entries.Size()
				r.matchIndex[peer] = r.nextIndex[peer] - 1
				r.AdvanceCommitIndex()
			} else {
				r.Logf("peer %s replication failed, decrementing index", peer)
				r.nextIndex[peer]--
			}

			if response.Ack && !r.HasSufficientAcks() {
				*acks = append(*acks, response.Source)
				if r.HasMajorityAcks(acks) {
					r.sufficientAcks = true
					done <- true
				}
			}
		}
	case <-ctx.Done():
		r.Logf("parent context for append entry timed out when sending to %s", peer)
	}

	return nil
}

func (r *Raft) AdvanceCommitIndex(lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(55)
		defer r.Unlock(55)
	}
	if !r.IsLeader() {
		return
	}

	medianCommit := r.MedianCommitIndex()
	if r.commitIndex < medianCommit && r.logs.Get(medianCommit).Term == r.term {
		r.Logf("Advancing commit from %d to %d", r.commitIndex, medianCommit)
		r.commitIndex = medianCommit
	}
}

func (r *Raft) MedianCommitIndex() int {
	matches := []int{}
	for _, matched := range r.matchIndex {
		matches = append(matches, matched)
	}
	sort.Ints(matches)
	l := len(matches)
	var median int
	if l%2 == 0 {
		median = matches[l/2-1]
	} else {
		median = matches[l/2]
	}
	return median
}

func (r *Raft) AdvanceStateMachine(lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(57)
		defer r.Unlock(57)
	}
	for {
		if r.lastApplied >= r.commitIndex {
			return
		}
		r.lastApplied++
		req := r.logs.Get(r.lastApplied).Operation
		for _, txn := range req {
			r.stateMachine.Apply(txn.([]any))
		}
	}
}

func (r *Raft) ReceiveAppendEntries(msg maelstrom.Message) error {
	var request AppendEntryRequest
	if jsonErr := json.Unmarshal(msg.Body, &request); jsonErr != nil {
		r.Logf("malformed append entries request %s", msg.Body)
		return nil
	}

	r.Lock(35)
	defer r.Unlock(35)
	response := AppendEntryResponse{
		Type: "append_entries_response",
		Term: r.term,
	}
	if request.Term < r.term {
		r.Logf("append_entries request deny ACK -- term is less than ours %d", r.term)
		return r.node.Reply(msg, response)
	}

	r.ResetElectionDeadline()
	response.Ack = true
	defer r.BecomeFollower(request.Leader, request.Term)

	if request.PrevLogIndex <= 0 {
		r.Logf("append_entries request index is out of bounds")
		return errors.New("previous log index out of bounds")
	}

	// Invalid if we don't have the prev log index, or if we do and the terms do not match
	if !r.logs.HasIndex(request.PrevLogIndex) || r.logs.Get(request.PrevLogIndex).Term != request.PrevLogTerm {
		r.Logf("append_entries request term/index mismatch")
		return r.node.Reply(msg, response)
	}

	r.logs.Truncate(request.PrevLogIndex)
	r.logs.Append(request.Entries...)

	if r.commitIndex < request.LeaderCommit {
		r.commitIndex = min(r.logs.Size(), request.LeaderCommit)
	}
	r.AdvanceStateMachine()
	r.Logf("current state %v", r.stateMachine.Data)
	response.ReplicationSuccess = true
	r.Logf("responding back to append_entries request")
	return r.node.Reply(msg, response)
}