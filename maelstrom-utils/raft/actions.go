package raft

import (
	"context"
	"encoding/json"
	"errors"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"time"
)

// Main raft actions (e.g. Role changes, Request/Submit votes, append entries, etc.)

// BecomeCandidate - When a node becomes a candidate, a new election term is started and the node votes for itself
func (r *Raft) BecomeCandidate() {
	r.Lock(1)
	r.ResetElectionDeadline()
	r.ResetVotesFlag()

	r.leader = "" // remove anyone we may have been following
	r.votedFor = r.node.ID()
	r.role = ROLE_CANDIDATE

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
	r.leader = leader
	r.AdvanceTerm(term)
	r.votedFor = ""
	r.role = ROLE_FOLLOWER
	r.ResetElectionDeadline()

	r.matchIndex = nil
	r.nextIndex = nil

	r.Logf("became follower to %s for term %d", leader, r.term)
}

func (r *Raft) BecomeLeader() {
	r.Lock(3)
	defer r.Unlock(3)

	if !r.IsCandidate() {
		r.Logf("Need to be a candidate to become a leader")
		return
	}

	r.ResetStepDownDeadline()
	r.ResetElectionDeadline()

	r.leader = ""
	r.votedFor = ""
	r.role = ROLE_LEADER

	r.nextIndex = map[string]int{}
	r.matchIndex = map[string]int{}
	for _, peer := range r.node.NodeIDs() {
		r.nextIndex[peer] = r.logs.Size() + 1
		r.matchIndex[peer] = 0
	}

	r.Logf("became leader on term %d", r.term)
}

// Request votes by making RPC calls to other nodes
func (r *Raft) RequestVotes() {
	done := make(chan bool)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*1000)
	defer cancel()
	votes := []string{r.node.ID()}
	for _, node := range r.node.NodeIDs() {
		if node == r.node.ID() {
			continue
		}
		go r.requestVote(ctx, node, &votes, done)
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

func (r *Raft) requestVote(ctx context.Context, peer string, votes *[]string, done chan bool) error {
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
		r.maybeStepDown(response.Term)

		// peer voted for us!
		if response.VoteGranted && r.IsCandidate() && !r.HasSufficientVotes() {
			*votes = append(*votes, peer)
			r.ResetStepDownDeadline()
			if r.HasMajority(votes) {
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
	r.maybeStepDown(request.Term)

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
		go r.appendEntry(ctx, node, &acks, done)
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

func (r *Raft) appendEntry(ctx context.Context, peer string, acks *[]string, done chan bool) error {
	defer r.wait.Done()
	if !r.IsLeader(true) {
		r.Logf("not going to append this entry because we're not a leader")
		return nil
	}

	r.mux.RLock()
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
		r.maybeStepDown(response.Term)

		if r.IsLeader() {
			r.ResetStepDownDeadline()

			if response.ReplicationSuccess {
				r.Logf("peer %s replication succeeded, updating next and match index", peer)
				r.nextIndex[peer] = nextIndex + entries.Size()
				r.matchIndex[peer] = r.nextIndex[peer] - 1
			} else {
				r.Logf("peer %s replication failed, decrementing index", peer)
				r.nextIndex[peer]--
			}

			if response.Ack && !r.HasSufficientAcks() {
				*acks = append(*acks, response.Source)
				if r.HasMajority(acks) {
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
	response.ReplicationSuccess = true
	r.Logf("responding back to append_entries request")
	return r.node.Reply(msg, response)
}

// Must be used with a lock
func (r *Raft) maybeStepDown(term int) {
	if r.term < term {
		r.Logf("Stepping down because we see a larger term %d than our term %d", term, r.term)
		r.BecomeFollower("", term)
	}
}

//
//func (r *Raft) Commit(operation any) (any, error) {
//	r.Lock(0)
//	defer r.Unlock(0)
//	r.logs.Append(&Log{
//		Term:      r.term,
//		Operation: operation,
//	})
//	r.Logf("commit logs %s", r.logs.PrintLogs())
//
//	r.commitIndex++
//	return r.stateMachine.Apply(operation)
//}
