package main

import "time"

// Modifiers
func (r *Raft) ResetElectionDeadline(lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(20)
		defer r.Unlock(20)
	}
	r.electionDeadline = time.Now().Add(generateRandomTimeout(150, 300))
}

func (r *Raft) ResetStepDownDeadline(lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(21)
		defer r.Unlock(21)
	}
	r.Logf("resetting step down deadline")
	r.stepDownDeadline = time.Now().Add(r.stepDownTimeout * time.Millisecond)
}

func (r *Raft) AdvanceTerm(term int, lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(22)
		defer r.Unlock(22)
	}
	if term < r.term {
		r.Logf("Aborting advance term. Current term %d; previous term %d", r.term, term)
		return
	}
	r.term = term
}

func (r *Raft) AssignRole(role int, lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(23)
		defer r.Unlock(23)
	}
	r.role = role
}

func (r *Raft) ElectLeader(leader string, lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(24)
		defer r.Unlock(24)
	}
	r.leader = leader
}

func (r *Raft) ClearVotes(lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(21)
		defer r.Unlock(21)
	}
	r.votes.Clear()
}

func (r *Raft) CollectVote(node string, lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(21)
		defer r.Unlock(21)
	}
	switch {
	case r.IsLeader():
		r.Logf("Already role leader, ignoring AddVote request")
	case r.IsFollower():
		r.Logf("Not a candidate, ignoring AddVote request")
	case r.IsCandidate():
		r.votes.Add(node)
	}
}

func (r *Raft) ResetAcksFlag(lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(28)
		defer r.Unlock(28)
	}
	r.sufficientAcks = false
}

func (r *Raft) ResetVotesFlag(lock ...bool) {
	if len(lock) != 0 && lock[0] {
		r.Lock(28)
		defer r.Unlock(28)
	}
	r.sufficientVotes = false
}
