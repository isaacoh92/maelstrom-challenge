package raft

import "time"

// Accessors

func (r *Raft) IsLeader(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.role == roleLeader
}

func (r *Raft) IsCandidate(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.role == roleCandidate
}

func (r *Raft) IsFollower(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.role == roleFollower
}

func (r *Raft) Term(lock ...bool) int {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.term
}

func (r *Raft) Leader(lock ...bool) string {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.leader
}

func (r *Raft) HasMajority(a *[]string, lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	majority := len(*a) >= len(r.node.NodeIDs())/2+1
	return majority
}

func (r *Raft) HasSufficientAcks(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.sufficientAcks
}

func (r *Raft) HasSufficientVotes(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.sufficientVotes
}

func (r *Raft) ElectionDeadlineExceeded(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return time.Now().After(r.electionDeadline)
}
