package main

// Accessors

func (r *Raft) IsLeader(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.role == ROLE_LEADER
}

func (r *Raft) IsCandidate(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.role == ROLE_CANDIDATE
}

func (r *Raft) IsFollower(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.role == ROLE_FOLLOWER
}

func (r *Raft) Term(lock ...bool) int {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	return r.term
}

func (r *Raft) HasMajorityVotes(lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	majority := r.votes.Length() >= len(r.node.NodeIDs())/2+1
	return majority
}

func (r *Raft) HasMajorityAcks(acks *[]string, lock ...bool) bool {
	if len(lock) != 0 && lock[0] {
		r.mux.RLock()
		defer r.mux.RUnlock()
	}
	majority := len(*acks) >= len(r.node.NodeIDs())/2+1
	r.Logf("Has majority %v", majority)

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
