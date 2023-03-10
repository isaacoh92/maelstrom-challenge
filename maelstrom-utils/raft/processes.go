package raft

import "time"

// Long-lasting processes each raft instance executes
// After the election timeout, a follower becomes a candidate and starts a new election term
func (r *Raft) ScheduleCandidate() {
	for range time.Tick(time.Millisecond * r.checkElectionTicker) {
		if r.ElectionDeadlineExceeded(true) {
			r.Logf("scheduling to become candidate...%v", r.IsLeader(true))

			if r.IsLeader(true) {
				r.ResetElectionDeadline(true)
				return
			}

			// if we are not a leader, then become a candidate
			r.BecomeCandidate()
		}
	}
}

// As a leader, we must constantly ping our followers
func (r *Raft) ScheduleAppendEntries() {
	for range time.Tick(time.Millisecond * r.leaderHeartbeatTicker) {
		if r.IsLeader(true) {
			r.Logf("scheduling append entries")
			r.AppendEntries()
		}
	}
}

func (r *Raft) ScheduleStepDownAsLeader() {
	for range time.Tick(time.Millisecond * r.stepDownTicker) {
		r.Lock(10)
		if time.Now().After(r.stepDownDeadline) && r.IsLeader() {
			r.Logf("Haven't received Acks recently, Stepping down as leader...")
			r.BecomeFollower("", r.term)
		}
		r.Unlock(10)
	}
}

func (r *Raft) PrintLeader() {
	for range time.Tick(time.Millisecond * 1000) {
		r.mux.RLock()
		leader := r.leader
		if r.IsLeader() {
			leader = r.node.ID()
		}
		r.Logf("My leader for term %d is %s", r.term, leader)
		r.mux.RUnlock()
	}
}
