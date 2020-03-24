package raft

import (
	rpc "labrpc"

	// "log"
	"time"
)

// RequestVoteArgs define RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// according to paper figure 2:
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply define RequestVote RPC reply structure.
type RequestVoteReply struct {
	// according to paper figure 2:
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntriesArgs define AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	// according to paper figure 2:
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	LeaderCommit int        // leader’s commitIndex
	Logs         []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	// LastIncludedIndex int
}

// AppendEntriesReply define AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	// according to paper figure 2:
	Term          int  // currentTerm, for leader to update itself
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ExistSnapshot bool // whether a snapshot already exists
}

// InstallSnapshotArgs define InstallSnapshot RPC arguments structure.
// from the hints of lab3B: "you should send the entire snapshot in a single InstallSnapshot RPC.
// You do not have to implement Figure 13's offset mechanism for splitting up the snapshot."
// so it don't use parameters: Offset, Done.
type InstallSnapshotArgs struct {
	// according to paper figure 13:
	Term              int    // leader’s term
	LeaderID          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

// InstallSnapshotReply define InstallSnapshot RPC reply structure.
type InstallSnapshotReply struct {
	Term        int  // currentTerm, for leader to update itself
	IfCommitted bool // whether all logs have been committed
}

// RequestVote RPC invoked by candidates to gather votes
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Receiver implementation, reference raft paper's Figure 2:
	// 1. Reply false if args.term < rf.currentTerm (§5.1)
	// 2. If rf.votedFor is null or rf.candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("[State]: node = %v, state = %d, current_term = %v \n", rf.me, rf.state, rf.currentTerm)

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm {
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.SetFollower(args.Term)
	}

	// from paper 5.4.1 Election restriction:
	// the RPC includes information about the candidate’s log, and the voter denies its vote
	// if its own log is more up-to-date than that of the candidate.
	// check up-to-date:
	// (1). if the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// (2). if the logs end with the same term, then whichever log is longer is more up-to-date.
	lastLogTerm := rf.Logs[len(rf.Logs)-1].Term
	if lastLogTerm > args.LastLogTerm {
		return
	}
	if lastLogTerm == args.LastLogTerm && len(rf.Logs)-1 > args.LastLogIndex {
		return
	}

	if rf.VotedFor == VOTE_NIL || rf.VotedFor == args.CandidateID {
		// grant vote
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		rf.state = FOLLOWER

		rf.persist()
		rf.InitElectionConf()
		return
	}
}

// AppendEntries RPC invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Receiver implementation, reference raft paper's Figure 2:
	// 1. Reply false if term < currentTerm (§5.1)
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.Success = false
	reply.ExistSnapshot = false

	if args.Term < rf.CurrentTerm {
		return
	}

	rf.SetFollower(args.Term)

	if args.PrevLogIndex < rf.LastIncludedIndex {
		reply.ExistSnapshot = true
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	var prevLogIndex = len(rf.Logs) - 1 + rf.LastIncludedIndex
	if prevLogIndex < args.PrevLogIndex || rf.Logs[args.PrevLogIndex-rf.LastIncludedIndex].Term != args.PrevLogTerm {
		// miss log
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
	var commitIndex = prevLogIndex
	for i, entry := range args.Logs {
		index := args.PrevLogIndex + i + 1
		// term conflict, delete follower's log, overwrite it
		if index <= prevLogIndex && rf.Logs[index-rf.LastIncludedIndex].Term != entry.Term {
			commitIndex = index
			rf.Logs[index-rf.LastIncludedIndex] = entry
		}

		// Append any new entries not already in the log
		if index > prevLogIndex {
			rf.Logs = append(rf.Logs, entry)
			commitIndex = len(rf.Logs) + rf.LastIncludedIndex - 1
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, commitIndex)
	}

	rf.persist()
	reply.Success = true
}

// InstallSnapshot RPC invoked by leader to send snapshot
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// receiver implementation, according to paper figure 13.
	// (ignore the offset mechanism to split the snapshot)

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	// 7. Discard the entire log
	// 8. Reset state machine using snapshot contents (and load
	// snapshot’s cluster configuration)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// log.Printf("server = %v, log1 = %v, args.Last = [%v, %v], rf.Last = [%v, %v] \n", rf.me, rf.Logs, args.LastIncludedIndex, args.LastIncludedTerm, rf.LastIncludedIndex, rf.LastIncludedTerm)

	// 1. Reply immediately if term < currentTerm
	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		reply.IfCommitted = false
		return
	}

	// this gives the follower a sign of life with each chunk, so it can reset its election timer.
	rf.SetFollower(args.Term)
	reply.Term = rf.CurrentTerm

	// 6. If existing log entry has same index and term as snapshot’s
	// last included entry, retain log entries following it and reply
	// 7. Discard the entire log
	if args.LastIncludedIndex >= rf.LastIncludedIndex && args.LastIncludedIndex <= rf.LastIncludedIndex+len(rf.Logs)-1 && rf.Logs[args.LastIncludedIndex-rf.LastIncludedIndex].Term == args.LastIncludedTerm {
		rf.Logs = rf.Logs[args.LastIncludedIndex-rf.LastIncludedIndex:]
		reply.IfCommitted = false
	} else {
		rf.Logs = []LogEntry{
			{
				Term: rf.LastIncludedTerm,
			},
		}
		rf.lastApplied = rf.LastIncludedIndex
		rf.commitIndex = rf.LastIncludedIndex
		reply.IfCommitted = true
	}

	// 8. Reset state machine using snapshot contents (and load snapshot's cluster configuration)
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	raftstate := rf.persist()
	rf.persister.SaveStateAndSnapshot(raftstate, args.Data)

	msg := ApplyMsg{
		CommandValid: false, // it's snapshot raw data, not a command
		CommandIndex: rf.LastIncludedIndex,
		Command:      args.Data,
	}
	rf.applyCh <- msg
	rf.lastApplied = rf.LastIncludedIndex
}

// SendRPCHandler send RPC request
func (rf *Raft) SendRPCHandler(server int, method string, args interface{}, reply interface{}) rpc.Code {
	var okCh = make(chan bool)

	go func() {
		var ok = rf.peers[server].Call(method, args, reply)
		okCh <- ok
	}()

	select {
	case <-time.After(rpc.RPC_TIMEOUT):
		return rpc.DEADLINE_EXCEEDED
	case ok := <-okCh:
		if ok {
			return rpc.OK
		}
		return rpc.UNAVAILABLE
	}
}
