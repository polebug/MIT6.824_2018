package raft

import (
	"labrpc"
	// "log"
	"time"
)

const (
	// rpc call timeout
	RPC_TIMEOUT = 500 * time.Millisecond
)

// RequestVoteArgs define RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// RequestVoteReply define RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// AppendEntriesArgs define AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	LeaderCommit int        // leader’s commitIndex
	Logs         []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
}

// AppendEntriesReply define AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
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

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.SetFollower(args.Term)
	}

	// from paper 5.4.1 Election restriction:
	// the RPC includes information about the candidate’s log, and the voter denies its vote
	// if its own log is more up-to-date than that of the candidate.
	// check up-to-date:
	// (1). if the logs have last entries with different terms, then the log with the later term is more up-to-date.
	// (2). if the logs end with the same term, then whichever log is longer is more up-to-date.
	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	if lastLogTerm > args.LastLogTerm {
		return
	}
	if lastLogTerm == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
		return
	}

	if rf.votedFor == VOTE_NIL || rf.votedFor == args.CandidateID {
		// grant vote
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.state = FOLLOWER
		rf.InitElectionConf()
		return
	}
}

// SendRequestVote send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) int {
	var okCh = make(chan bool)

	go func() {
		var ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		// log.Printf("[RequestVote RPC]: candidate = %v, term = %v, req_node = %v, reply = %v | %v,  \n", rf.me, rf.currentTerm, server, ok, reply)
		okCh <- ok
	}()

	select {
	case <-time.After(RPC_TIMEOUT):
		return labrpc.DEADLINE_EXCEEDED
	case ok := <-okCh:
		if ok {
			return labrpc.OK
		}
		return labrpc.UNAVAILABLE
	}
}

// AppendEntries RPC Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
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

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.SetFollower(args.Term)

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	var prevLogIndex = len(rf.logs) - 1
	if prevLogIndex < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// miss log
		// log.Printf("[AppendEntries RPC]: server = %v, prevLogIndex = %v, leader match prevLogIndex = %v, miss log !\n", rf.me, prevLogIndex, args.PrevLogIndex)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry
	var commitIndex = prevLogIndex
	for i, entry := range args.Logs {
		index := args.PrevLogIndex + i + 1
		// term conflict, delete follower's log, overwrite it
		if index <= prevLogIndex && rf.logs[index].Term != entry.Term {
			// log.Printf("[AppendEntries RPC]: server = %v, fix conflict log %v -> %v \n", rf.me, rf.logs[index], entry)
			commitIndex = index
			rf.logs[index] = entry
		}

		// Append any new entries not already in the log
		if index > prevLogIndex {
			// log.Printf("[AppendEntries RPC]: server = %v, log = %v append = %v \n", rf.me, rf.logs, entry)
			rf.logs = append(rf.logs, entry)
			commitIndex = len(rf.logs) - 1
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, commitIndex)
	}

	reply.Success = true
	// log.Printf("[AppendEntries RPC]: server = %v, logs = %v, rf.commitIndex = %v \n", rf.me, rf.logs, rf.commitIndex)
}

// SendAppendEntries send a AppendEntries RPC to a server.
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) int {
	var okCh = make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// log.Printf("[AppendEntries RPC]: leader = %v, term = %v, req_node = %v, reply = %v | %v \n", rf.me, rf.currentTerm, server, ok, reply)
		okCh <- ok
	}()

	select {
	case <-time.After(RPC_TIMEOUT):
		return labrpc.DEADLINE_EXCEEDED
	case ok := <-okCh:
		if ok {
			return labrpc.OK
		}
		return labrpc.UNAVAILABLE
	}
}
