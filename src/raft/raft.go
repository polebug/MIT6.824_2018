package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.

const (
	// a node can be in 1 of 3 states: follower, canidate, leader
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	// not voted
	VOTE_NIL = -1

	// rpc call timeout
	TIMEOUT = 500 * time.Millisecond
)

// ApplyMsg
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry contains state and term, reference raft paper's Figure 2
type LogEntry struct {
	State int // command for state machine
	Term  int // term when entry was received by leader (first index is 1)
}

// Raft struct, reference raft paper's Figure 2
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	state     int                 // current role

	// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateID that received vote in current term (or null if none)
	logs        []LogEntry // log entries

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int // index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// use to election
	electionST       int64 // election start timestamp
	electionDuration int64 // election duration
}

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

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	log.Printf("[GetState] node = %v, term = %v, state = %v \n", rf.me, term, rf.state)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// SetFollower set the peer into Follower
func (rf *Raft) SetFollower(term int) {
	rf.state = FOLLOWER
	rf.votedFor = VOTE_NIL
	rf.currentTerm = term

	rf.InitElectionConf()
	// log.Printf("[State]: node = %v becomes Follower, current_term = %v \n", rf.me, rf.currentTerm)
	return
}

// SetCandidate set the peer into Candidate
func (rf *Raft) SetCandidate() {
	// 1. transitions to candidate state
	rf.state = CANDIDATE

	// 2. increments its current term
	rf.currentTerm++

	// 3. votes for itself
	rf.votedFor = rf.me

	rf.InitElectionConf()
	// log.Printf("[State]: node = %v becomes Candidate, current_term = %v \n", rf.me, rf.currentTerm)
	return
}

// SetLeader set the peer into Leader
func (rf *Raft) SetLeader() {
	rf.state = LEADER
	rf.InitElectionConf()
	// log.Printf("[State]: node = %v becomes Leader, current_term = %v \n", rf.me, rf.currentTerm)
	return
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

	if rf.votedFor == VOTE_NIL || rf.votedFor == args.CandidateID {
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
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	var okCh = make(chan bool)

	go func() {
		var ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		// log.Printf("[RequestVote RPC]: candidate = %v, term = %v, req_node = %v, reply = %v | %v,  \n", rf.me, rf.currentTerm, server, ok,reply)
		okCh <- ok
	}()

	select {
	case <-time.After(TIMEOUT):
		return false
	case <-okCh:
		return true
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
	reply.Success = true
}

// SendAppendEntries send a AppendEntries RPC to a server.
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	var okCh = make(chan bool)

	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		// log.Printf("[AppendEntries RPC]: leader = %v, term = %v, req_node = %v, reply = %v | %v \n", rf.me, rf.currentTerm, server, ok, reply)
		okCh <- ok
	}()

	select {
	case <-time.After(TIMEOUT):
		return false
	case <-okCh:
		return true
	}
}

// InitElectionConf initial election start timestamp, duration, number of votes
func (rf *Raft) InitElectionConf() {
	// The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
	// Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds.
	// Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger than the paper's 150 to 300 milliseconds,
	// but not too large, because then you may fail to elect a leader within five seconds.
	rf.electionST = time.Now().UnixNano() / int64(time.Millisecond)
	rf.electionDuration = rand.Int63n(5)*100 + 300
}

// ElectionTimeOut Followers election time out
// Raft uses randomized election timeouts to ensure that split votes are rare
// and that they are resolved quickly. To prevent split votes in the first place,
// election timeouts are chosen randomly from a fixed interval (e.g., 150–300ms).
// This spreads out the servers so that in most cases only a single server will time out;
// it wins the election and sends heartbeats before any other servers time out.
// The same mechanism is used to handle split votes. Each candidate restarts its randomized
// election timeout at the start of an election, and it waits for that timeout to elapse before
// starting the next election; this reduces the likelihood of another split vote in the new election.
func (rf *Raft) ElectionTimeOut() {
	for {
		// According to rf.electionST and rf.electionDuration,
		// judge whether the election is timed out every 10 milliseconds
		rf.mu.Lock()
		time.Sleep(time.Duration(10) * time.Millisecond)
		var duration = time.Now().UnixNano()/int64(time.Millisecond) - rf.electionST
		if duration >= rf.electionDuration {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
	}

	// After the election timeout,
	// if the current node is Follower or Canidate, enter the election stage
	rf.mu.Lock()
	if rf.state == FOLLOWER || rf.state == CANDIDATE {
		rf.mu.Unlock()
		rf.StartElection()
		return
	}
	rf.mu.Unlock()
}

// StartElection Candidate start election
// According to the paper. begin an election:
// 1. transitions to candidate state.
// 2. issues `RequestVote RPCs in parallel` to each of the other servers in the cluster.
// 3. A candidate continues in this state until one of three things happens:
// (a) it wins the election,
// (b) another server establishes itself as leader, or
// (c) a period of time goes by with no winner.
func (rf *Raft) StartElection() {
	// log.Printf("[Election]: node = %v start\n", rf.me)

	rf.mu.Lock()
	rf.SetCandidate()

	var (
		relyCh = make(chan RequestVoteReply, len(rf.peers))
		wg     sync.WaitGroup
		count  = 1
	)

	var voteReq = RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}
	rf.mu.Unlock()

	for idxPeer := range rf.peers {
		if idxPeer == rf.me {
			rf.InitElectionConf()
			continue
		}

		wg.Add(1)
		go func(server int, voteReq RequestVoteArgs) {
			defer wg.Done()

			var voteResp = RequestVoteReply{}
			if ok := rf.SendRequestVote(server, &voteReq, &voteResp); ok {
				relyCh <- voteResp
			}
		}(idxPeer, voteReq)
	}

	go func() {
		wg.Wait()
		close(relyCh)
	}()

	for resp := range relyCh {
		rf.mu.Lock()
		// 3.(a) results occur
		// A candidate wins an election if it receives votes from a majority of the servers in the full
		// cluster for the same term. Each server will vote for at most one candidate in a given term,
		// on a first-come-first-served basis. The majority rule ensures that at most one candidate can win the
		// election for a particular term (the Election Safety Property in Figure 3).
		// Once a candidate wins an election, it becomes `Leader`.
		if resp.VoteGranted {
			// receive a vote successful
			count++

			if count > len(rf.peers)/2 {
				rf.SetLeader()
				rf.mu.Unlock()

				// After being `Leader`, it then sends heartbeat messages to all of
				// the other servers to establish its authority and prevent new elections.
				go rf.SendHeartBeat()
				return
			}
		}

		// (b) results occur
		// If the leader’s term (another server claiming to be leader) is at least
		// as large as the candidate’s current term, then the `Candidate`
		// recognizes the leader as legitimate and returns to `Follower` state.
		if resp.Term > rf.currentTerm {
			rf.SetFollower(resp.Term)
			rf.mu.Unlock()
			go rf.ElectionTimeOut()
			return
		}
		rf.mu.Unlock()
	}

	// (c) results occur
	// The third possible outcome is that a candidate neither wins nor loses the election:
	// if many followers become candidates at the same time, votes could be split so that
	// no candidate obtains a majority. When this happens, each candidate will time out and
	// start a new election by incrementing its term and initiating another round of RequestVote RPCs.
	log.Println("[WARNING] split vote!")
	rf.mu.Lock()
	rf.SetFollower(rf.currentTerm)
	rf.mu.Unlock()
	go rf.ElectionTimeOut()
}

// SendHeartBeat new `Leader` sends heartbeat messages to all of the other servers
// to establish its authority and prevent new elections.
func (rf *Raft) SendHeartBeat() {
	for true {
		for idxPeer := range rf.peers {
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}

			if idxPeer == rf.me {
				rf.InitElectionConf()
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()

			go func(server int) {
				// send initial empty AppendEntries RPCs (heartbeat) to each server;
				// repeat during idle periods to prevent election timeouts
				rf.mu.Lock()
				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				var (
					appendReq = AppendEntriesArgs{
						LeaderID: rf.me,
						Term:     rf.currentTerm,
					}
					appendResp = AppendEntriesReply{}
				)
				rf.mu.Unlock()

				rf.SendAppendEntries(server, &appendReq, &appendResp)

				rf.mu.Lock()
				if appendResp.Term > rf.currentTerm {
					rf.SetFollower(appendResp.Term)
					rf.mu.Unlock()
					go rf.ElectionTimeOut()
					return
				}
				rf.mu.Unlock()
			}(idxPeer)
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// Make create a Raft server
func Make(
	peers []*labrpc.ClientEnd, // the ports of all the Raft servers, all the servers' peers[] arrays have the same order
	me int, // this server's port is peers[me]
	persister *Persister, // is a place for this server to save its persistent state, and also initially holds the most recent saved state
	applyCh chan ApplyMsg, // a channel on which the tester or service expects Raft to send ApplyMsg messages
) *Raft {
	// initialization
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.SetFollower(0)
	rf.logs = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// create a background goroutine that will kick off leader election periodically
	go rf.ElectionTimeOut()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
