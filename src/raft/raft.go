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
	"bytes"
	"labgob"
	rpc "labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

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
)

// ApplyMsg
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// LogEntry contains state and term, reference raft paper's Figure 2
type LogEntry struct {
	Command interface{} // command for state machine
	Term    int         // term when entry was received by leader (first index is 1)
}

// Raft struct, reference raft paper's Figure 2
type Raft struct {
	mu        sync.Mutex       // Lock to protect shared access to this peer's state
	peers     []*rpc.ClientEnd // RPC end points of all peers
	persister *Persister       // Object to hold this peer's persisted state
	me        int              // this peer's index into peers[]
	state     int              // current role

	// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	CurrentTerm int        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	VotedFor    int        // candidateID that received vote in current term (or null if none)
	Logs        []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// apply command chan
	applyCh chan ApplyMsg

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int // index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// use to election
	electionST       int64 // election start timestamp
	electionDuration int64 // election duration

	// snapshot, from paper figure 12: (persistent state)
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex
}

// SetFollower set the peer into Follower
func (rf *Raft) SetFollower(term int) {
	rf.state = FOLLOWER
	rf.VotedFor = VOTE_NIL
	rf.CurrentTerm = term

	rf.persist()
	rf.InitElectionConf()
	// log.Printf("[State]: node = %v becomes Follower, current_term = %v \n", rf.me, rf.CurrentTerm)
	return
}

// SetCandidate set the peer into Candidate
func (rf *Raft) SetCandidate() {
	// 1. transitions to candidate state
	rf.state = CANDIDATE

	// 2. increments its current term
	rf.CurrentTerm++

	// 3. votes for itself
	rf.VotedFor = rf.me

	rf.persist()
	rf.InitElectionConf()
	// log.Printf("[State]: node = %v becomes Candidate, current_term = %v \n", rf.me, rf.CurrentTerm)
	return
}

// SetLeader set the peer into Leader
func (rf *Raft) SetLeader() {
	rf.state = LEADER

	rf.persist()
	rf.InitElectionConf()
	// log.Printf("[State]: node = %v becomes Leader, current_term = %v \n", rf.me, rf.CurrentTerm)
	return
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
		duration := time.Now().UnixNano()/int64(time.Millisecond) - rf.electionST
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
	rf.mu.Lock()
	rf.SetCandidate()

	var (
		wg      sync.WaitGroup
		relyCh  = make(chan RequestVoteReply, len(rf.peers))
		count   = 1
		voteReq = RequestVoteArgs{
			Term:         rf.CurrentTerm,
			CandidateID:  rf.me,
			LastLogTerm:  rf.Logs[len(rf.Logs)-1].Term,
			LastLogIndex: len(rf.Logs) - 1,
		}
	)
	rf.mu.Unlock()

	for idxPeer := range rf.peers {
		if idxPeer == rf.me {
			rf.InitElectionConf()
			continue
		}

		wg.Add(1)
		go func(server int, voteReq RequestVoteArgs) {
			defer wg.Done()

			voteResp := RequestVoteReply{}
			if response := rf.SendRPCHandler(server, "Raft.RequestVote", &voteReq, &voteResp); response == rpc.OK {
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
				go rf.Broadcast()
				go rf.CheckCommit()
				return
			}
		}

		// (b) results occur
		// If the leader’s term (another server claiming to be leader) is at least
		// as large as the candidate’s current term, then the `Candidate`
		// recognizes the leader as legitimate and returns to `Follower` state.
		if resp.Term > rf.CurrentTerm {
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
	// log.Println("WARNING: split vote!")
	rf.mu.Lock()
	rf.SetFollower(rf.CurrentTerm)
	rf.mu.Unlock()
	go rf.ElectionTimeOut()
}

// Broadcast leader broadcast to followers
// 1. send snapshot to compact log
// 2. send heartbeat and log replication
func (rf *Raft) Broadcast() {
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
				rf.mu.Lock()

				if rf.state != LEADER {
					rf.mu.Unlock()
					return
				}

				// log.Printf("broadcast: leader = %v, server  = %v, nextIndex = %v, matchIndex = %v, lastIncludedIndex = %v, logs = %v \n", rf.me, server, rf.nextIndex, rf.matchIndex, rf.LastIncludedIndex, rf.Logs)

				// log compaction
				if rf.nextIndex[server] <= rf.LastIncludedIndex {
					rf.mu.Unlock()
					rf.SendSnapshot(server)
					return
				}
				rf.mu.Unlock()
				rf.SendAppendEntries(server)
			}(idxPeer)

		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// SendAppendEntries leader sends followers heartbeat and replicates log
// 1. new `Leader` sends heartbeat messages to all of the other servers
// to establish its authority and prevent new elections.
// 2. leader sends followers log synchronization messages.
// in Raft, the leader handles inconsistencies by forcing the followers’ logs to duplicate its own.
func (rf *Raft) SendAppendEntries(server int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	// send initial empty AppendEntries RPCs (heartbeat) to each server;
	// repeat during idle periods to prevent election timeouts
	appendReq := AppendEntriesArgs{
		LeaderID:     rf.me,
		Term:         rf.CurrentTerm,
		LeaderCommit: rf.commitIndex,
		// PrevLogIndex: rf.nextIndex[server] - 1,
	}
	appendResp := AppendEntriesReply{}
	nextIndex := rf.nextIndex[server]

	// need to sync new logs to thie server
	if nextIndex > rf.LastIncludedIndex && nextIndex < len(rf.Logs)+rf.LastIncludedIndex {
		// log.Printf("need to sync new logs, server = %v, len(rf.logs) = %v, nextIndex = %v \n", server, len(rf.Logs), nextIndex)
		prevLogIndex := nextIndex - 1
		appendReq.PrevLogIndex = prevLogIndex
		appendReq.PrevLogTerm = rf.Logs[prevLogIndex-rf.LastIncludedIndex].Term
		appendReq.Logs = rf.Logs[nextIndex-rf.LastIncludedIndex:]
	}
	rf.mu.Unlock()

	if response := rf.SendRPCHandler(server, "Raft.AppendEntries", &appendReq, &appendResp); response == rpc.OK {
		rf.mu.Lock()
		if appendResp.Term > rf.CurrentTerm {
			rf.SetFollower(appendResp.Term)
			rf.mu.Unlock()
			go rf.ElectionTimeOut()
			return
		}

		if appendResp.ExistSnapshot {
			rf.mu.Unlock()
			return
		}

		if !appendResp.Success {
			rf.nextIndex[server]--
			// log.Println("-")
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = appendReq.PrevLogIndex + len(appendReq.Logs)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// log.Printf("[AppendEntries RPC]: leader = %v, server = %v, logs = %v, matchIndex | nextIndex = %v | %v, lastIncludedIndex = %v \n", rf.me, server, rf.Logs, rf.matchIndex, rf.nextIndex, rf.LastIncludedIndex)

		rf.mu.Unlock()
	} else {
		// log.Println("append fail!")
	}

}

// SendSnapshot send followers snapshot to compaction log
func (rf *Raft) SendSnapshot(server int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}

	// check again
	if rf.nextIndex[server] > rf.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	if response := rf.SendRPCHandler(server, "Raft.InstallSnapshot", &args, &reply); response == rpc.OK {
		rf.mu.Lock()
		if reply.Term > rf.CurrentTerm {
			rf.SetFollower(reply.Term)
			rf.mu.Unlock()
			go rf.ElectionTimeOut()
			return
		}

		if reply.IfCommitted {
			rf.matchIndex[server] = rf.LastIncludedIndex
			rf.nextIndex[server] = rf.LastIncludedIndex + 1
		}

		// log.Printf("Take Snapshot: leader = %v, server = %v, Logs = %v, matchIndex = %v, nextIndex = %v, lastIncludedIndex = %v \n", rf.me, server, rf.Logs, rf.matchIndex, rf.nextIndex, rf.LastIncludedIndex)
		rf.mu.Unlock()
	}
}

// CheckCommit The leader decides when it is safe to apply a log entry to the state machines;
// such an entry is called `committed`.
// Raft guarantees that committed entries are durable and will eventually be executed by all
// of the available state machines. A log entry is committed once the leader that created the entry
// has replicated it on a majority of the servers (e.g., entry 7 in Figure 6). This also commits
// all preceding entries in the leader’s log, including entries created by previous leaders.
func (rf *Raft) CheckCommit() {
	for true {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}

		// it also commits all preceding entries in the leader’s log,
		// including entries created by previous leaders.
		for index := rf.commitIndex + 1; index < len(rf.Logs)+rf.LastIncludedIndex; index++ {
			var count int
			if rf.Logs[index-rf.LastIncludedIndex].Term == rf.CurrentTerm {
				for server := range rf.peers {
					// already replicated
					if index <= rf.matchIndex[server] {
						count++
					}

					if count > len(rf.peers)/2 {
						rf.commitIndex = index
						// log.Printf("[CheckCommit]: log = %v, commitIndex = %v \n", rf.Logs[index], index)
						break
					}
				}
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// WaitCmdApplied check committed logs in real time and apply commands to the state machine
func (rf *Raft) WaitCmdApplied() {
	for true {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied > rf.LastIncludedIndex && rf.lastApplied-rf.LastIncludedIndex < len(rf.Logs) {
				applyEntry := ApplyMsg{
					CommandValid: true,
					Command:      rf.Logs[rf.lastApplied-rf.LastIncludedIndex].Command,
					CommandIndex: rf.lastApplied,
				}
				// log.Printf("[Apply]: mgs = %v \n", applyEntry)
				rf.applyCh <- applyEntry
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// persist save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() []byte {
	if len(rf.Logs) < 1 {
		return nil
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// from Figure2, raft shoule save these persistent state: rf.CurrentTerm, rf.VotedFor, rf.Logs
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	return data
}

// readPersist restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	var (
		currentTerm       int
		votedFor          int
		logs              []LogEntry
		lastIncludedIndex int
		lastIncludedTerm  int
	)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Printf("ERROR: ReadPersist() decode failed, unable to get server state! data = %v \n", string(data))
		return
	}
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.Logs = logs

	if d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Printf("ERROR: ReadPersist() decode failed, unable to get data about snapshot! data = %v \n", string(data))
		return
	}
	rf.LastIncludedIndex = lastIncludedIndex
	rf.LastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

// SaveSnapshot save snapshot and discard old log entries
func (rf *Raft) SaveSnapshot(index int, rawsnapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.LastIncludedIndex || index > rf.commitIndex {
		return
	}

	// discard log entries that precede the snapshot
	rf.Logs = rf.Logs[index-rf.LastIncludedIndex:]
	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = rf.Logs[0].Term

	// save raft state and snapshot
	raftstate := rf.persist()
	rf.persister.SaveStateAndSnapshot(raftstate, rawsnapshot)
}
