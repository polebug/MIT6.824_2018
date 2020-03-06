package raft

import (
	"labrpc"
	// "log"
)

// GetState return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	// log.Printf("[GetState] node = %v, term = %v, state = %v \n", rf.me, term, rf.state)
	return term, isleader
}

// Start lab 3 k/v server’s RPC handlers(e.g. Put/Get) call Start().
// start Raft agreement on a new log entry. Start() returns immediately —
// RPC handler must then wait for commit.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// even if the Raft instance has been killed, this function should return gracefully.
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if isLeader = (rf.state == LEADER); isLeader {
		term = rf.currentTerm
		index = len(rf.logs) // initialized to 1
		rf.logs = append(rf.logs, LogEntry{
			Command: command,
			Term:    term,
		})
		rf.matchIndex[rf.me] = len(rf.logs) - 1
		rf.nextIndex[rf.me] = len(rf.logs)
	}

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

// Make create a Raft server
func Make(
	peers []*labrpc.ClientEnd, // the ports of all the Raft servers, all the servers' peers[] arrays have the same order
	me int, // this server's port is peers[me]
	persister *Persister, // is a place for this server to save its persistent state, and also initially holds the most recent saved state
	applyCh chan ApplyMsg, // a channel on which the tester or service expects Raft to send ApplyMsg messages
) *Raft {
	// initialization
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.SetFollower(0)
	rf.logs = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
	}
	rf.mu.Unlock()

	// create a background goroutine that will kick off leader election periodically
	go rf.ElectionTimeOut()
	// check committed logs in real time and apply commands to the state machined
	go rf.WaitCmdApplied()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
