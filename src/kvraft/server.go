package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const (
	Debug            = 0
	WaitAgreeTimeOut = 500
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ClientInfo struct {
	ID  int64 // client id
	Seq int64 // request sequence number
}

// Op is the type of sent to raft log
type Op struct {
	Type   string // get/put/append
	Key    string
	Value  string
	Client ClientInfo // record client request inforamtion to prevent repeated requests
}

type KVServer struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int               // snapshot if log grows this big
	db           map[string]string // kv
	lastSeqTable map[int64]int64   // client latest request seq, igonre old requests
	agreeCh      map[int]chan Op   // agreed op
}

type GetArgs struct {
	Key    string
	Client ClientInfo
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Op     string // "Put" or "Append"
	Client ClientInfo
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

// isEqual compare whether the latest agreed command between kvservers and the command requested by the client.
func (kv *KVServer) isEqual(agreeCmd, rq Op) bool {
	return agreeCmd.Type == rq.Type && agreeCmd.Key == rq.Key && agreeCmd.Value == rq.Value && agreeCmd.Client == rq.Client
}

// Get kvserver handles Clerk's `Get` request
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Type:   "Get",
		Key:    args.Key,
		Client: args.Client,
	}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-kv.GetAgreedCmd(index):
		// log.Printf("agreed cmd = %v \n", op)
		if kv.isEqual(op, command) {
			reply.WrongLeader = false
			kv.mu.Lock()
			reply.Value = kv.db[args.Key]
			kv.mu.Unlock()
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		// log.Printf("cmd agree timeout! cmd = %v, logIndx = %v \n", command, logIndex)
		return
	}
}

// PutAppend kvserver handles Clerk's `Put` or `Append` request
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{
		Type:   args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Client: args.Client,
	}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-kv.GetAgreedCmd(index):
		// log.Printf("agreed cmd = %v \n", op)
		if kv.isEqual(op, command) {
			reply.WrongLeader = false
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		// log.Printf("cmd agree timeout! cmd = %v, logIndx = %v \n", command, logIndex)
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// getAgreedCmd
func (kv *KVServer) GetAgreedCmd(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.agreeCh[index]; !ok {
		kv.agreeCh[index] = make(chan Op, 1)
	}
	return kv.agreeCh[index]
}

// UpdateAgreedCmd
func (kv *KVServer) UpdateAgreedCmd(index int, op Op) {
	kv.GetAgreedCmd(index) <- op
	// log.Printf("update agree ch = %v \n", op)
}

// UpdateDB
func (kv *KVServer) UpdateDB() {
	for {
		if msg := <-kv.applyCh; msg.CommandValid {
			if msg.CommandValid == false {
				continue
			}
			// log.Printf("apply cmd = %v \n", msg)
			op := msg.Command.(Op)

			kv.mu.Lock()
			if seq, ok := kv.lastSeqTable[op.Client.ID]; !ok || seq < op.Client.Seq {
				kv.lastSeqTable[op.Client.ID] = op.Client.Seq
				switch op.Type {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
			}
			kv.mu.Unlock()

			kv.UpdateAgreedCmd(msg.CommandIndex, op)
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)

	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.lastSeqTable = make(map[int64]int64)
	kv.agreeCh = make(map[int]chan Op)
	kv.mu.Unlock()

	go kv.UpdateDB()

	return kv
}
