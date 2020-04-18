package raftkv

import (
	"bytes"
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
	persister    *raft.Persister
	maxraftstate int             // snapshot if log grows this big
	agreeCh      map[int]chan Op // agreed op

	// persist:
	DB           map[string]string // kv
	LastSeqTable map[int64]int64   // client latest request seq, igonre old requests
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
			reply.Value = kv.DB[args.Key]
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

// Kill the tester calls Kill() when a KVServer instance won't
// be needed again.
func (kv *KVServer) Kill() {
	// log.Println("kv kill")
	kv.rf.Kill()
}

// GetAgreedCmd get consistent command
func (kv *KVServer) GetAgreedCmd(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.agreeCh[index]; !ok {
		kv.agreeCh[index] = make(chan Op, 1)
	}
	return kv.agreeCh[index]
}

// UpdateAgreedCmd save consistent command from raft
func (kv *KVServer) UpdateAgreedCmd(index int, op Op) {
	kv.GetAgreedCmd(index) <- op
	// log.Printf("update agree ch = %v \n", op)
}

// WaitApplyCh wait for raft nodes to be consistent
func (kv *KVServer) WaitApplyCh() {
	for {
		kv.mu.Lock()

		msg := <-kv.applyCh
		if msg.CommandValid == false {
			kv.mu.Unlock()
			kv.ReadSnapshot(msg.Command.([]byte))
			continue
		}
		// log.Printf("kv apply cmd = %v \n", msg)
		op := msg.Command.(Op)

		if seq, ok := kv.LastSeqTable[op.Client.ID]; !ok || seq < op.Client.Seq {
			kv.LastSeqTable[op.Client.ID] = op.Client.Seq
			switch op.Type {
			case "Put":
				kv.DB[op.Key] = op.Value
			case "Append":
				kv.DB[op.Key] += op.Value
			case "Get":
			default:
				panic("unknown method")
			}
		}
		kv.mu.Unlock()

		kv.UpdateAgreedCmd(msg.CommandIndex, op)

		// check raft log size
		kv.CheckRaftSize(msg.CommandIndex)

		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

// CheckRaftSize compare `maxraftstate` to `persister.RaftStateSize()`. Whenever kv server detects that
// the Raft state size is approaching this threshold, it will save a snapshot.
func (kv *KVServer) CheckRaftSize(index int) {
	kv.mu.Lock()
	// if maxraftstate is -1, you do not have to snapshot
	if kv.maxraftstate == -1 {
		kv.mu.Unlock()
		return
	}

	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.mu.Unlock()
		kv.SaveSnapshot(index)
		return
	}
	kv.mu.Unlock()
}

// SaveSnapshot save snapshot
func (kv *KVServer) SaveSnapshot(index int) {
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.DB); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.LastSeqTable); err != nil {
		panic(err)
	}
	kv.mu.Unlock()

	// tell Raft that it can save snapshot and discard old log entries
	kv.rf.SaveSnapshot(index, w.Bytes())
}

// ReadSnapshot read snapshot
func (kv *KVServer) ReadSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var (
		db           map[string]string
		lastSeqTable map[int64]int64
	)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&db) != nil || d.Decode(&lastSeqTable) != nil {
		log.Printf("ERROR: ReadSnapshot() decode failed! data = %v \n", string(snapshot))
		return
	}

	kv.DB = db
	kv.LastSeqTable = lastSeqTable
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
	kv.persister = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.DB = make(map[string]string)
	kv.LastSeqTable = make(map[int64]int64)
	kv.agreeCh = make(map[int]chan Op)
	kv.mu.Unlock()

	kv.ReadSnapshot(kv.persister.ReadSnapshot())
	go kv.WaitApplyCh()

	return kv
}
