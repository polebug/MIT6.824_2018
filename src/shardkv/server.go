package shardkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

type ClientInfo struct {
	ID  int64 // client id
	Seq int64 // request sequence number
}

type GroupInfo struct {
	GID int
	Seq int
}

type KV struct {
	Key   string
	Value string
}

type Migrate struct {
	Shard int
	// Data      map[string]string
	ConfigNum int
	Test      int
}

type Op struct {
	Type      string // Get/Put/Append/RequestMigrate/SendMigrate
	Kv        KV
	Migrate   Migrate
	Data      map[string]string
	Client    ClientInfo
	GroupInfo GroupInfo
}

type ShardKV struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	make_end  func(string) *labrpc.ClientEnd
	persister *raft.Persister

	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	DB              map[string]string
	RequestSeqTable map[int64]int64 // client -> seq
	agreeCh         map[int]chan Op

	Config          shardmaster.Config // current config
	Shards          map[int]int        // current shards -> version
	mck             *shardmaster.Clerk // Query
	MigrateSeqTable map[int]int        // gid -> seq
}

// RPC interface:
// Get
// Put or Append
// RequestMigrate
// SendMigrate

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

type GetArgs struct {
	Key    string
	Client ClientInfo
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type RequestMigrateArgs struct {
	Shard     int
	FromGID   int
	ToGID     int
	ConfigNum int
	GroupInfo GroupInfo
}

type RequestMigrateReply struct {
	Data        map[string]string
	WrongLeader bool
	GID         int
	Err         string
}

type SendMigrateArgs struct {
	Data      map[string]string
	Shard     int
	FromGID   int
	ToGID     int
	ConfigNum int
	GroupInfo GroupInfo
}

type SendMigrateReply struct {
	Err         string
	GID         int
	WrongLeader bool
}

// Get RPC
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	command := Op{
		Type: "Get",
		Kv: KV{
			Key: args.Key,
		},
		Client: args.Client,
	}
	reply.WrongLeader = true

	// judge whether the server need to manage the shard where the key is located
	// if not, the server refuse to serve
	if kv.matchGroup(args.Key) == false {
		reply.Err = ErrWrongGroup
		return
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-kv.getAgreedCmd(index):
		if kv.isEqual(op, command) {
			reply.WrongLeader = false
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = op.Kv.Value
			kv.mu.Unlock()
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		return
	}

}

// PutAppend RPC
// handles Clerk's `Put` or `Append` request
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	command := Op{
		Type: args.Op,
		Kv: KV{
			Key:   args.Key,
			Value: args.Value,
		},
		Client: args.Client,
	}
	reply.WrongLeader = true

	if kv.matchGroup(args.Key) == false {
		// log.Println("wrong group")
		reply.Err = ErrWrongGroup
		return
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-kv.getAgreedCmd(index):
		if kv.isEqual(op, command) {
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		return
	}
}

// RequestMigrate RPC
// request shard data from other group
func (kv *ShardKV) RequestMigrate(args *RequestMigrateArgs, reply *RequestMigrateReply) {
	command := Op{
		Type: "RequestMigrate",
		Migrate: Migrate{
			Shard:     args.Shard,
			ConfigNum: args.ConfigNum,
		},
		GroupInfo: args.GroupInfo,
	}
	reply.WrongLeader = true

	if args.ConfigNum >= kv.Config.Num {
		return
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-kv.getAgreedCmd(index):
		if kv.isEqual(op, command) {
			reply.Err = OK
			reply.GID = kv.gid
			reply.Data = op.Data
			reply.WrongLeader = false
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		// fmt.Println("timeout!")
		return
	}
}

// SendMigrate RPC
func (kv *ShardKV) SendMigrate(args *SendMigrateArgs, reply *SendMigrateReply) {
	command := Op{
		Type: "SendMigrate",
		Migrate: Migrate{
			Shard:     args.Shard,
			ConfigNum: args.ConfigNum,
		},
		Data:      args.Data,
		GroupInfo: args.GroupInfo,
	}
	reply.WrongLeader = true

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-kv.getAgreedCmd(index):
		if kv.isEqual(op, command) {
			reply.Err = OK
			reply.GID = kv.gid
			reply.WrongLeader = false
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		return
	}
}

// doSendMigrate `gid` gains this shard, kv.gid should send data to `gid`
func (kv *ShardKV) doSendMigrate(shard int, nowConfig shardmaster.Config) {
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.mu.Unlock()
		return
	}

	toGID := nowConfig.Shards[shard]
	kv.MigrateSeqTable[toGID]++
	args := SendMigrateArgs{
		FromGID:   kv.gid,
		ToGID:     toGID,
		Shard:     shard,
		ConfigNum: nowConfig.Num,
		GroupInfo: GroupInfo{
			GID: toGID,
			Seq: kv.MigrateSeqTable[toGID],
		},
	}
	args.Data = make(map[string]string)
	for k, v := range kv.DB {
		if s := key2shard(k); s == shard {
			args.Data[k] = v
		}
	}

	if kv.Config.Num != nowConfig.Num {
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()

	for {
		if servers, ok := kv.Config.Groups[toGID]; ok {
			for _, peer := range servers {
				reply := SendMigrateReply{}
				if ok := kv.SendRPCHandler(peer, "ShardKV.SendMigrate", &args, &reply); ok == labrpc.OK {
					if reply.WrongLeader == false && reply.Err == OK {
						log.Printf("[shardkv|migrate] server(%v) Send RPC, method = SendMigrate, args = %v, reply = %v \n", peer, args, reply)

						if _, ok := kv.Shards[args.Shard]; !ok {
							delete(kv.Shards, args.Shard)
						}
						return
					}
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// doRequestMigrate
func (kv *ShardKV) doRequestMigrate(shard int, oldConfig shardmaster.Config) {
	kv.mu.Lock()
	toGID := oldConfig.Shards[shard]
	kv.MigrateSeqTable[toGID]++
	args := RequestMigrateArgs{
		Shard:     shard,
		FromGID:   kv.gid,
		ToGID:     toGID,
		ConfigNum: oldConfig.Num,
		GroupInfo: GroupInfo{
			GID: toGID,
			Seq: kv.MigrateSeqTable[toGID],
		},
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	for {
		if servers, ok := oldConfig.Groups[toGID]; ok {
			for _, peer := range servers {
				reply := RequestMigrateReply{}
				if ok := kv.SendRPCHandler(peer, "ShardKV.RequestMigrate", &args, &reply); ok == labrpc.OK {
					if reply.WrongLeader == false && reply.Err == OK {
						log.Printf("[shardkv|migrate] server(%v) Send RPC, method = RequestMigrate, args = %v, reply = %v \n", peer, args, reply)

						if num, ok := kv.Shards[args.Shard]; !ok || num < args.ConfigNum {
							kv.Shards[args.Shard] = args.ConfigNum
							// append data
							for k, v := range reply.Data {
								kv.DB[k] = v
							}
							log.Printf("request migrate data after, server(%v) db = %v \n", kv.me, kv.DB)
						}
						return
					}
				}
			}
		}
	}
}

// refreshConfig
func (kv *ShardKV) refreshConfig() {
	for {
		time.Sleep(50 * time.Millisecond)

		oldConfig := kv.Config
		oldShard := make(map[int]bool)
		for shard, gid := range oldConfig.Shards {
			if gid == kv.gid {
				oldShard[shard] = true
			}
		}
		// kv.shards = make(map[int]bool)
		kv.Config = kv.mck.Query(oldConfig.Num + 1)

		// log.Printf("old config = %v, new config = %v, num = %v \n", oldConfig, kv.config, oldConfig.Num+1)

		for shard, gid := range kv.Config.Shards {
			if gid != kv.gid {
				continue
			}

			if _, ok := oldShard[shard]; ok || oldConfig.Num == 0 {
				kv.Shards[shard] = kv.Config.Num
				delete(oldShard, shard)
			} else {
				// kv.gid gains new shard
				// need to request data from the original owner
				kv.doRequestMigrate(shard, oldConfig)
			}
		}

		// kv.gid managed these shard last time, but now it is not
		// need to send these shard data
		if len(oldShard) > 0 {
			for shard := range oldShard {
				go kv.doSendMigrate(shard, kv.Config)
			}
		}
	}
}

// isEqual compare whether the latest agreed command between shardmaster and the command requested by the client.
func (kv *ShardKV) isEqual(agreeCmd, rq Op) bool {
	return agreeCmd.Type == rq.Type && agreeCmd.Client == rq.Client && agreeCmd.GroupInfo == rq.GroupInfo
}

// matchGroup judge whether the server need to manage the shard where the key is located
func (kv *ShardKV) matchGroup(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(key)
	shard2gid := kv.Config.Shards[shard]
	_, ok := kv.Shards[shard]
	if shard2gid == kv.gid && (ok || kv.Config.Num == 0) {
		return true
	}
	fmt.Printf("shard = %v, shard2gid = %v, gid = %v, shard list = %v \n", shard, shard2gid, kv.gid, kv.Shards)
	return false
}

// getAgreedCmd get consistent command
func (kv *ShardKV) getAgreedCmd(index int) chan Op {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	if _, ok := kv.agreeCh[index]; !ok {
		kv.agreeCh[index] = make(chan Op, 1)
	}
	return kv.agreeCh[index]
}

// updateAgreedCmd save consistent command from raft
func (kv *ShardKV) updateAgreedCmd(index int, op Op) {
	// log.Printf("update agree ch = %v \n", op)
	kv.getAgreedCmd(index) <- op
}

// waitApplyCh wait for raft nodes to be consistent
func (kv *ShardKV) waitApplyCh() {
	for {
		msg := <-kv.applyCh
		if msg.CommandValid == false {
			kv.readSnapshot(msg.Command.([]byte))
			continue
		}

		kv.mu.Lock()
		op := msg.Command.(Op)
		switch op.Type {
		case "Put":
			if seq, ok := kv.RequestSeqTable[op.Client.ID]; !ok || seq < op.Client.Seq {
				kv.RequestSeqTable[op.Client.ID] = op.Client.Seq
				kv.DB[op.Kv.Key] = op.Kv.Value
			}
		case "Append":
			if seq, ok := kv.RequestSeqTable[op.Client.ID]; !ok || seq < op.Client.Seq {
				kv.RequestSeqTable[op.Client.ID] = op.Client.Seq
				kv.DB[op.Kv.Key] += op.Kv.Value
			}
		case "Get":
			if seq, ok := kv.RequestSeqTable[op.Client.ID]; !ok || seq < op.Client.Seq {
				kv.RequestSeqTable[op.Client.ID] = op.Client.Seq
				op.Kv.Value = kv.DB[op.Kv.Key]
			}
		case "SendMigrate":
			if seq, ok := kv.MigrateSeqTable[op.GroupInfo.GID]; !ok || seq < op.GroupInfo.Seq {
				kv.MigrateSeqTable[op.GroupInfo.GID] = op.GroupInfo.Seq

				// install migrate data
				if num, ok := kv.Shards[op.Migrate.Shard]; !ok || num < op.Migrate.ConfigNum {
					kv.Shards[op.Migrate.Shard] = op.Migrate.ConfigNum
					// append data
					for k, v := range op.Data {
						kv.DB[k] = v
					}
					log.Printf("install migrate data, server(%v) db = %v \n", kv.me, kv.DB)
				}
			}
		case "RequestMigrate":
			if seq, ok := kv.MigrateSeqTable[op.GroupInfo.GID]; !ok || seq < op.GroupInfo.Seq {
				kv.MigrateSeqTable[op.GroupInfo.GID] = op.GroupInfo.Seq

				data := make(map[string]string)
				for k, v := range kv.DB {
					if op.Migrate.Shard == key2shard(k) {
						data[k] = v
					}
				}
				op.Data = data
			}
		default:
			panic("unknown method")
		}

		kv.mu.Unlock()

		kv.updateAgreedCmd(msg.CommandIndex, op)

		// check raft log size
		kv.checkRaftSize(msg.CommandIndex)

		time.Sleep(10 * time.Millisecond)
	}
}

// CheckRaftSize compare `maxraftstate` to `persister.RaftStateSize()`. Whenever kv server detects that
// the Raft state size is approaching this threshold, it will save a snapshot.
func (kv *ShardKV) checkRaftSize(index int) {
	kv.mu.Lock()
	// if maxraftstate is -1, you do not have to snapshot
	if kv.maxraftstate == -1 {
		kv.mu.Unlock()
		return
	}

	if kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.mu.Unlock()
		kv.saveSnapshot(index)
		return
	}
	kv.mu.Unlock()
}

// saveSnapshot save snapshot
func (kv *ShardKV) saveSnapshot(index int) {
	kv.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.DB)
	e.Encode(kv.RequestSeqTable)
	e.Encode(kv.Config)
	e.Encode(kv.Shards)
	e.Encode(kv.MigrateSeqTable)
	kv.mu.Unlock()

	// tell Raft that it can save snapshot and discard old log entries
	kv.rf.SaveSnapshot(index, w.Bytes())
}

// readSnapshot read snapshot
func (kv *ShardKV) readSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	var (
		db              map[string]string
		requestSeqTable map[int64]int64
		config          shardmaster.Config
		shards          map[int]int
		migrateSeqTable map[int]int
	)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&db) != nil || d.Decode(&requestSeqTable) != nil || d.Decode(&config) != nil ||
		d.Decode(&shards) != nil || d.Decode(&migrateSeqTable) != nil {
		log.Printf("ERROR: ReadSnapshot() decode failed! data = %v \n", string(snapshot))
		return
	}

	kv.DB = db
	kv.RequestSeqTable = requestSeqTable
	kv.Config = config
	kv.Shards = shards
	kv.MigrateSeqTable = migrateSeqTable
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// log.Println("Kill")
}

// SendRPCHandler send RPC request
func (kv *ShardKV) SendRPCHandler(server string, method string, args interface{}, reply interface{}) labrpc.Code {
	var okCh = make(chan bool)

	go func() {
		srv := kv.make_end(server)
		var ok = srv.Call(method, args, reply)
		// log.Printf("[shardkv|migrate] server(%v) Send RPC, method = %v, args = %v, reply = %v \n", server, method, args, reply)
		okCh <- ok
	}()

	select {
	case <-time.After(labrpc.RPC_TIMEOUT):
		return labrpc.DEADLINE_EXCEEDED
	case ok := <-okCh:
		if ok {
			return labrpc.OK
		}
		return labrpc.UNAVAILABLE
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Migrate{})

	kv := new(ShardKV)

	kv.mu.Lock()
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.DB = make(map[string]string)
	kv.RequestSeqTable = make(map[int64]int64)
	kv.agreeCh = make(map[int]chan Op)

	kv.Config = shardmaster.Config{}
	kv.Shards = make(map[int]int)
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.MigrateSeqTable = make(map[int]int)
	kv.mu.Unlock()

	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.refreshConfig()

	go kv.waitApplyCh()

	return kv
}
