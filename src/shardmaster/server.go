package shardmaster

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

const (
	WaitAgreeTimeOut = 500
)

type Err string

// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	alive   chan bool

	configs      []Config        // indexed by config num
	agreeCh      map[int]chan Op // agreed operations
	lastSeqTable map[int64]int64 // client latest request seq, igonre old requests
}

type ClientInfo struct {
	ID  int64 // client id
	Seq int64 // request sequence number
}

type Op struct {
	Type   string      // Join/Leave/Move/Query
	Args   interface{} // QueryArgs/JoinArgs/LeaveArgs/MoveArgs
	Client ClientInfo  // record client request inforamtion to prevent repeated requests
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Client  ClientInfo
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs   []int
	Client ClientInfo
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard  int
	GID    int
	Client ClientInfo
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num    int // desired config number
	Client ClientInfo
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

// Join RPC
// Join(servers) -- add a set of groups (gid -> server-list mapping).
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	command := Op{
		Type: "Join",
		Args: JoinArgs{
			Servers: args.Servers,
		},
		Client: args.Client,
	}

	reply.WrongLeader = true
	index, _, isLeader := sm.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-sm.getAgreedCmd(index):
		if sm.isEqual(op, command) {
			reply.WrongLeader = false
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		// log.Printf("cmd agree timeout! cmd = %v, logIndx = %v \n", command, logIndex)
		return
	}
}

// Leave RPC
// Leave(gids) -- delete a set of groups.
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	command := Op{
		Type: "Leave",
		Args: LeaveArgs{
			GIDs: args.GIDs,
		},
		Client: args.Client,
	}

	reply.WrongLeader = true
	index, _, isLeader := sm.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-sm.getAgreedCmd(index):
		if sm.isEqual(op, command) {
			reply.WrongLeader = false
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		return
	}
}

// Move RPC
// Move(shard, gid) -- hand off one shard from current owner to gid.
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	command := Op{
		Type: "Move",
		Args: MoveArgs{
			GID:   args.GID,
			Shard: args.Shard,
		},
		Client: args.Client,
	}

	reply.WrongLeader = true
	index, _, isLeader := sm.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-sm.getAgreedCmd(index):
		if sm.isEqual(op, command) {
			reply.WrongLeader = false
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		return
	}
}

// Query RPC
// Query(num) -> fetch Config # num, or latest config if num==-1.
// he shardmaster replies with the configuration that has that number. If the number is -1
// or bigger than the biggest known configuration number, the shardmaster should reply
// with the latest configuration. The result of Query(-1) should reflect every Join, Leave,
// or Move RPC that the shardmaster finished handling before it received the Query(-1) RPC.
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	command := Op{
		Type: "Query",
		Args: QueryArgs{
			Num: args.Num,
		},
		Client: args.Client,
	}

	reply.WrongLeader = true
	index, _, isLeader := sm.rf.Start(command)
	if !isLeader {
		return
	}

	select {
	case op := <-sm.getAgreedCmd(index):
		if sm.isEqual(op, command) {
			sm.mu.Lock()
			defer sm.mu.Unlock()

			reply.WrongLeader = false
			if args.Num >= 0 && args.Num < len(sm.configs) {
				reply.Config = sm.configs[args.Num]
			} else {
				reply.Config = sm.configs[len(sm.configs)-1]
			}
			return
		}
	case <-time.After(WaitAgreeTimeOut * time.Millisecond):
		// log.Printf("cmd agree timeout! cmd = %v, logIndx = %v \n", command, logIndex)
		return
	}

}

// isEqual compare whether the latest agreed command between shardmaster and the command requested by the client.
func (sm *ShardMaster) isEqual(agreeCmd, rq Op) bool {
	return agreeCmd.Type == rq.Type && agreeCmd.Client == rq.Client
}

// getAgreedCmd get consistent command
func (sm *ShardMaster) getAgreedCmd(index int) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.agreeCh[index]; !ok {
		sm.agreeCh[index] = make(chan Op, 1)
	}
	return sm.agreeCh[index]
}

// updateAgreedCmd save consistent command from raft
func (sm *ShardMaster) updateAgreedCmd(index int, op Op) {
	sm.getAgreedCmd(index) <- op
	// log.Printf("update agree ch = %v \n", op)
}

// waitApplyCh wait for raft nodes to be consistent
func (sm *ShardMaster) waitApplyCh() {
	select {
	case state := <-sm.alive:
		if state == false {
			return
		}
	case msg := <-sm.applyCh:
		if msg.CommandValid == false {
			return
		}

		sm.mu.Lock()
		op := msg.Command.(Op)
		if seq, ok := sm.lastSeqTable[op.Client.ID]; !ok || seq < op.Client.Seq {
			sm.lastSeqTable[op.Client.ID] = op.Client.Seq

			switch op.Type {
			case "Join":
				sm.joinConfigs(op.Args.(JoinArgs))
			case "Leave":
				sm.leaveConfigs(op.Args.(LeaveArgs))
			case "Move":
				sm.moveConfigs(op.Args.(MoveArgs))
			case "Query":
			default:
				panic("unknown method")
			}
		}
		sm.mu.Unlock()

		sm.updateAgreedCmd(msg.CommandIndex, op)
	}
}

// joinConfigs the shardmaster should react by creating a new configuration that includes
// the new replica groups. The new configuration should divide the shards as evenly as
// possible among the full set of groups, and should move as few shards as possible to
// achieve that goal. The shardmaster should allow reuse of a GID if it's not part of the
// current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
func (sm *ShardMaster) joinConfigs(args JoinArgs) {
	newConfig := sm.createNewConfig(sm.configs)

	// join
	for gid, serverList := range args.Servers {
		newConfig.Groups[gid] = serverList
	}

	// rebalance
	newConfig = sm.rebalanceLoad(newConfig)
	sm.configs = append(sm.configs, newConfig)

	// log.Printf("[shardmaster|join] configs = %v \n", sm.configs)
}

// leaveConfigs the shardmaster should create a new configuration that does not include those groups, and that
// assigns those groups' shards to the remaining groups. The new configuration should divide the shards as
// evenly as possible among the groups, and should move as few shards as possible to achieve that goal.
func (sm *ShardMaster) leaveConfigs(args LeaveArgs) {
	newConfig := sm.createNewConfig(sm.configs)

	// leave
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}

	// rebalance
	newConfig = sm.rebalanceLoad(newConfig)
	sm.configs = append(sm.configs, newConfig)

	// log.Printf("[shardmaster|leave] configs = %v \n", sm.configs)
}

// moveConfigs The shardmaster should create a new configuration in which the shard is assigned to the group. A Join or Leave following a Move will likely undo the Move,
// since Join and Leave rebalance.
func (sm *ShardMaster) moveConfigs(args MoveArgs) {
	newConfig := sm.createNewConfig(sm.configs)

	// move
	newConfig.Shards[args.Shard] = args.GID

	sm.configs = append(sm.configs, newConfig)

	// log.Printf("[shardmaster|move] configs = %v \n", sm.configs)
}

// createNewConfig create a new version of config
func (sm *ShardMaster) createNewConfig(configList []Config) Config {
	currentConfig := configList[len(configList)-1]
	newConfig := Config{
		Num: currentConfig.Num + 1,
	}
	newConfig.Shards = [NShards]int{} // shard -> gid
	for i, gid := range currentConfig.Shards {
		newConfig.Shards[i] = gid
	}
	newConfig.Groups = make(map[int][]string) // GID -> servers mappings
	for gid, servers := range currentConfig.Groups {
		newConfig.Groups[gid] = servers
	}
	return newConfig
}

// rebalanceLoad rebalance shard load
func (sm *ShardMaster) rebalanceLoad(newConfig Config) Config {
	var gidsList []int
	for gid := range newConfig.Groups {
		gidsList = append(gidsList, gid)
	}

	if len(gidsList) > 0 {
		for i := 0; i < NShards; i++ {
			newConfig.Shards[i] = gidsList[i%len(newConfig.Groups)]
		}
	}

	return newConfig
}

// Kill the tester calls Kill() when a ShardMaster instance won't be needed again.
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.alive <- false
}

// Raft needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// StartServer initial shardmaster instance
// servers[] contains the ports of the set of servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.alive = make(chan bool, 1)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.lastSeqTable = make(map[int64]int64)
	sm.agreeCh = make(map[int]chan Op)

	go func() {
		for {
			sm.waitApplyCh()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return sm
}
