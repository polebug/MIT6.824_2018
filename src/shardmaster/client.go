package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	rpc "labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*rpc.ClientEnd

	// about client requests:
	id  int64 // client id
	seq int64 // request sequence number

	lastLeader int // the leader for the last RPC, send the next RPC to the server first.
}

// nrand generate client id randomly
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// MakeClerk create a new Clerk and initialize it
func MakeClerk(servers []*rpc.ClientEnd) *Clerk {
	clerk := new(Clerk)
	clerk.servers = servers
	clerk.id = nrand()
	clerk.seq = 0
	return clerk
}

// SendRPCHandler send RPC request
func (c *Clerk) SendRPCHandler(peer int, method string, args interface{}, reply interface{}) rpc.Code {
	var okCh = make(chan bool)

	go func() {
		var ok = c.servers[peer].Call(method, args, reply)
		okCh <- ok
		// log.Printf("[shardmaster] Send RPC, method = %v, args = %v \n", method, args)
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

// Query The Query RPC's argument is a configuration number. The shardmaster replies
// with the configuration that has that number. If the number is -1 or bigger than the
// biggest known configuration number, the shardmaster should reply with the latest
// configuration. The result of Query(-1) should reflect every Join, Leave, or Move RPC
// that the shardmaster finished handling before it received the Query(-1) RPC.
func (c *Clerk) Query(num int) Config {
	peer := c.lastLeader
	args := QueryArgs{
		Num: num,
		Client: ClientInfo{
			ID:  c.id,
			Seq: c.seq,
		},
	}
	c.seq++

	for {
		// try each known server.
		reply := QueryReply{}
		if ok := c.SendRPCHandler(peer, "ShardMaster.Query", &args, &reply); ok == rpc.OK {
			if reply.WrongLeader == false {
				c.lastLeader = peer
				return reply.Config
			}
		}
		peer = (peer + 1) % len(c.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// Join The Join RPC is used by an administrator to add new replica groups.
// Its argument is a set of mappings from unique, non-zero replica group identifiers
// (GIDs) to lists of server names. The shardmaster should react by creating a new
// configuration that includes the new replica groups. The new configuration should
// divide the shards as evenly as possible among the full set of groups, and should
// move as few shards as possible to achieve that goal. The shardmaster should allow
// reuse of a GID if it's not part of the current configuration (i.e. a GID should be
// allowed to Join, then Leave, then Join again).
func (c *Clerk) Join(servers map[int][]string) {
	peer := c.lastLeader
	args := JoinArgs{
		Servers: servers,
		Client: ClientInfo{
			ID:  c.id,
			Seq: c.seq,
		},
	}
	c.seq++

	for {
		// try each known server.
		reply := JoinReply{}
		if ok := c.SendRPCHandler(peer, "ShardMaster.Join", &args, &reply); ok == rpc.OK {
			if reply.WrongLeader == false {
				c.lastLeader = peer
				return
			}
		}
		peer = (peer + 1) % len(c.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// Leave The Leave RPC's argument is a list of GIDs of previously joined groups.
// The shardmaster should create a new configuration that does not include those groups,
// and that assigns those groups' shards to the remaining groups. The new configuration
// should divide the shards as evenly as possible among the groups, and should move as few
// shards as possible to achieve that goal.
func (c *Clerk) Leave(gids []int) {
	peer := c.lastLeader
	args := LeaveArgs{
		GIDs: gids,
		Client: ClientInfo{
			ID:  c.id,
			Seq: c.seq,
		},
	}
	c.seq++

	for {
		// try each known server.
		reply := LeaveReply{}
		if ok := c.SendRPCHandler(peer, "ShardMaster.Leave", &args, &reply); ok == rpc.OK {
			if reply.WrongLeader == false {
				c.lastLeader = peer
				return
			}
		}
		peer = (peer + 1) % len(c.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

// Move The Move RPC's arguments are a shard number and a GID. The shardmaster should create
// a new configuration in which the shard is assigned to the group. The purpose of Move is to
// allow us to test your software. A Join or Leave following a Move will likely un-do the
// Move, since Join and Leave re-balance.
func (c *Clerk) Move(shard int, gid int) {
	peer := c.lastLeader
	args := MoveArgs{
		Shard: shard,
		GID:   gid,
		Client: ClientInfo{
			ID:  c.id,
			Seq: c.seq,
		},
	}
	c.seq++

	for {
		// try each known server.
		reply := MoveReply{}
		if ok := c.SendRPCHandler(peer, "ShardMaster.Move", &args, &reply); ok == rpc.OK {
			if reply.WrongLeader == false {
				c.lastLeader = peer
				return
			}
		}
		peer = (peer + 1) % len(c.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
