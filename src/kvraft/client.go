package raftkv

import (
	"crypto/rand"
	rpc "labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	// about client requests:
	id  int64 // client id
	seq int64 // request sequence number

	// about raft peers:
	servers    []*rpc.ClientEnd // raft peers
	lastLeader int              // the leader for the last RPC, send the next RPC to that server first
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

// Get the value of key
func (c *Clerk) Get(key string) string {
	var (
		peer       = c.lastLeader
		clientInfo = ClientInfo{ID: c.id, Seq: c.seq}
		args       = GetArgs{Key: key, Client: clientInfo}
	)
	c.seq++

	for {
		reply := GetReply{}
		if ok := c.SendRPCHandler(peer, "KVServer.Get", &args, &reply); ok == rpc.OK {
			if !reply.WrongLeader {
				c.lastLeader = peer
				return reply.Value
			}
		}
		peer = (peer + 1) % len(c.servers)
	}
}

// PutAppend shared by Put and Append.
func (c *Clerk) PutAppend(key string, value string, op string) {
	var (
		peer       = c.lastLeader
		clientInfo = ClientInfo{ID: c.id, Seq: c.seq}
		args       = PutAppendArgs{Key: key, Value: value, Op: op, Client: clientInfo}
	)
	c.seq++

	for {
		reply := PutAppendReply{}
		if ok := c.SendRPCHandler(peer, "KVServer.PutAppend", &args, &reply); ok == rpc.OK {
			if !reply.WrongLeader {
				c.lastLeader = peer
				return
			}
		}
		peer = (peer + 1) % len(c.servers)
	}
}

// Put override the value of key
func (c *Clerk) Put(key string, value string) {
	c.PutAppend(key, value, "Put")
}

// Append append after the original value of key
func (c *Clerk) Append(key string, value string) {
	c.PutAppend(key, value, "Append")
}
