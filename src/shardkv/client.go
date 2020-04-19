package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"labrpc"
	"log"
	"shardmaster"
	"time"
)

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd

	// about client requests:
	id  int64 // client id
	seq int64 // request sequence number
}

// MakeClerk create a new Clerk and initialize it
// masters[] is needed to call shardmaster.MakeClerk().
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpck.ClientEnd on which you can
// send RPCs.
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	clerk := new(Clerk)
	clerk.sm = shardmaster.MakeClerk(masters)
	clerk.make_end = make_end

	clerk.id = nrand()
	clerk.seq = 0
	return clerk
}

// SendRPCHandler send RPC request
func (ck *Clerk) SendRPCHandler(server string, method string, args interface{}, reply interface{}) labrpc.Code {
	var okCh = make(chan bool)

	go func() {
		srv := ck.make_end(server)
		var ok = srv.Call(method, args, reply)
		log.Printf("[shardkv|client] Server(%v) Send RPC, method = %v, args = %v, reply = %v \n", server, method, args, reply)
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

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
		Client: ClientInfo{
			ID:  ck.id,
			Seq: ck.seq,
		},
	}
	ck.seq++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]

		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for peer := 0; peer < len(servers); peer++ {
				reply := GetReply{}
				if ok := ck.SendRPCHandler(servers[peer], "ShardKV.Get", &args, &reply); ok == labrpc.OK {
					if reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
						return reply.Value
					}
					if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask shardmaster for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// PutAppend shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Client: ClientInfo{
			ID:  ck.id,
			Seq: ck.seq,
		},
	}
	ck.seq++

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for peer := 0; peer < len(servers); peer++ {
				reply := PutAppendReply{}
				if ok := ck.SendRPCHandler(servers[peer], "ShardKV.PutAppend", &args, &reply); ok == labrpc.OK {
					if reply.WrongLeader == false && reply.Err == OK {
						return
					}
					if reply.Err == ErrWrongGroup {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask shardmaster for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

// Put
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
