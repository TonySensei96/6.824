package shardmaster

//
// Shardmaster clerk.
//

import (
	"labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

const RetryInterval = time.Duration(30 * time.Millisecond)

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// Your data here.
	id 			int64 // Unique identifier for the client.
	seqNum		int	  // Monotonically increasing sequence number for the client's request.
	lastLeader 	int   // Memorizing the last leader who is serving in the raft for the client.
	mu          sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand() // Random number for the client's unique identifier.
	ck.seqNum = 0 // initial sequence number for each operation.
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num}  // Initializes a new reply object.
	ck.mu.Lock()
	index := ck.lastLeader
	ck.mu.Unlock()
	for {
		var reply QueryReply // Unique reply from each server.
		// If there is a reply from the server and the reply implies correct leader,
		// then it means the reply is positive and just reply the configuration.
		if ck.servers[index].Call("ShardMaster.Query", &args, &reply) && !reply.WrongLeader {
			ck.mu.Lock()
			ck.lastLeader = index
			ck.mu.Unlock()
			return reply.Config
		}
		index = (index + 1) % len(ck.servers) // Try next server.
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	args := JoinArgs{
		Servers: 	servers,
		Cid: 		ck.id,
		SeqNum: 	ck.seqNum,
	}
	ck.seqNum++
	index := ck.lastLeader
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply JoinReply
		if ck.servers[index].Call("ShardMaster.Join", &args, &reply) && !reply.WrongLeader {
			ck.mu.Lock()
			ck.lastLeader = index
			ck.mu.Unlock()
			return
		}
		index = (index + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	args := LeaveArgs{
		GIDs: gids,
		Cid: ck.id,
		SeqNum: ck.seqNum,
	}
	ck.seqNum++
	index := ck.lastLeader
	ck.mu.Unlock()
	for {
		var reply LeaveReply
		if ck.servers[index].Call("ShardMaster.Leave", &args, &reply) && !reply.WrongLeader {
			ck.mu.Lock()
			ck.lastLeader = index
			ck.mu.Unlock()
			return
		}
		index = (index + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	args := MoveArgs{
		Shard: shard,
		GID: gid,
		Cid: ck.id,
		SeqNum: ck.seqNum,
	}
	ck.seqNum++
	index := ck.lastLeader
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply MoveReply
		if ck.servers[index].Call("ShardMaster.Move", &args, &reply) && !reply.WrongLeader {
			ck.mu.Lock()
			ck.lastLeader = index
			ck.mu.Unlock()
			return
		}
		index = (index + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}
