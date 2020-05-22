package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

// Summarizing of the logics for client side:
// Clerk Struct:
// There should be a slice of servers for the servers that the clerk is connecting to.
// It should store the last leader's id, so that the next time the client is trying
// to connect to a leader, it shouldn't iterate through the list of servers and find
// one to connect to. Instead, it should try to connect the last leader and only
// iterate through the list of server to find a leader if the old leader isn't leader
// anymore.
// The clerk should have a unique identifier and a sequence number for the operation.

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	lastLeader 	int
	id 			int64
	seqNum		int
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
	ck.id = nrand() // Random number for the client's unique identifier.
	ck.seqNum = 0 // initial sequence number for each operation.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// Records the last leader for speeding up.
	index := ck.lastLeader

	// Initializes the arguments for connecting to kv server.
	args := GetArgs{Key: key}

	for {
		// Big take-away: never re-use the same reply variable here.
		reply := GetReply{}
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
		// If the reply tells everything is ok, remember the kv server that
		// the current client is connecting to and return the reply.
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return reply.Value
		}
		// Checks with the next server cause probably the
		// server just checked is not the right leader.
		index = (index + 1) % len(ck.servers)
	}

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// Records the last leader for the current client.
	index := ck.lastLeader

	// Initializes the PutAppendArgs for sending the PutAppend operation.
	args := PutAppendArgs{
		Key:    key,
		Value:  value,
		Op:     op,
		Cid:    ck.id,
		SeqNum: ck.seqNum,
	}

	// Monotonically increasing the sequence number for the next request.
	// NOTICE: The SeqQum number should be one per request, so just one
	// per request. Don't increase the sequence number while one server
	// is failing and increase the sequence number for retrying one another
	// leader in raft.
	ck.seqNum++

	for {
		// Big take-away: never re-use the same reply variable here.
		reply := PutAppendReply{}
		// Sends a RPC to the KVServer with the arguments and wait for the reply.
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return
		}

		// Checks for the next server.
		index = (index + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
