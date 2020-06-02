package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            	= "OK"
	ErrWrongGroup 	= "ErrWrongGroup"
	ErrWrongLeader 	= "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   	string
	Value 	string
	Op    	string 	// "Put" or "Append"
	Cid	  	int64 	// client unique identifier
	SeqNum	int 	// Each request with a monotonically increasing sequence number
}

type PutAppendReply struct {
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err         Err
	Value       string
}

type MigrateArgs struct {
	Shard 		int
	ConfigNum 	int
}

type MigrateReply struct {
	Err 		Err
	ConfigNum 	int
	Shard 		int
	DB 			map[string]string
	Cid2Seq 	map[int64]int
}

// Gets the max between the given two integers.
func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
