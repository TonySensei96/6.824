package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
// Field names must start with capital letters,
// otherwise RPC will break.
type PutAppendArgs struct {
	Key   	string
	Value 	string
	Op    	string // "Put" or "Append"
	Cid 	int64  // Unique identifier for the client
	SeqNum 	int	   // Sequence number for the operation
}

// For the reply from the connected kv server.
type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

// Arguments for sending a request to the kv server
// for "Get" operation.
type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

// For the reply from the kv server for "Get" operation.
type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
