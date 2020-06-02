package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType 	string
	Key 	string
	Value 	string
	Cid 	int64
	SeqNum 	int
}

type KVServer struct {
	mu      		sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg

	maxraftstate 	int // snapshot if log grows this big
	persist 		*raft.Persister // Persister to persist the state of the kv server.

	// Your definitions here.
	db 				map[string]string // Simple database for key/value pairs
	// A map of channel for each log index in the k/v services
	// with this we can know which operation message to wait for
	// by specifying the log index of that operation. Then, we can
	// have a channel for that index to wait until the message is
	// finished to return from the kv service.
	chMap 			map[int]chan Op
	cid2Seq			map[int64]int
	killCh			chan bool
}

// RPC handler for Get
// This is a client-facing methods. It should simply submit
// the operation to raft, and then wait for the operation to be
// "applied" in the applied loop. This indicates that the applied
// loop must be in a backend service method, which is not here,
// because this is a client-facing method.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	originalOp := Op{
		OpType: "Get",
		Key:    args.Key,
		Value:  strconv.FormatInt(nrand(), 10),
	}

	reply.WrongLeader = true

	// Checks if this is a wrong leader, if not, just return.
	// Use of the start function here to communicate to the
	// raft directly to append the operation log entry to the
	// leader, and also propagate the new log entry to the followers.
	index, _, isLeader := kv.rf.Start(originalOp)
	if !isLeader {
		return
	}

	ch := kv.putIfAbsent(index)
	op := beNotified(ch) // Waiting for a reply of confirming finishing the operation.

	if equalOp(op, originalOp) {
		reply.WrongLeader = false
		kv.mu.Lock()
		defer kv.mu.Unlock()
		reply.Value = kv.db[op.Key]
	}
}

// RPC handler for PutAppend
// This is a client-facing methods. It should simply submit
// the operation to raft, and then wait for the operation to be
// "applied" in the applied loop. This indicates that the applied
// loop must be in a backend service method, which is not here,
// because this is a client-facing method.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originalOp := Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Cid:   	args.Cid,
		SeqNum: args.SeqNum,
	}

	reply.WrongLeader = true

	// Checks if this is a wrong leader, if not, just return.
	index, _, isLeader := kv.rf.Start(originalOp)
	if !isLeader {
		return
	}

	ch := kv.putIfAbsent(index)
	op := beNotified(ch) // Waiting for a reply of confirming finishing the operation.
	if equalOp(op, originalOp) {
		reply.WrongLeader = false
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persist = persister

	// Initialization code here.
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op) // Map of channel to monitor the operation of each log index.
	kv.cid2Seq = make(map[int64]int) // Remember the sequence number of each clerk id.
	kv.readSnapShot(kv.persist.ReadSnapshot()) // Reads the snapshot when reboot.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool)

	// Applied loop to take client operations one at a time.
	// The following code should be in a long-running go routine
	// because this function must return quickly.
	go func() {
		for {
			select {
			case <- kv.killCh:
				return
			case applyMsg := <- kv.applyCh:
				// If the apply message tells the kv server to read from a
				// snapshot, then read from the snapshot and continue
				// the applied loop.
				if !applyMsg.CommandValid {
					kv.readSnapShot(applyMsg.SnapShot)
					continue
				}
				// Operations will send through the applyCh.
				op := applyMsg.Command.(Op)

				kv.mu.Lock()
				maxSeq, found := kv.cid2Seq[op.Cid]
				if !found || op.SeqNum > maxSeq {
					switch op.OpType {
					case "Put" :
						kv.db[op.Key] = op.Value
					case "Append":
						kv.db[op.Key] += op.Value
					}
					kv.cid2Seq[op.Cid] = op.SeqNum
				}
				kv.mu.Unlock()

				// Use the index of the apply message to indicate the
				// index of the log. Then, after successfully doing the
				// operation in this backend method, sends the operation
				// back to the "index" apply channel, which is indicating
				// that the operation for this "index" is done.
				index := applyMsg.CommandIndex
				notifyCh := kv.putIfAbsent(index)

				// Save the current state of the kv server to snapshot.
				// Also, save the raft state in the underlying function.
				if kv.needSnapshot() {
					// Creates go routine here to prevent blocking the apply channel.
					go kv.doSnapShot(index)
				}

				// Sends the notify signal through the notify channel of this index
				// to acknowledge the operation of this index.
				// Notice that the following send function is safe, and can go
				// without blocking the channel forever.
				send(notifyCh, op)
			}
		}
	}()

	return kv
}

// Creates a operation channel.
func (kv *KVServer) putIfAbsent(idx int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		kv.chMap[idx] = make(chan Op, 1) // A 1 buffer channel here to avoid sender block.
	}
	return kv.chMap[idx]
}

// Checks if two operations are the same.
func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.Value == b.Value &&
		a.SeqNum == b.SeqNum && a.Cid == b.Cid && a.OpType == b.OpType
}

// A helper function to wait an operation for the channel for that index until timeout.
func beNotified(ch chan Op) Op {
	// Wait until a one second timeout.
	select {
	case op := <- ch:
		return op
	case <- time.After(time.Second):
		return Op{}
	}
}

// Decodes the saved snapshot and migrate it from the snapshot to the
// actual state machine of the kv server
func (kv *KVServer) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil {
		log.Fatalf("ERROR: Cannot decode the db and cid2Seq for %v", kv.me)
	} else {
		kv.db, kv.cid2Seq = db, cid2Seq
	}
}

// A helper function to determine whether or not that the kv server
// need a snapshot to save the state.
// Hint: You should compare maxraftstate to persister.RaftStateSize().
func (kv *KVServer) needSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.maxraftstate > 0 && kv.maxraftstate - kv.persist.RaftStateSize() < kv.maxraftstate / 10
}

// Encodes the database and the sequence number map for each client request
// to a snapshot. Then, save the state of the raft and the snapshot.
func (kv *KVServer) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cid2Seq)
	kv.mu.Unlock()
	kv.rf.DoSnapShot(index, w.Bytes())
}

// Sends the given operation through the given notifyCh.
// If there is already something in the notify channel, then ignore it
// and sends a new one.
func send(notifyCh chan Op, op Op) {
	select {
	case <- notifyCh:
	default:
	}
	notifyCh <- op
}