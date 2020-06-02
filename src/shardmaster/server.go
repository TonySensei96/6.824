package shardmaster


import (
	"log"
	"math"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config 		// indexed by config num
	chMap	map[int]chan Op // Map each log index to channel
	cid2Seq map[int64]int	// Convert the unique client identifier to it's max sequence number applied
	killCh	chan bool		// kill channel
}


type Op struct {
	// Your data here.
	OpType 	string 		// The type of the operation (join/leave/move/query)
	Args	interface{} // The arguments, which should be compatible to the operation type.
	Cid 	int64 		// The unique identifier of the client.
	SeqNum	int 		// The sequence number for the client of this operation.
}

// The Join RPC is used by an administrator to add new replica groups.
// Its argument is a set of mappings from unique, non-zero replica group
// identifiers (GIDs) to lists of server names.
// The shardmaster should react by creating a new configuration
// that includes the new replica groups.
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	originalOp := Op{
		OpType: "Join",
		Args:   *args,
		Cid:    args.Cid,
		SeqNum: args.SeqNum,
	}
	reply.WrongLeader = sm.templateHandler(originalOp)
}

// The Leave RPC's argument is a list of GIDs of previously joined groups.
// The shardmaster should create a new configuration that does not include
// those groups, and that assigns those groups' shards to the remaining groups.
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	originalOp := Op{
		OpType: "Leave",
		Args:   *args,
		Cid:    args.Cid,
		SeqNum: args.SeqNum,
	}
	reply.WrongLeader = sm.templateHandler(originalOp)
}

// The Move RPC's arguments are a shard number and a GID.
// The shardmaster should create a new configuration
// in which the shard is assigned to the group.
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	originalOp := Op{
		OpType: "Move",
		Args:   *args,
		Cid:    args.Cid,
		SeqNum: args.SeqNum,
	}
	reply.WrongLeader = sm.templateHandler(originalOp)
}

// The Query RPC's argument is a configuration number.
// The shardmaster replies with the configuration that
// has that number. If the number is -1 or bigger than
// the biggest known configuration number, the shardmaster
// should reply with the latest configuration.
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	reply.WrongLeader = true
	originalOp := Op{
		OpType: "Query",
		Args:   *args,
		Cid:    nrand(),
		SeqNum: -1,
	}
	wrongLeader, sendBackOp := sm.queryHandler(originalOp)
	reply.WrongLeader = wrongLeader
	// Use the operation from the server to reply for the client.
	if !reply.WrongLeader {
		reply.Config = sendBackOp.Args.(Config)
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	sm.killCh <- true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	// Registering the struct for using.
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.chMap = make(map[int]chan Op)
	sm.cid2Seq = make(map[int64]int)
	sm.killCh = make(chan bool, 1)

	go func() {
		for {
			select {
			case <- sm.killCh:
				return
			case applyMsg := <- sm.applyCh:
				if !applyMsg.CommandValid {
					continue
				}

				// Gets the operation to do:
				// (Join/Leave/Move/Query).
				op := applyMsg.Command.(Op)

				sm.mu.Lock()
				maxSeq, found := sm.cid2Seq[op.Cid]

				// Duplicate detection:
				// If the sequence number is out-dated for the client
				// then the operation shouldn't be applied twice.
				if op.SeqNum >= 0 && (!found || op.SeqNum > maxSeq) {
					sm.updateConfig(op.OpType, op.Args)
					sm.cid2Seq[op.Cid] = op.SeqNum
				}

				// Only for query here, because we don't want to have
				// race condition between Query function getting
				// notified and the configuration being changed.
				// So we just do the query here and send the operation
				// argument back to the notify channel. So, the client
				// will get consistent data.
				if op.OpType == "Query" {
					cfgNum := op.Args.(QueryArgs).Num
					if cfgNum >= 0 && cfgNum < len(sm.configs) {
						op.Args = sm.configs[cfgNum]
					} else {
						op.Args = sm.configs[len(sm.configs) - 1]
					}
				}
				sm.mu.Unlock()

				// Sends a notification through channel to notify that the
				// operation has been done for the client.
				if notifyCh := sm.getCh(applyMsg.CommandIndex, false); notifyCh != nil {
					send(notifyCh, op)
				}
			}
		}
	}()

	return sm
}

// Handler for the shardmaster's operations.
// First, make sure that the current shardmaster that the client
// is connnecting to is a valid leader. Second, make sure that
// the operation is done correctly by waiting for the operation
// to be done until timeout. Finally, make sure that the operation
// done in the backend is the same one as the operation that the
// client was requesting for.
func (sm *ShardMaster) templateHandler(originalOp Op) bool {
	wrongLeader := true
	index, _, isLeader := sm.rf.Start(originalOp)
	if !isLeader {
		return wrongLeader
	}
	ch := sm.getCh(index, true)
	op := sm.beNotified(ch, index)
	if equalOp(originalOp, op) {
		wrongLeader = false
	}
	return wrongLeader
}

// Handler for query operation. After gets the operation result back,
// Check if the argument sent has a result back. Then, return
// whether the operation is successful and the configuration
// back to the client.
func (sm *ShardMaster) queryHandler(originalOp Op) (bool, Op) {
	wrongLeader := true
	index, _, isLeader := sm.rf.Start(originalOp)
	if !isLeader {
		return wrongLeader, Op{}
	}
	ch := sm.getCh(index, true)
	op := sm.beNotified(ch, index)
	if equalOp(originalOp, op) {
		wrongLeader = false
	}
	return wrongLeader, op
}

// Waiting from the given channel for the operation to be done in
// the backend. If the operation is done within the timeout, then
// delete the channel and return the operation. If timeout occurs,
// returns an empty operation.
func (sm *ShardMaster) beNotified(ch chan Op, index int) Op {
	select {
	case notifyArg := <- ch:
		close(ch)
		sm.mu.Lock()
		delete(sm.chMap, index)
		sm.mu.Unlock()
		return notifyArg
	case <- time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}

// Checks if the two operations are the same operation.
func equalOp(a Op, b Op) bool {
	return a.OpType == b.OpType && a.Cid == b.Cid && a.SeqNum == b.SeqNum
}

// Gets the channel for the index of the operation if it is already created
// in the chMap. If it is not exists, and the "createIfNotExists" is true,
// then creates a new channel for the index of operation and return it.
// Otherwise, return nil.
func (sm *ShardMaster) getCh(idx int, createIfNotExists bool) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.chMap[idx]; !ok {
		if !createIfNotExists {
			return nil
		}
		sm.chMap[idx] = make(chan Op, 1)
	}
	return sm.chMap[idx]
}

// Sends the operation through the given channel for operation.
func send(notifyCh chan Op, op Op) {
	select {
	case <- notifyCh:
	default:
	}
	notifyCh <- op
}

// Create a new configuration and update the configuration according to the
// given arguments. Also, applied re-balancing the shards so that the shards
// are evenly distributed among the replica groups. Finally, append the new
// configuration to the shardmaster's configuration logs.
func (sm *ShardMaster) updateConfig(op string, arg interface{}) {
	cfg := sm.createNextConfig()
	if op == "Move" {
		// Assign the shard to the given group id.
		moveArg := arg.(MoveArgs)
		if _, exists := cfg.Groups[moveArg.GID]; exists {
			cfg.Shards[moveArg.Shard] = moveArg.GID
		} else {
			return
		}
	} else if op == "Join" {
		// Adds the new group and the associate servers to the
		// new configuration. Also, re-balance the replica groups.
		joinArg := arg.(JoinArgs)
		for gid, servers := range joinArg.Servers {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			cfg.Groups[gid] = newServers
			sm.rebalance(&cfg, op, gid)
		}
	} else if op == "Leave" {
		// Deletes the given list of gid from the groups.
		// Re-balance the groups after deleting.
		leaveArg := arg.(LeaveArgs)
		for _, gid := range leaveArg.GIDs {
			delete(cfg.Groups, gid)
			sm.rebalance(&cfg, op, gid)
		}
	} else {
		log.Fatal("Invalid area", op)
	}
	sm.configs = append(sm.configs, cfg) // Appends the new configuration.
}

// Creates a new configuration from the last one.
func (sm *ShardMaster) createNextConfig() Config {
	lastCfg := sm.configs[len(sm.configs) - 1]
	nextCfg := Config{
		Num:    lastCfg.Num + 1,
		Shards: lastCfg.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range lastCfg.Groups {
		nextCfg.Groups[gid] = append([]string{}, servers...)
	}
	return nextCfg
}

// Re-balance the new replica groups after a "Join"/"Leave" operation.
// For "Join" operation, moves a shard once a time for the largest group
// to the new group until the new group reach the average number of shards.
// For "Leave" operation, moves a shard once a time from the minimum group
// to other group, until all the shards from the deleted group are moved
// to other groups.
func (sm *ShardMaster) rebalance(cfg *Config, request string, gid int) {
	shardCount := sm.groupByGid(cfg)
	switch request {
	case "Join":
		avg := NShards / len(cfg.Groups)
		for i := 0; i < avg; i++ {
			maxGid := sm.getMaxShardGid(shardCount)
			cfg.Shards[shardCount[maxGid][0]] = gid
			shardCount[maxGid] = shardCount[maxGid][1:]
		}
	case "Leave":
		shardArray, exists := shardCount[gid]
		if !exists {
			return
		}
		delete(shardCount, gid)
		if len(cfg.Groups) == 0 {
			cfg.Shards = [NShards]int{}
			return
		}
		for _, v := range shardArray {
			minGid := sm.getMinShardGid(shardCount)
			cfg.Shards[v] = minGid
			shardCount[minGid] = append(shardCount[minGid], v)
		}
	}
}

// Creates a 2-dimensional slice, which groups the shards by
// the gid that they're belong to.
func (sm *ShardMaster) groupByGid(cfg *Config) map[int][]int {
	shardCount := make(map[int][]int)
	for k, _ := range cfg.Groups {
		shardCount[k] = make([]int, 0)
	}
	for k, v := range cfg.Shards {
		shardCount[v] = append(shardCount[v], k)
	}
	return shardCount
}

// Gets the shard that is managing the maximum number of shards.
func (sm *ShardMaster) getMaxShardGid(shardCount map[int][]int) int {
	max := -1
	var gid int
	for k, v := range shardCount {
		if max < len(v) {
			max = len(v)
			gid = k
		}
	}
	return gid
}

// Gets the gid that is managing minimum number of shards.
func (sm *ShardMaster) getMinShardGid(shardCount map[int][]int) int {
	min := math.MaxInt32
	var gid int
	for k, v := range shardCount {
		if min > len(v) {
			min = len(v)
			gid = k
		}
	}
	return gid
}