package shardkv


// import "shardmaster"
import (
	"bytes"
	"labrpc"
	"log"
	"shardmaster"
	"strconv"
	"time"
)
import "raft"
import "sync"
import "labgob"



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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	mck				*shardmaster.Clerk // Use the client interface to talk to the shardmaster.
	cfg				shardmaster.Config // Current configuration of the shardKV.
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

	toOutShards		map[int]map[int]map[string]string // cfg Num -> (shard -> db)
	comeInShards	map[int]int // Shard -> config Num
	myShards		map[int]bool // To determine which shard belongs to the current group.
	garbages		map[int]map[int]bool // cfg Num -> shards, after finish the migration.

	killCh			chan bool
}

// Replies the put operation result to the clients.
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	originalOp := Op{
		OpType: "Get",
		Key:    args.Key,
		Value: 	"",
		Cid: 	nrand(),
		SeqNum: 0,
	}
	reply.Err, reply.Value = kv.templateStart(originalOp)
}

// Replies the "put" or "append" result to the clients.
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	originalOp := Op{
		OpType: args.Op,
		Key:    args.Key,
		Value:  args.Value,
		Cid:   	args.Cid,
		SeqNum: args.SeqNum,
	}
	reply.Err, _ = kv.templateStart(originalOp)
}

// For a server to try delete its garbage after migrating the data
// from it to another.
func (kv *ShardKV) GarbageCollection(args *MigrateArgs, reply *MigrateReply) {
	// Checks if the server that want to delete garbage
	// is the leader for the group.
	reply.Err = ErrWrongLeader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[args.ConfigNum]; !ok {
		return
	}
	if _, ok := kv.toOutShards[args.ConfigNum][args.Shard]; !ok {
		return
	}
	originalOp := Op{
		OpType: "GC",
		Key:    strconv.Itoa(args.ConfigNum),
		Value:  "",
		Cid:    nrand(),
		SeqNum: args.Shard,
	}
	kv.mu.Unlock()
	reply.Err, _ = kv.templateStart(originalOp)
	kv.mu.Lock()
}

// Checks if contacting the correct leader for all operations.
// Checks if the operation has been received and processed by
// the server and if mapping to a wrong replica group for all
// operations. For "Get" operations, return the value that
// the client requires.
func (kv *ShardKV) templateStart(originalOp Op) (Err, string) {
	index, _, isLeader := kv.rf.Start(originalOp)
	if isLeader {
		ch := kv.put(index, true)
		op := kv.beNotified(ch, index)
		if equalOp(originalOp, op) {
			return OK, op.Value
		}
		if op.OpType == ErrWrongGroup {
			return ErrWrongGroup, ""
		}
	}
	return ErrWrongLeader, ""
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	select {
	case <- kv.killCh:
	default:
	}
	kv.killCh <- true
}

// Try to poll for a new configuration and append the log
// entry for that new configuration to the Raft's log.
// If there are still incoming shards migrating to the
// current group, then reject updating new configuration.
func (kv *ShardKV) tryPollNewCfg() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	// If there are incoming shards and are not fully migrated,
	// just don't try to poll newer configuration.
	if !isLeader || len(kv.comeInShards) > 0 {
		kv.mu.Unlock()
		return
	}
	next := kv.cfg.Num + 1
	kv.mu.Unlock()
	cfg := kv.mck.Query(next)
	if cfg.Num == next {
		// Starts to append the new configuration to
		// the leader's log and append that log for the followers.
		kv.rf.Start(cfg)
	}
}

// If the current server is the leader for the replica group and
// if the current group is serving the shard that the arguments
// specified, then try migrating the shard database out.
func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup

	// Only deal with shard migration from a server of old configuration.
	// The current must be serving the shard for an older configuration.
	if args.ConfigNum >= kv.cfg.Num {
		return
	}
	reply.Err, reply.ConfigNum, reply.Shard = OK, args.ConfigNum, args.Shard
	reply.DB, reply.Cid2Seq = kv.deepCopyDBAndDedupMap(args.ConfigNum, args.Shard)
}

// Gets a deep copy of database and a deep copy of "cid2Seq" map for the
// given configuration number and a specific shard.
func (kv *ShardKV) deepCopyDBAndDedupMap(config int, shard int) (map[string]string, map[int64]int) {
	db2 := make(map[string]string)
	cid2Seq2 := make(map[int64]int)
	for k, v := range kv.toOutShards[config][shard] {
		db2[k] = v
	}
	for k, v := range kv.cid2Seq {
		cid2Seq2[k] = v
	}
	return db2, cid2Seq2
}

// Delete the already migrated data from the current
// server's "toOutShards" in the apply
func (kv *ShardKV) gc(cfgNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.toOutShards[cfgNum]; ok {
		delete(kv.toOutShards[cfgNum], shard)
		if len(kv.toOutShards[cfgNum]) == 0 {
			delete(kv.toOutShards, cfgNum)
		}
	}
}

// Try to pull come-in shards for migration.
func (kv *ShardKV) tryPullShard() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	// Exits if the current server isn't the leader or there is no incoming shard.
	if !isLeader || len(kv.comeInShards) == 0 {
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for shard, idx := range kv.comeInShards {
		wait.Add(1)
		go func(shard int, cfg shardmaster.Config) {
			defer wait.Done()
			args := MigrateArgs{
				Shard:     shard,
				ConfigNum: cfg.Num,
			}
			gid := cfg.Shards[shard]

			// Only the leader will be able to reply the whole migration.
			for _, server := range cfg.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK {
					kv.rf.Start(reply)
				}
			}
		}(shard, kv.mck.Query(idx))
	}
	kv.mu.Unlock()
	wait.Wait()
}

// Try to send a RPC to a server who previously own the shard data
// to let it remove the copy of database(garbage). After the garbage
// is removed, delete the mark of garbage from the garbage list.
// Notice that the current server is the one who receive a data
// because of the change of configuration.
func (kv *ShardKV) tryGC() {
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbages) == 0 {
		kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for cfgNum, shards := range kv.garbages {
		for shard := range shards {
			wait.Add(1)
			go func(shard int, cfg shardmaster.Config) {
				defer wait.Done()
				args := MigrateArgs{
					Shard:     shard,
					ConfigNum: cfg.Num,
				}
				gid := cfg.Shards[shard]
				for _, server := range cfg.Groups[gid] {
					srv := kv.make_end(server)
					reply := MigrateReply{}
					if ok := srv.Call("ShardKV.GarbageCollection", &args, &reply); ok && reply.Err == OK {
						kv.mu.Lock()
						delete(kv.garbages[cfgNum], shard)
						if len(kv.garbages[cfgNum]) == 0 {
							delete(kv.garbages, cfgNum)
						}
						kv.mu.Unlock()
					}
				}
			}(shard, kv.mck.Query(cfgNum))
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}

// Execute the function given periodically.
func (kv *ShardKV) daemon(do func(), sleepMS int) {
	for {
		select {
		case <- kv.killCh:
			return
		default:
			do()
		}
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
	}
}

// This method is for dealing with operation from log coming out
// from the apply channel. The log type will include update in-and-out
// shard migration message due to configuration change. Also, the log
// type will include actual migration of database from one to another.
// It will also deal with garbage collection log and normal operaion
// logs. When the log length reach a limit, it will do a snapshot for
// the current state.
func (kv *ShardKV) apply(applyMsg raft.ApplyMsg) {
	if cfg, ok := applyMsg.Command.(shardmaster.Config); ok {
		kv.updateInAndOutDateShard(cfg)
	} else if migrationData, ok := applyMsg.Command.(MigrateReply); ok {
		kv.updateDBWithMigrateData(migrationData)
	} else {
		op := applyMsg.Command.(Op)
		if op.OpType == "GC" {
			cfgNum, _ := strconv.Atoi(op.Key)
			kv.gc(cfgNum, op.SeqNum)
		} else {
			kv.normal(&op)
		}
		if notifyCh := kv.put(applyMsg.CommandIndex, false); notifyCh != nil {
			send(notifyCh, op)
		}
	}
	if kv.needSnapshot() {
		go kv.doSnapShot(applyMsg.CommandIndex)
	}
}

// Update the mark for which data should send in and send out.
func (kv *ShardKV) updateInAndOutDateShard(cfg shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Only consider newer configuration.
	if cfg.Num <= kv.cfg.Num {
		return
	}
	oldCfg, toOutShard := kv.cfg, kv.myShards
	kv.myShards, kv.cfg = make(map[int]bool), cfg
	for shard, gid := range cfg.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := toOutShard[shard]; ok || oldCfg.Num == 0 {
			kv.myShards[shard] = true
			delete(toOutShard, shard)
		} else {
			kv.comeInShards[shard] = oldCfg.Num
		}
	}

	if len(toOutShard) > 0 {
		kv.toOutShards[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShard {
			outDb := make(map[string]string)
			for k, v := range kv.db {
				if key2shard(k) == shard {
					outDb[k] = v
					delete(kv.db, k)
				}
			}
			kv.toOutShards[oldCfg.Num][shard] = outDb
		}
	}
}

// Updates the database for current server with incoming migraing data.
func (kv *ShardKV) updateDBWithMigrateData(migrationData MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// The migrate data must be from the last configuration.
	if migrationData.ConfigNum != kv.cfg.Num - 1 {
		return
	}
	delete(kv.comeInShards, migrationData.Shard)
	// Must check this, because if the current DB already includes it,
	// we shouldn't override our copy of database for this shard.
	if _, ok := kv.myShards[migrationData.Shard]; !ok {
		kv.myShards[migrationData.Shard] = true
		for k, v := range migrationData.DB {
			kv.db[k] = v
		}
		for k, v := range migrationData.Cid2Seq {
			kv.cid2Seq[k] = Max(v, kv.cid2Seq[k])
		}
		// Mark the data that has been migrated as garbage.
		// Then, wait for the "from" server to delete this garbage.
		if _, ok := kv.garbages[migrationData.ConfigNum]; !ok {
			kv.garbages[migrationData.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[migrationData.ConfigNum][migrationData.Shard] = true
	}
}

// Do normal operation such as put/append/get.
func (kv *ShardKV) normal(op *Op) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	// Checks for wrong group at the application layer
	// here, because if we check from wrong group in
	// after the operation is done and send back, there
	// might be a race condition. The race condition is
	// due to the fact that, after sending a message through
	// the notify channel, some data might have been changed.
	// This is the same for all the operations here, because
	// we don't want to risk serving the data or modifying
	// a data that was lost or changed by others.
	if _, ok := kv.myShards[shard]; !ok {
		op.OpType = ErrWrongGroup
	} else {
		maxSeq, found := kv.cid2Seq[op.Cid]
		if !found || op.SeqNum > maxSeq {
			if op.OpType ==  "Put" {
				kv.db[op.Key] = op.Value
			} else if op.OpType == "Append" {
				kv.db[op.Key] += op.Value
			}
			kv.cid2Seq[op.Cid] = op.SeqNum
		}
		if op.OpType == "Get" {
			op.Value = kv.db[op.Key]
		}
	}
	kv.mu.Unlock()
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persist = persister

	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.cfg = shardmaster.Config{}

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.chMap = make(map[int]chan Op) // Map of channel to monitor the operation of each log index.
	kv.cid2Seq = make(map[int64]int) // Remember the sequence number of each clerk id.

	kv.toOutShards = make(map[int]map[int]map[string]string)
	kv.comeInShards = make(map[int]int)
	kv.myShards = make(map[int]bool)
	kv.garbages = make(map[int]map[int]bool)

	kv.readSnapShot(kv.persist.ReadSnapshot()) // Reads the snapshot when reboot.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killCh = make(chan bool, 1)

	go kv.daemon(kv.tryPollNewCfg, 50)
	go kv.daemon(kv.tryPullShard, 80)
	go kv.daemon(kv.tryGC, 100)

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
				kv.apply(applyMsg)
			}
		}
	}()

	return kv
}

// Checks if two operations are the same.
func equalOp(a Op, b Op) bool {
	return a.Key == b.Key && a.SeqNum == b.SeqNum && a.Cid == b.Cid && a.OpType == b.OpType
}

// A helper function to wait an operation for the channel for that index until timeout.
func (kv *ShardKV) beNotified(ch chan Op, index int) Op {
	// Wait until a one second timeout.
	select {
	case notifyArg, ok := <- ch:
		if ok {
			close(ch)
		}
		kv.mu.Lock()
		delete(kv.chMap, index)
		kv.mu.Unlock()
		return notifyArg
	case <- time.After(time.Second):
		return Op{}
	}
}

// Creates a channel with createIfNotExists parameter. If the channel
// is non-existing and this parameter is true, creates that channel
// and return that channel, otherwise, it should return nil.
// If the channel for that index has already existed, return that
// already existing channel.
func (kv *ShardKV) put(idx int, createIfNotExists bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.chMap[idx]; !ok {
		if !createIfNotExists {
			return nil
		}
		kv.chMap[idx] = make(chan Op, 1)
	}
	return kv.chMap[idx]
}

// Decodes the saved snapshot and migrate it from the snapshot to the
// actual state machine of the kv server
func (kv *ShardKV) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var cid2Seq map[int64]int
	var toOutShards map[int]map[int]map[string]string
	var comeInShards map[int]int
	var myShards map[int]bool
	var garbages map[int]map[int]bool
	var cfg shardmaster.Config
	if d.Decode(&db) != nil || d.Decode(&cid2Seq) != nil || d.Decode(&comeInShards) != nil ||
		d.Decode(&toOutShards) != nil || d.Decode(&myShards) != nil || d.Decode(&cfg) != nil ||
		d.Decode(&garbages) != nil {
		log.Fatalf("ERROR: ReadSnapShot problem with %v", kv.me)
	} else {
		kv.db, kv.cid2Seq, kv.cfg = db, cid2Seq, cfg
		kv.toOutShards, kv.comeInShards, kv.myShards, kv.garbages = toOutShards, comeInShards, myShards, garbages
	}
}

// A helper function to determine whether or not that the kv server
// need a snapshot to save the state.
// Hint: You should compare maxraftstate to persister.RaftStateSize().
func (kv *ShardKV) needSnapshot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Have some buffer to avoid reaching the limit before doing actual snapshot.
	return kv.maxraftstate > 0 && kv.maxraftstate - kv.persist.RaftStateSize() < kv.maxraftstate / 10
}

// Encodes the database and the sequence number map for each client request
// to a snapshot. Then, save the state of the raft and the snapshot.
func (kv *ShardKV) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.db)
	e.Encode(kv.cid2Seq)
	e.Encode(kv.comeInShards)
	e.Encode(kv.toOutShards)
	e.Encode(kv.myShards)
	e.Encode(kv.cfg)
	e.Encode(kv.garbages)
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
