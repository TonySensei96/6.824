package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	UseSnapShot	 bool 	// Indicate whether or not to use snapshot for this apply message.
	SnapShot 	 []byte // The actual bytes of the snapshot.
}

type State int

// Define constant server state
const (
	Follower  State = iota // value -> 0
	Candidate              // value -> 1
	Leader                 // value -> 2
)

const NULL int = -1

type Log struct {
	Term    int         // Term Id from the leader
	Command interface{} // command that the state machine will execute
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State

	// Updated on stable storage before responding to RPCs
	// This is persisted on the servers (non-volatile states)
	currentTerm int
	votedFor    int
	log         []Log

	// The following two fields are for snapshotting purpose
	lastIncludedIndex 	int // The snapshot replaces all entries up through and including this index
	lastIncludedTerm	int // The term of the lastIncludedIndex

	// Volatile states on servers
	commitIndex int // index of the highest log entry known to be commited
	lastApplied int // index of the highest log entry applied to the state machine

	// Volatile states on the LEADER!!
	// reinitialized after election
	nextIndex  []int // for the server, the index of the next log entry to send to that server
	matchIndex []int // for the server, the index of the highest log entry known to be replicated to the server

	applyCh     chan ApplyMsg // Channel for apply massage
	voteCh      chan bool     // message for vote from peers
	appendLogCh chan bool     // channel for appending log to this server
	killCh      chan bool     // channel for kill message
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeRaftState())
}

// A helper function to encode the state of the raft into a byte slice.
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

// A function to persist the state of the raft with a snapshot.
func (rf *Raft) persistWithSnapShot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}

// Excluding the old log entries and persist with the given snapshot.
func (rf *Raft) DoSnapShot(curIdx int, snapshot []byte)  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Every log entries preceding the "curIdx" must be discarded,
	// so this boundary check is necessary.
	if curIdx <= rf.lastIncludedIndex {
		return
	}

	newLog := make([]Log, 0)
	newLog = append(newLog, rf.log[curIdx - rf.lastIncludedIndex:]...)

	// update last include index and term'
	rf.lastIncludedIndex = curIdx
	rf.lastIncludedTerm = rf.getLog(curIdx).Term
	rf.log = newLog

	rf.persistWithSnapShot(snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var clog []Log
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&clog) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil{
		log.Fatalf("readPersist ERROR for server %v", rf.me)
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm, rf.votedFor, rf.log = currentTerm, voteFor, clog
		rf.lastIncludedTerm, rf.lastIncludedIndex = lastIncludedTerm, lastIncludedIndex
		rf.commitIndex, rf.lastApplied = rf.lastIncludedIndex, rf.lastIncludedIndex
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// Invoke by candidates to gather votes
	Term         int // candidates' term number
	CandidateId  int // Candidate requesting vote (candidate's id)
	LastLogIndex int // index of the candidate's last log entry
	LastLogTerm  int // Term of the candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate receive vote
}

// AppendEntries RPC: used to replicated log entry by the leader,
// also used as heartbeat
type AppendEntriesArgs struct {
	Term         int   // leader's term
	LeaderId     int   // leader's id, so follower can redirect clients
	PrevLogIndex int   // index of the log entry immediately preceeding the new ones
	PrevLogTerm  int   // term of the previous log entry
	Entries      []Log // log entries to store, empry for heartbeat
	LeaderCommit int   // leader's commit index for log entry
}

type AppendEntriesReply struct {
	Term          int  // current term id, used for the leader to update itself
	Success       bool // True if follower contains entry matching prevLogIndex and prevLogTerm
	ConflictIndex int  // the index of the firse log which has conflict term
	ConflictTerm  int  // The term of the log entry which has conflict term
}

func send(ch chan bool) {
	// the select clause here is to avoid any blocking in the channel
	select {
	case <-ch: // if there is already something in the channel, just let it go
	default: // use default to avoid blocking
	}
	ch <- true
}

// Get the index of the last log entry
func (rf *Raft) getLastLogIndex() int {
	return rf.logLen() - 1
}

func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIndex()
	// That means the log slice is still empty
	if idx < rf.lastIncludedIndex {
		return NULL
	}
	return rf.getLog(idx).Term
}

// start election for new leader
func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()
	var votes int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			reply := &RequestVoteReply{}
			ret := rf.sendRequestVote(idx, &args, reply)
			if ret {
				// The following operations will subject to race condition
				// So, add mutex to the followings
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// If there is a inconsistency between reply term and current
				// term. Then set the current server to be a follower.
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					return
				}
				// compares the current term with the term you sent in your
				// original RPC. If the two are different, drop the reply
				// and return
				if rf.state != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
				}
				// If above half of the peers vote for the current server
				// then a leader is granted for the current server
				if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
					rf.beLeader() // update status
					// sending out heartbeats immediately after winning election
					send(rf.appendLogCh)
				}
			}
		}(i)
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}
	var success bool
	if args.Term < rf.currentTerm {
		// return false in this case
		success = false
	} else if rf.votedFor != NULL && rf.votedFor != args.CandidateId {
		// return false in this case
		success = false
	} else if args.LastLogTerm < rf.getLastLogTerm() {
		// return false if the candidate's term is not up-to-date
		success = false
	} else if args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex() {
		// return false if the candidate's term is not up-to-date
		success = false
	} else {
		// the current receiver vote for the request initializer
		rf.votedFor = args.CandidateId
		success = true
		rf.state = Follower
		rf.persist()
		send(rf.voteCh)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = success
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler implementation according to the paper's Figure 2
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer send(rf.appendLogCh)
	// all servers rules
	if args.Term > rf.currentTerm { // Set the current server as follower and keep term up-to-date
		rf.beFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = NULL
	reply.ConflictIndex = 0

	// 1. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	//
	// If a follower does not have prevLogIndex in its log, it should return
	// with conflictIndex = len(log) and conflictTerm = None.
	//
	// If a follower does have prevLogIndex in its log, but the term does not
	// match, it should return conflictTerm = log[prevLogIndex].Term, and then
	// search its log for the first index whose entry has term equal to
	// conflictTerm.
	prevLogIndexTerm := NULL
	logSize := rf.logLen()
	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < logSize {
		prevLogIndexTerm = rf.getLog(args.PrevLogIndex).Term
	}
	if prevLogIndexTerm != args.PrevLogTerm {
		reply.ConflictIndex = logSize
		if prevLogIndexTerm != NULL { // The follower has prevLogIndex
			reply.ConflictTerm = prevLogIndexTerm
			for i := rf.lastIncludedIndex; i < logSize; i++ {
				if rf.getLog(i).Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
		}
		return
	}

	// 2. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index >= logSize {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		// Same index but different term
		if rf.getLog(index).Term != args.Entries[i].Term {
			rf.log = rf.log[:index - rf.lastIncludedIndex]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	// This is for a follower to update the commit index and last applied log index.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
		rf.updateLastApplied()
	}
	reply.Success = true
}

// For partA, this function will purely send a heartbeat without actually
// appending the logs
func (rf *Raft) startAppendLog() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}

				// To prevent the follower's log to be too far
				// behind the leader's last included index.
				if rf.nextIndex[idx] - rf.lastIncludedIndex < 1 {
					rf.sendSnapshot(idx)
					return
				}

				// If last log index ≥ nextIndex for a follower: send
				// AppendEntries RPC with log entries starting at nextIndex
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.getPrevLogIndex(idx),
					PrevLogTerm:  rf.getPrevLogTerm(idx),
					Entries:      append(make([]Log, 0), rf.log[rf.nextIndex[idx] - rf.lastIncludedIndex:]...),
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}
				rf.mu.Unlock()

				ret := rf.sendAppendEntries(idx, &args, reply)

				rf.mu.Lock()

				// compares the current term with the term you sent in your
				// original RPC. If the two are different, drop the reply
				// and return
				if !ret || rf.state != Leader || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.beFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				// If last log index ≥ nextIndex for a follower: send
				// AppendEntries RPC with log entries starting at nextIndex
				// 1. If successful, update nextIndex and matchIndex for the
				// followers
				// 2. If AppendEntries fails because of log inconsistency,
				// decrement nextIndex and retry
				if reply.Success {
					rf.updateNextMatchIndex(idx, args.PrevLogIndex + len(args.Entries))
					rf.mu.Unlock()
					return
				} else {
					// Upon receiving a conflict response, the leader should first search its
					// log for conflictTerm. If it finds an entry in its log with that term,
					// it should set nextIndex to be the one beyond the index of the last entry
					// in that term in its log.
					//
					// If it does not find an entry with that term,
					// it should set nextIndex = conflictIndex.
					tarIndex := reply.ConflictIndex
					if reply.ConflictTerm != NULL {
						logSize := rf.logLen()
						for i := rf.lastIncludedIndex; i < logSize; i++ {
							if rf.getLog(i).Term != reply.ConflictTerm {
								continue
							}
							for i < logSize && rf.getLog(i).Term == reply.ConflictTerm {
								i++
							}
							// Beyond the index of the last entry in that term in its log.
							tarIndex = i
						}
					}

					// The next index of the follower must not exceeding the length of the log.
					rf.nextIndex[idx] = Min(rf.logLen(), tarIndex)
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

// For a Leader:
// If there exists an N such that N > commitIndex, a majority
// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
// set commitIndex = N
func (rf *Raft) updateCommitIndex() {
	rf.matchIndex[rf.me] = rf.logLen() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}

// If commitIndex > lastApplied: increment lastApplied, apply
// log[lastApplied] to state machine.
// This function simply means applied the logs one by one
// up to the commit index (rf.commitIndex).
func (rf *Raft) updateLastApplied() {
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		curLog := rf.getLog(rf.lastApplied)
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      curLog.Command,
			CommandIndex: rf.lastApplied,
			UseSnapShot: false,
			SnapShot: nil,
		}
		rf.applyCh <- applyMsg
	}
}

// A helper funciton to get the index of the previous log.
func (rf *Raft) getPrevLogIndex(i int) int {
	return rf.nextIndex[i] - 1
}

// A helper function to get the term of the previous log.
func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIdx := rf.getPrevLogIndex(i)
	if prevLogIdx < rf.lastIncludedIndex {
		return NULL
	}
	return rf.getLog(prevLogIdx).Term
}

// Gets the log at the given index "i".
func (rf *Raft) getLog(i int) Log {
	return rf.log[i - rf.lastIncludedIndex]
}

// Gets the length of the log in total
// (not just the length of the underlying slice of log).
func (rf *Raft) logLen() int {
	return len(rf.log) + rf.lastIncludedIndex
}

// Update the match index and next index from the leader to the follower.
func (rf *Raft) updateNextMatchIndex(server int, matchIdx int) {
	rf.matchIndex[server] = matchIdx
	rf.nextIndex[server] = matchIdx + 1
	rf.updateCommitIndex()
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// If the current server is the leader, that means it should
	// accept the incoming operation. Then, we should initializes
	// a new log, and append the new log to the leader's log
	// entries. Finally, we should persist the current state of the
	// leader, and also propagate the new log entries to all the
	// followers.
	if isLeader {
		index = rf.getLastLogIndex() + 1
		newLog := Log{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, newLog)
		rf.persist()
		rf.startAppendLog()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	send(rf.killCh)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = NULL
	rf.log = make([]Log, 1) // the first index is 1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.applyCh = applyCh
	// because go-routine only send the chan to below goroutine, to avoid block
	// we need 1 buffer
	rf.voteCh = make(chan bool, 1)
	rf.appendLogCh = make(chan bool, 1)
	rf.killCh = make(chan bool, 1)
	rf.readPersist(persister.ReadRaftState())

	// Hearbeat RPC timeout
	heartbeatTime := time.Duration(100) * time.Millisecond

	// Hint: You'll need to write code that takes actions periodically or after
	// delays in time. The easiest way to do this is to create a goroutine with
	// a loop that calls time.Sleep()
	go func() {
		for {
			// If there is a kill message received, accept this message and
			// terminate
			select {
			case <-rf.killCh:
				return
			default:
			}

			// Hint: You will need to have a election timeout larger than the paper's
			// 150 to 300 middliseconds, but not too large
			electionTime := time.Duration(rand.Intn(100)+300) * time.Millisecond

			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			switch state {
			case Follower, Candidate:
				// For buffered channel, the sender can send value to the
				// channel without blocking. However, the select clause here
				// will block and wait for a message to come in. The receiver
				// below will wait until the election timeout.
				select {
				case <-rf.voteCh:
				case <-rf.appendLogCh:
				// If election timeout elapses without receiving AppendEntries
				// RPC from current leader or granting vote to candidate:
				// convert to candidate.
				case <-time.After(electionTime):
					rf.beCandidate()
				}
			// For leader, repeat during idle periods to prevent election timeout
			case Leader:
				rf.startAppendLog() // sends heartbeats periodically
				time.Sleep(heartbeatTime)
			}
		}
	}()

	return rf
}

// Change the server state to candidate for new election
func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist() // Use lock to protech persist
	rf.mu.Unlock()
	go rf.startElection()
}

// change the server state to the follower
func (rf *Raft) beFollower(term int) {
	rf.state = Follower
	rf.votedFor = NULL
	rf.currentTerm = term
	rf.persist() // This function is covered by lock, so no need to have lock
}

func (rf *Raft) beLeader() {
	if rf.state != Candidate {
		return
	}

	rf.state = Leader

	// Re-initializes the volatile state data
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
}

type InstallSnapShotArgs struct {
	Term 				int // Leader's term
	LeaderID 			int // So follower can redirect clients
	LastIncludedIndex 	int // The snapshot replaces all entries up through and including this index
	LastIncludedTerm 	int
	Data 				[]byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for the leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//	Receiver implementation:
//	1. Reply immediately if term < currentTerm
//	2. Create new snapshot file if first chunk (offset is 0)
//	3. Write data into snapshot file at given offset
//	4. Reply and wait for more data chunks if done is false
//	5. Save snapshot file, discard any existing or partial snapshot
//	with a smaller index
//	6. If existing log entry has same index and term as snapshot’s
//	last included entry, retain log entries following it and reply
//	7. Discard the entire log
//	8. Reset state machine using snapshot contents (and load
//	snapshot’s cluster configuration)
func (rf *Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// 1. Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// All server rule :
	// If RPC request or response contains Term T > currentTerm,
	// set currentTerm = T, convert to follower.
	if args.Term > rf.currentTerm {
		rf.beFollower(args.Term)
	}

	// If election timeout elapse without receiving AppendEntries RPC from current leader.
	// Send a message through the appendLogCh to prevent election timeout.
	send(rf.appendLogCh)

	// The new snapshot must have the lastIncludedIndex greater then the current server's
	// lastIncludedIndex. Otherwise, the snapshot will have no entries to save.
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	applyMsg := ApplyMsg{CommandValid: false, UseSnapShot: true, SnapShot: args.Data}

	// If existing log entry has same index and term as snapshot's last included entry,
	// retain log entries following it and reply.
	// This means that the argument's lastIncludedLogIndex will
	// fall within the log's indices.
	if args.LastIncludedIndex < rf.logLen() - 1 {
		rf.log = append(make([]Log, 0), rf.log[args.LastIncludedIndex - rf.lastIncludedIndex:]...)
	} else {
		rf.log = []Log{{Term: args.LastIncludedTerm, Command: nil}}
	}

	// Reset the state machine using snapshot contents and load snapshot's cluster configuration.
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.persistWithSnapShot(args.Data)
	rf.commitIndex = Max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, args.LastIncludedIndex)

	// If the snapshot's db has an older version than the current kv server,
	// then return without installing the outdated version snapshot.
	if rf.lastApplied > rf.lastIncludedIndex {
		return
	}

	rf.applyCh <- applyMsg
}

// Send snapshots from the leader to the followers using this function.
func (rf *Raft) sendSnapshot(server int) {
	args := InstallSnapShotArgs{
		Term: rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
		LeaderID: rf.me,
		Data: rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ret := rf.sendInstallSnapshot(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Checks if there is a valid reply.
	// Checks if the InstallSnapshot RPC sender is a leader.
	// Checks if the original RPC included leader's term in consistent with the leader's current term.
	if !ret || rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	// All server's rule:
	// If the sender's term is less than the server that was sent an InstallSnapshot RPC,
	// then the leader is out to date in terms of "Term" and should be converted to follower.
	if reply.Term > rf.currentTerm {
		rf.beFollower(reply.Term)
		return
	}

	// Then, update the match index and next index of the follower.
	// Also, inside the following function, it will also update the
	// commit index and the last applied of the follower.
	rf.updateNextMatchIndex(server, rf.lastIncludedIndex)
}
