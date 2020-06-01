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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

// import "bytes"
// import "../labgob"

type Nodestate int8

const (
	Follower  = Nodestate(1)
	Candidate = Nodestate(2)
	Leader    = Nodestate(3)
)
const (
	HeartbeatInterval    = time.Duration(120) * time.Millisecond
	ElectionTimeoutLower = time.Duration(150) * time.Millisecond
	ElectionTimeoutUpper = time.Duration(300) * time.Millisecond
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
}

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               //setby kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//2A
	currentTerm    int
	votedFor       int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	state          Nodestate

	//2B
	log         []LogEntry
	commitIndex int   //（半数通过了的命令
	lastApplied int   //(最终运用到状态机当中的命令)
	nextIndex   []int //下一个待发送日志的下标，初始化值是 新leader的last Log Index +1
	matchIndex  []int //日志最匹配的一个下标，初始化值是0
	applych     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.encodeRaftState())

}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	return w.Bytes()

}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewEncoder(r)
	var currentTerm, votedFor int
	if d.Encode(currentTerm) != nil || d.Encode(votedFor) != nil {
		DPrintf("%v fails to recover from persist", rf)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Term        int
	CandidateId int
	//2B
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool

	//2B
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	Term     int // 2A
	LeaderId int // 2A

	//2B
	PrevLogIndex int        //日志最匹配的一个下标，初始化值是0
	PrevLogTerm  int        // 该位置上的 entry 的 Term 属性
	Entries      []LogEntry // leader 的 log[PrevLogIndex+1 : ]
	LeaderCommit int        //// 将 leader 的 commitIndex 通知各个 follower ++follower 将据此更新自身的 commitIndex

}
type AppendEntriesReply struct {
	Term    int  // 2A
	Success bool // 2A
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//2A
	//rf is the one receive vote request and args is the one who send vote request
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // execute before rf.mu.Unlock()

	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		//reply.VoteGranted=true
		reply.Term = args.Term
		rf.convertTo(Follower)
	}

	rf.votedFor = args.CandidateId
	reply.VoteGranted = true

	reply.Term = rf.currentTerm

	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

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
	term := -1
	isLeader := true

	// Your code here (2B).
	//由于Start()的功能是将接收到的客户端命令追加到自己的本地log，
	//然后给其他所有peers并行发送AppendEntries RPC来迫使其他peer也同意领导者日志的内容，
	//在收到大多数peers的已追加该命令到log的肯定回复后，
	//若该entry的任期等于leader的当前任期，
	//则leader将该entry标记为已提交的(committed)，
	//提升(adavance)commitIndex到该entry所在的index，
	//并发送ApplyMsg消息到ApplyCh，相当于应用该entry的命令到状态机。
	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		rf.matchIndex[rf.me] = len(rf.log)
		rf.nextIndex[rf.me] = len(rf.log) + 1
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	//2A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	rf.state = Follower //all servers starting with follower
	//2B
	rf.applych = applyCh
	rf.log = make([]LogEntry, 1)

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	//2B
	rf.nextIndex = make([]int, len(rf.peers))  //one nextIndex for each server
	rf.matchIndex = make([]int, len(rf.peers)) //one matchIndex for each server
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) // initialize each nextIndex to be the len of log in the server
	}

	//Starting raft algorithm- leader election
	go func(node *Raft) { //passby a raft pointer
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				if rf.state == Follower {
					rf.convertTo(Candidate)
				} else {
					rf.startElection()
				}
				rf.mu.Unlock()
			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.broadcastHeartbeat()
					//rf.heartbeatTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
					rf.heartbeatTimer.Reset(HeartbeatInterval)
				}
				rf.mu.Unlock()
			}
		}

	}(rf)

	return rf
}

func randTimeDuration(lower, upper time.Duration) time.Duration {
	num := rand.Int63n(upper.Nanoseconds()-lower.Nanoseconds()) + lower.Nanoseconds()
	return time.Duration(num) * time.Nanosecond
}

func (rf *Raft) convertTo(s Nodestate) {
	if rf.state == s {
		return
	}
	DPrintf("Term %d: server %d convert from %v to %v\n",
		rf.currentTerm, rf.me, rf.state, s)
	rf.state = s
	switch s {
	case Follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
		rf.votedFor = -1
	case Candidate:
		rf.startElection()
	case Leader:
		rf.electionTimer.Stop()
		rf.broadcastHeartbeat()
		rf.heartbeatTimer.Reset(HeartbeatInterval)

	}

}

func (rf *Raft) startElection() {
	defer rf.persist()
	rf.currentTerm = rf.currentTerm + 1
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	lastLogIndex := len(rf.log) - 1
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		//2B
		LastLogIndex: lastLogIndex,
		LastLogTerm:  rf.log[lastLogIndex].Term,
	}
	var voteCount int32
	for i := range rf.peers {
		if i == rf.me {
			rf.votedFor = rf.me
			atomic.AddInt32(&voteCount, 1)
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				DPrintf("%v got RequestVote response from node %d, VoteGranted=%v, Term=%d",
					rf, server, reply.VoteGranted, reply.Term)
				if reply.VoteGranted && rf.state == Candidate {
					atomic.AddInt32(&voteCount, 1)
					if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
						rf.convertTo(Leader)
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						rf.persist()
					}
				}
				rf.mu.Unlock()
			}

		}(i)
	}

}

func (rf *Raft) broadcastHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}

			args := AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			rf.mu.Unlock()
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if reply.Success {

				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						rf.persist()
					}

				}
				rf.mu.Unlock()

			}

		}(i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
	}
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	reply.Success = true
}
