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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"
type Role int

const (
	Leader    = 0
	Candidate = 1
	Follower  = 2
)

type State interface {
	voteTimeout() // this an interface function, any struct can implement it.
	currentRole() Role
}

type BaseState struct {
	raft *Raft
	role Role
}

func (baseState *BaseState) voteTimeout()      {}
func (baseState *BaseState) currentRole() Role { return baseState.role }
func (baseState *BaseState) candidate() {
	//candidate's job
	rf := baseState.raft
	currentTerm := rf.CurrentTerm
	candidateId := rf.me
	lastLogIndex := len(rf.Logs) - 1 + rf.SnapshotIndex
	//when we use slice to get logs,we need -rf.SnapshotIndex
	lastLogTerm := rf.Logs[lastLogIndex-rf.SnapshotIndex].Term

	go func() {
		// 一直保持在候选人状态，直到下面三个事情发生：
		//1. 赢得了大多数选票
		//2. 其他服务器成为leader
		//3. 没有任何人获得选举
		grantedCount := 1
		for peerIndex := range rf.peers {
			if peerIndex == rf.me {
				continue
			}
			go func(peer int) { // 逐个发送请求
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(peer, &RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  candidateId,
					LastLogIndex: lastLogIndex,
					LastLogTerm:  lastLogTerm,
				}, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if ok {
					if reply.Term > rf.CurrentTerm {
						// 发现任期更大的响应，将自己设置为追随者
						rf.CurrentTerm = reply.Term
						rf.votedFor = -1 // 取消对自己的投票
						rf.state = newFollowerState(rf)
						rf.timerRest <- struct{}{} //reset the timer
						rf.persist()
						return
					}
					if currentTerm != rf.CurrentTerm {
						// 收到过期任期的响应，忽略不处理, 或者已经投票完毕
						return
					}
					if reply.VoteGranted { // if the voter vote current candidate
						grantedCount++
						//when handle something for reply,need to check the role
						if grantedCount >= (len(rf.peers)/2+1) && rf.state.currentRole() == Candidate {
							//become a leader
							//初始化所有的nextIndex, 初始化为最后一条日志的index加1
							for i := range rf.peers {
								//rf.MatchIndex[i] = 0
								rf.MatchIndex[i] = len(rf.Logs) - 1 + rf.SnapshotIndex
							}
							for i := range rf.peers {
								rf.NextIndex[i] = rf.SnapshotIndex + len(rf.Logs)
							}
							// 初始化自己的nextIndex和matchIndex
							rf.NextIndex[rf.me] = len(rf.Logs) + rf.SnapshotIndex
							rf.MatchIndex[rf.me] = len(rf.Logs) - 1 + rf.SnapshotIndex
							// 收到大多数的投票，成为Leader
							rf.state = newLeaderState(rf)
							rf.appendRest <- struct{}{} //重置计时器，马上开始发送心跳
						}
					}
				}

			}(peerIndex)

		}
	}()
}

type LeaderState struct {
	//no variable name==> extends father
	*BaseState
}

func newLeaderState(rf *Raft) *LeaderState {
	return &LeaderState{&BaseState{raft: rf, role: Leader}}
}

type CandidateState struct {
	//no variable name==> extends father
	*BaseState
}

func (state *CandidateState) voteTimeout() {
	//if candidate timeout, then increment the currentTerm, call candidate()
	state.raft.mu.Lock()
	defer state.raft.mu.Unlock()
	state.raft.CurrentTerm = state.raft.CurrentTerm + 1
	//starting doing the candidate job
	state.candidate()
}
func newCandidateState(rf *Raft) *CandidateState {
	return &CandidateState{&BaseState{raft: rf, role: Candidate}}
}

type FollowerState struct {
	//no variable name==> extends father
	*BaseState
}

func (state *FollowerState) voteTimeout() {
	//when follower encounter a
	state.raft.mu.Lock()
	defer state.raft.mu.Unlock()
	// 增加自己的任期号
	state.raft.CurrentTerm = state.raft.CurrentTerm + 1
	// 为自己投票
	state.raft.votedFor = state.raft.me
	//generate a new candidate,state is an interface and right handside must be a pointer
	state.raft.state = newCandidateState(state.raft)
	state.candidate()
	state.raft.persist()

}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persist on all servers
	CurrentTerm int
	votedFor    int
	Logs        []LogEntry

	//volatile on all servers
	CommitIndex int
	LastApplied int
	//volatile on leaders
	NextIndex  map[int]int
	MatchIndex map[int]int
	//snapshot
	SnapshotIndex int
	SnapshotTerm  int
	//channel communicate with
	applyChan chan ApplyMsg
	//follower,leader,candidate state
	//struct or interface must be named with capital
	state State

	//timer
	timerRest  chan struct{}
	appendRest chan struct{}
}
type LogEntry struct {
	Term       int
	Command    interface{}
	ResultChan LogEntryAppendResult
}
type LogEntryAppendResult struct {
	Index int
	Term  int
}

//follower extends baseState struct, but still need initial BaseState at first.
func newFollowerState(rf *Raft) *FollowerState {
	return &FollowerState{&BaseState{raft: rf, role: Follower}}
}

func (rf *Raft) voteTimeout() {
	rf.state.voteTimeout()
}
func (rf *Raft) startTimeoutVoteThread() {
	// a timer daemon for follower and candidate
	//once timer timeout, then convert to followers
	//
	minTimeout := 400
	maxTimeout := 600

	for {
		func() {
			//randomly choose a number between[min,max]
			timeout := rand.Intn(maxTimeout-minTimeout) + minTimeout
			timer := time.NewTimer(time.Duration(timeout) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				// 超时, 将自己设置为候选人, 并开始选举
				rf.voteTimeout()
			case <-rf.timerRest:
				DPrintf("服务器%d重置计时器", rf.me)
			}

		}()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
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
	Term         int // 候选人的任期号
	CandidateId  int // 候选人Id
	LastLogIndex int // 候选人最后日志的索引值
	LastLogTerm  int // 候选人最后日志的任期号

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号
	VoteGranted bool // 是否同意投票

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
	//left is a state interface, but right side is FollowerState
	//it will report an error, if state is not an interface
	rf.state = newFollowerState(rf)
	//persistence on all servers
	rf.CurrentTerm = 0
	rf.votedFor = -1
	rf.Logs = make([]LogEntry, 1)
	rf.Logs[0] = LogEntry{Term: 0}
	//volatile on all servers
	rf.CommitIndex = 0
	rf.LastApplied = 0
	//volatile on leaders
	rf.MatchIndex = make(map[int]int)
	rf.NextIndex = make(map[int]int)
	//snapShot
	rf.SnapshotIndex = 0
	rf.SnapshotTerm = 0

	rf.applyChan = applyCh
	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())
	if rf.SnapshotIndex != 0 {
		rf.LastApplied = rf.SnapshotIndex
	}

	return rf
}
