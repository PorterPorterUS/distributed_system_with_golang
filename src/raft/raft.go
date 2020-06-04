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
	ElectionTimeoutLower = time.Duration(300) * time.Millisecond
	ElectionTimeoutUpper = time.Duration(400) * time.Millisecond
)

// should be called with a lock
func (rf *Raft) convertTo(s Nodestate) {
	if s == rf.state {
		return
	}
	DPrintf("Term %d: server %d convert from %v to %v\n",
		rf.currentTerm, rf.me, rf.state, s)
	rf.state = s
	switch s {

	//follower->candidate->leader
	//candidate->follower
	//leader->follower
	case Follower:
		//转为follower之后，
		//reset election  timer(从candidate转来)
		//停止发送心跳(从leader转来)
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
		rf.votedFor = -1

	case Candidate:
		//转为follower之后，reset election  timer(只可能是通过follower转来)
		rf.startElection()

	case Leader:
		//stop election timer
		//start heartbeat broadcast
		//initialize matchIndex to be 0
		//initialize nextIndex to be len of rf.log
		for i := range rf.nextIndex {
			// initialized to leader last log index + 1
			rf.nextIndex[i] = len(rf.log)
		}
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}

		rf.electionTimer.Stop()
		rf.broadcastHeartbeat()
		rf.heartbeatTimer.Reset(HeartbeatInterval)
	}
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
	currentTerm    int //persistence on all servers
	votedFor       int //persistence on all servers
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	state          Nodestate

	//2B
	log         []LogEntry //persistence on all servers
	commitIndex int        //all servers(non-persistence):最大的已经被提交的日志索引,initial to 0
	lastApplied int        //all servers(non-persistence):最后被应用到状态的日志条目索引,initial to 0
	nextIndex   []int      //领导人维护(non-persistence): 对于每一个服务器，需要发送给他的下一个日志条目的索引值，初始化为当前领导人的最后的日志索引值加1
	matchIndex  []int      //领导人维护(non-persistence):对于每一个服务器，已经复制给他的日志的最高索引值,initial to 0
	applych     chan ApplyMsg
}

func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex

	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	if rf.commitIndex > rf.lastApplied {
		go func(start_idx int, entries []LogEntry) {
			for idx, entry := range entries {
				var msg ApplyMsg
				msg.CommandValid = true
				msg.Command = entry.Command
				msg.CommandIndex = start_idx + idx
				rf.applych <- msg
				//rf.applyCh <- msg
				// do not forget to update lastApplied index
				// this is another goroutine, so protect it with lock
				rf.mu.Lock()
				rf.lastApplied = msg.CommandIndex
				rf.mu.Unlock()
			}
		}(rf.lastApplied+1, rf.log[rf.lastApplied+1:rf.commitIndex+1])
	}

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

}

//
//func (rf *Raft) encodeRaftState() []byte {
//	w := new(bytes.Buffer)
//	e := labgob.NewEncoder(w)
//	e.Encode(rf.currentTerm)
//	e.Encode(rf.votedFor)
//	return w.Bytes()
//
//}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("%v fails to recover from persist", rf)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = logs

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
	//these two for election restriction
	LastLogIndex int //index of candidate’s last log entry
	LastLogTerm  int //term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//被动技能 作为receiver接收sender的请求

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // execute before rf.mu.Unlock()
	// Your code here (2A, 2B).

	//1.
	// 请求者的任期比自己小，那么拒绝投票
	//voter并没有投票给candidate,那么拒绝投票
	//reply.term总是指向最新的term
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//2.如果请求的任期比自己大，那么自己切换为Follower
	//如果请求中的任期比自己的大，对方的日志并不一定比自己的新，还需要进一步判断
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
		// do not return here.
	}

	// 3.candidate最新的一条日志的term 不可以 比voter最新的一条日志的term更旧
	//或者
	//两者的term是同样的新，但是candidate的日志长度不可以比voter的短
	// "up-to-date" is defined in thesis 5.4.1
	lastLogIndex := len(rf.log) - 1
	if args.LastLogTerm < rf.log[lastLogIndex].Term ||
		(args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < lastLogIndex) {
		// Receiver is more up-to-date, does not grant vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	reply.Term = rf.currentTerm // not used, for better logging
	reply.VoteGranted = true
	// reset timer after grant vote
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
}

type AppendEntriesArgs struct {
	Term     int // 2A
	LeaderId int // 2A

	//2B
	PrevLogIndex int        //当前要附加的日志entries的上一条的日志索引
	PrevLogTerm  int        // 当前要附加的日志entries的上一条的日志任期号
	Entries      []LogEntry // 需要附加的日志条目（心跳时为空）
	LeaderCommit int        //// 当前领导人已经提交的最大的日志索引值
}
type AppendEntriesReply struct {
	Term    int  // 2A  跟随者的当前的任务号
	Success bool // 2A  跟随者是否接收了当前的日志，在preLogIndex和preLogTerm匹配的情况下为true，否则返回false

	ConflictTerm  int //2B
	ConflictIndex int //2B

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//跟随者接收日志
	//跟随者收到附加日志的请求，不能简单的将日志追加到自己的日志后面，
	//因为跟随者的日志可能和领导人有冲突，或者跟随者缺失更多的日志
	//那么一定要确保本次附加日志的之前的所有日志都相同，也就是说附加当前的日志之前，
	//缺日志就要把缺失的日志补上，日志冲突了，就要把冲突的日志覆盖

	//1.判断附加日志任期Term和当前的Term是否相同：
	//1.A 如果请求的Term小于当前的Term，那么说明收到了来自过期的领导人的附加日志请求，那么拒接处理。
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	//1.B 如果请求的Term大于当前的Term，那么更新当前的Term为请求的Term,并且转为follower。
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.convertTo(Follower)
	}
	// 收到选举的请求，要立即reset election timer
	rf.electionTimer.Reset(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))

	//2.判断preLogIndex是否大于当前的日志长度
	//或者
	//preLogIndex位置处的任期是否和preLogTerm相等以检测要附加的日志之前的日志是否匹配
	currentLogLenth := len(rf.log)
	// 如果日志的长度没有PreLogIndex指定的长度长
	// 或者
	//在PreLogIndex位置处的日志任期不匹配
	if args.PrevLogIndex > currentLogLenth-1 ||
		(args.PrevLogIndex <= currentLogLenth-1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		reply.Term = rf.currentTerm
		// 如果preLogIndex的长度大于当前的日志的长度，那么说明跟随者缺失日志，那么拒绝附加日志，返回false
		if args.PrevLogIndex > currentLogLenth-1 {
			reply.ConflictIndex = currentLogLenth
			reply.ConflictTerm = -1
		} else {
			//如果preLogIndex处的任期和preLogTerm不相等，那么说明日志有冲突，拒绝附加日志，返回false
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			//then we need to find the first entries inside this conflict term,
			//then set the conflict index to be the first one +1
			i := args.PrevLogIndex
			for ; i >= 0 && rf.log[i].Term == reply.ConflictTerm; i-- {
			}
			reply.ConflictIndex = i + 1
		}
		return

	}

	//3. // PreLog的已经完全匹配，开始追加日志
	reply.Success = true
	//从PrevLogIndex+1的位置开始追加日志
	appendStartIndex := args.PrevLogIndex + 1
	//args.Entries starts from PrevlogIndex+1
	for _, logEntry := range args.Entries {
		// 检测对应的append位置是否有日志已经存在
		//如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的
		if len(rf.log)-1 >= appendStartIndex {
			//we need to trim the following logs in followers at the index of appendStartIndex
			//if their logs are not matched
			currentLogEntry := rf.log[appendStartIndex]
			if currentLogEntry.Term != logEntry.Term && currentLogEntry.Command != logEntry.Command {
				rf.log = rf.log[:appendStartIndex]
				rf.log = append(rf.log, LogEntry{Term: logEntry.Term, Command: logEntry.Command})
			}
			// 日志已经存在了，什么都不做
		} else {
			//对应的append位置如果没有日志存在，直接append即可
			rf.log = append(rf.log, LogEntry{Term: logEntry.Term, Command: logEntry.Command})
		}
		appendStartIndex++
	}

	//4.finishing appending, then we need to update commitIndex
	//rf.commitIndex=Min(args.LeaderCommit, lastLogIndex)
	lastLogIndex := len(rf.log) - 1
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > lastLogIndex {
			//rf.commitIndex=lastLogIndex
			rf.setCommitIndex(lastLogIndex)
		} else {
			//rf.commitIndex=args.LeaderCommit
			rf.setCommitIndex(args.LeaderCommit)
		}
		//rf.commitIndex= int(math.Min(args.LeaderCommit, lastLogIndex))
	}
	//5.finally, do persistence
	rf.persist()
}

// should be called with lock
func (rf *Raft) broadcastHeartbeat() {
	currentTerm := rf.currentTerm
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

			prevLogIndex := rf.nextIndex[server] - 1

			// use deep copy to avoid race condition
			// when override log in AppendEntries()
			entries := make([]LogEntry, len(rf.log[prevLogIndex+1:]))
			copy(entries, rf.log[prevLogIndex+1:])

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  rf.log[prevLogIndex].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				//领导人收到收到跟随者附加日志的响应该如何处理？
				//比对响应的Term和当前的Term以确认自己是否过期：
				//如果响应的Term大于当前的Term，那么说明当前的领导人已经过期，马上将自己切换为跟随者
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.convertTo(Follower)
					rf.persist()
				}
				//如果响应的Term小于当前的Term，那么说明当前的收到了过期的响应（可能网路延迟导致），那么忽略
				// 忽略过期任期的响应
				if currentTerm != rf.currentTerm {
					return
				}

				// 避免多个Reply并发的更新nextIndex和matchIndex，判断在收到reply以后，index是否发生变化
				// 如果NextIndex发生了变化，那么说明有其他的响应更改了，那么忽略此次响应
				if prevLogIndex != rf.nextIndex[server]-1 {
					return
				}
				//判断响应的success是否为真：
				//如果为假：那么说明附加日志失败，preLogIndex和preLogTerm和跟随者的日志不匹配，
				//进行步骤5:日志不匹配，那么需要找到下一个和跟随者匹配的日志索引，
				//简单一点可以通过递减nextIndex[peer]来实现。
				if !reply.Success {
					// 第一种情况follower的日志长度短于PrevLogIndex,那么nextIndex = len(follower的日志长短)
					if reply.ConflictTerm == -1 {
						rf.nextIndex[server] = reply.ConflictIndex
					} else {
						//第二种情况是 长度没有问题
						//但是在prevLogIndex位置的term不一样
						//所以在leader中从prevLogIndex往后找
						//unitl找到和conflict term一样term的index 作为 nextIndex
						//假如找不到，nextIndex 作为follower中的conflict term中的第一条的index
						i := prevLogIndex - 1
						for ; i >= 0 && rf.log[i].Term != reply.ConflictTerm; i-- {
						}
						if i < 0 {
							rf.nextIndex[server] = reply.ConflictIndex
						} else {
							rf.nextIndex[server] = i + 1
						}
					}
				} else {
					//响应的success为真,
					// PreLog匹配，发送nextIndex开始对应的日志
					rf.matchIndex[server] = prevLogIndex + len(entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					//DPrintf("服务器%d和Leader %d的日志匹配, 成功append的日志, %v", peer, rf.me, entries)

				}
				// 开始检查从commitIndex之后开始的matchIndex, 检查是否可以提交
				nextCommitIndex := rf.commitIndex
				//for i := rf.CommitIndex + 1; i <= len(rf.Logs); i++ {
				//从len-1到commitIndex,查找leader上的每一条logEntry,
				//去判断有多少个server的matchIndex大于这条LogEntry的index,
				//假如数量超过n/2+1，那么认为这条logEntry已经被提交了
				for i := len(rf.log) - 1; i > rf.commitIndex; i-- {
					committedPeerCount := 1 // 包括自己
					for key, value := range rf.matchIndex {
						if key == rf.me {
							continue
						}
						if value >= i {
							// 说明服务器已经提交
							committedPeerCount++
						}
					}

					// 检测是否大多数服务器都已经提交了，如果是，那么服务器就可以开始提交了
					if committedPeerCount >= len(rf.peers)/2+1 && rf.log[i].Term == rf.currentTerm {
						nextCommitIndex = i
						rf.setCommitIndex(i)
						break
					}
				}

				rf.commitIndex = nextCommitIndex

			}
		}(i)
	}
}

func (rf *Raft) startElection() {
	defer rf.persist()
	rf.currentTerm += 1
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
				//所有的接受消息处理例程都要考虑到 network delay，也就是说收到的消息是过时的，这个要正确处理
				rf.mu.Lock()
				DPrintf("%v got RequestVote response from node %d, VoteGranted=%v, Term=%d",
					rf, server, reply.VoteGranted, reply.Term)
				if reply.VoteGranted && rf.state == Candidate {
					//candidate 处理 vote reply：当这个消息 delay 时，自己已经可能不是candidate了
					//check whether it is still a candidate
					atomic.AddInt32(&voteCount, 1)
					if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
						rf.convertTo(Leader)
					}
				} else {
					//if the rejection reason is not due to voting for someone else,
					//but the term is outdated,
					//then transfer to follower
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

//
// example RequestVote RPC handler.
//

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

		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
		rf.persist()
		//after the log has been added into raft layer, we should send the heartbeat initiative instead of passive
		//之前的实现里，在Raft.Start() 中没有去直接发起同步，
		//而是被动等待下一个心跳包。
		//这样导致在 Raft 上搭建应用后，
		//对于每一个 client，
		//一个操作的返回需要的时间至少是一个心跳包（这样才能完成 log 同步）
		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.broadcastHeartbeat()
		}()

		rf.mu.Unlock()
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
	// Your code here, if desired.
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
	rf.currentTerm = 0
	rf.votedFor = -1 // voted for no one
	rf.heartbeatTimer = time.NewTimer(HeartbeatInterval)
	rf.electionTimer = time.NewTimer(randTimeDuration(ElectionTimeoutLower, ElectionTimeoutUpper))
	rf.state = Follower

	rf.applych = applyCh
	rf.log = make([]LogEntry, 1) // start from index 1

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		// initialized to leader last log index + 1
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))

	go func(node *Raft) {
		for {
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				if rf.state == Follower {
					// rf.startElection() is called in conversion to Candidate
					rf.convertTo(Candidate)
				} else {
					rf.startElection()
				}
				rf.mu.Unlock()

			case <-rf.heartbeatTimer.C:
				rf.mu.Lock()
				if rf.state == Leader {
					rf.broadcastHeartbeat()
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
