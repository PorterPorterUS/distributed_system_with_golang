package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

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
	//3A
	Key   string
	Value string
	Name  string

	ClientId  int64
	RequestId int
}

// notify the RPC handler that a request from a client has been done
type Notification struct {
	ClientId  int64
	RequestId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	//3A
	db map[string]string //数据
	//dispatcher: 解决多个Clerk操作raft（Leader）问题
	// （根据ClientId和RequsetId判断是否为当前Clerk和本次修改）
	dispatcher map[int]chan Notification
	//避免同一个Clerk（ClientId）重复操作
	lastAppliedRequestId map[int64]int
}

//should be called with lock
//check whether same client has already sent same command with same seq number
//3A
func (kv *KVServer) isDuplicatedRequest(clientId int64, requestId int) bool {
	// Your code here.
	appliedRequestId, ok := kv.lastAppliedRequestId[clientId]
	//if cannot find the clientId in the mapper
	//or can find the clientId but sequential number is larger than the current one
	if ok == false || appliedRequestId < requestId {
		return false
	} else {
		return true
	}

}

//3A
func (kv *KVServer) waitingApplying(op Op, timeout time.Duration) bool {
	//利用 Start() 来开始在 Raft 中同步一个操作
	index, _, isLeader := kv.rf.Start(op)
	// it is a wrong leader
	if isLeader == false {
		return true
	}
	var wrongLeader bool
	kv.mu.Lock()
	//create a channel for each client, and this channel wait until there is notification
	// inside it, then we can notify the client, the command execution has been finished
	if _, ok := kv.dispatcher[index]; !ok {
		kv.dispatcher[index] = make(chan Notification, 1)
	}
	ch := kv.dispatcher[index]
	kv.mu.Unlock()

	select {
	case notify := <-ch:
		kv.mu.Lock()
		delete(kv.dispatcher, index)
		kv.mu.Unlock()
		if notify.ClientId != op.ClientId || notify.RequestId != op.RequestId {
			// leader has changed
			wrongLeader = true
		} else {
			wrongLeader = false
		}

	case <-time.After(timeout):
		kv.mu.Lock()
		if kv.isDuplicatedRequest(op.ClientId, op.RequestId) {
			wrongLeader = false
		} else {
			wrongLeader = true
		}
		kv.mu.Unlock()

	}
	return wrongLeader
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//3A
	op := Op{
		Key:       args.Key,
		Name:      "Get",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	//wait until there is notification inside the channel
	reply.WrongLeader = kv.waitingApplying(op, 500*time.Millisecond)
	//if the leader is right,then  get the value for client
	if reply.WrongLeader == false {
		value, ok := kv.db[args.Key]
		if ok {
			reply.Value = value
			return
		}
		//cannot find the value
		reply.Err = ErrNoKey
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//3A
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Name:      args.Op,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	reply.WrongLeader = kv.waitingApplying(op, 500*time.Millisecond)

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// You may need initialization code here.
	//3A
	kv.db = make(map[string]string)
	kv.dispatcher = make(map[int]chan Notification)
	kv.lastAppliedRequestId = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	//3A
	go func() {
		//在初始化时开启一个 goroutine 不断读取 applyCh 中的消息，并执行这些消息中的操作。
		//执行结束后通知对应的等待的 handler。
		for msg := range kv.applyCh {
			if msg.CommandValid == false {
				continue
			}
			//use of interface
			//if the msg is a valid command, then proves that raft layer finish his job
			op := msg.Command.(Op)
			DPrintf("kvserver %d applied command %s at index %d",
				kv.me, op.Name, msg.CommandIndex)

			kv.mu.Lock()
			//check whether the client has processed the same command
			if kv.isDuplicatedRequest(op.ClientId, op.RequestId) {
				kv.mu.Unlock()
				continue
			}
			//if it is a new command
			switch op.Name {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			}
			//store the requestId that has been performed by server into the mapper
			kv.lastAppliedRequestId[op.ClientId] = op.RequestId
			//command execution finished, need to make a notification to the listening thread
			if ch, ok := kv.dispatcher[msg.CommandIndex]; ok {
				nofify := Notification{
					ClientId:  op.ClientId,
					RequestId: op.RequestId,
				}
				ch <- nofify
			}
			kv.mu.Unlock()

		}

	}()

	return kv
}
