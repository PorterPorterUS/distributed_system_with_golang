package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
import (
	"../shardmaster"
	"log"
	"math/rand"
)

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrTimeout    = "ErrTimeout"
	ErrExpire     = "ErrExpire"
)

type Err string

type BaseArgs struct {
	ClientId string
	Ticket   string
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	BaseArgs
}

type PutAppendReply struct {
	Err         Err
	WrongLeader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	BaseArgs
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader bool
}

type DeleteShardsArgs struct {
	//Key string
	//// You'll have to add definitions here.
	//BaseArgs
	Num     int
	ShardId int // 需要被删除的shard id
}

type DeleteShardsReply struct {
	Err Err
	//Value string
	WrongLeader bool
}

type MigrateShardsArgs struct {
	SourceGid int
	Num       int
	ShardsId  []int
	//Key string
	//// You'll have to add definitions here.
	//BaseArgs
}

type MigrateShardsReply struct {
	//Err   Err
	//Value string
	//Wrongleader bool
	WrongLeader bool
	Err         Err
	Ack         map[string]string
	Msg         string
	Data        [shardmaster.NShards]map[string]string
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func UUID() string {
	n := 6
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf("%%%%%%"+format, a...)
	}
	return
}
