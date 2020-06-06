package shardmaster

import (
	"log"
	"math/rand"
)

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

//Args interface
type Args interface {
	clientId() string
	ticket() string
}
type BaseArgs struct {
	ClientId string
	Ticket   string
}

func (baseArgs *BaseArgs) clientId() string {
	return baseArgs.ClientId
}
func (baseArgs *BaseArgs) ticket() string {
	return baseArgs.Ticket
}

type Reply interface {
	markWrongLeader()
	setCauseErr(err Err)
}
type BaseReply struct {
	WrongLeader bool
	Err         Err
}

func (baseReply *BaseReply) markWrongLeader() {
	baseReply.WrongLeader = true
}
func (baseReply *BaseReply) setCauseErr(err Err) {
	baseReply.Err = err
}

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	// config number
	// 配置文件编号，在整个系统运行过程中递增，每次reconfiguration时递增编号
	Num int
	// shard -> gid,mapper[shardId]=groupID
	Shards [NShards]int
	//每个repica group集群包括的具体节点
	Groups map[int][]string // gid -> servers[]
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	BaseArgs
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	BaseReply
	//WrongLeader bool
	//Err         Err
}

type LeaveArgs struct {
	BaseArgs
	GIDs []int
}

type LeaveReply struct {
	BaseReply
	//WrongLeader bool
	//Err         Err
}

type MoveShardArgs struct {
	Shard int
	GID   int
}

type MoveArgs struct {
	BaseArgs
	MoveShardArgs
}

type MoveReply struct {
	BaseReply
	//WrongLeader bool
	//Err         Err
}

type QueryArgs struct {
	BaseArgs
	Num int // desired config number
}

type QueryReply struct {
	//WrongLeader bool
	//Err         Err
	BaseReply
	Config Config
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
		log.Printf("@@@@@@@"+format, a...)
	}
	return
}
