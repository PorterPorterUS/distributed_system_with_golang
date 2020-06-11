package shardmaster

//
// Shardmaster clerk.
//

import "../labrpc"
import "time"
import "math/rand"

//import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	Id     string
	leader int
}

//func nrand() int64 {
//	max := big.NewInt(int64(1) << 62)
//	bigx, _ := rand.Int(rand.Reader, max)
//	x := bigx.Int64()
//	return x
//}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.Id = UUID()

	return ck
}

func (ck *Clerk) Query(num int) Config {
	//args := &QueryArgs{}
	// Your code here.
	args := QueryArgs{BaseArgs{Ticket: UUID(), ClientId: ck.Id}, num}
	args.Num = num
	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		src := ck.servers[currentLeader]
		index++

		var reply QueryReply
		ok := src.Call("ShardMaster.Query", &args, &reply)
		if !ok {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, request failed", ck.Id, num)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.WrongLeader {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, wrong leader", ck.Id, num)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.Err != "" {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, request failed", ck.Id, num)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("[Client %s]ShardMaster.Query #%d successfully, result is %+v", ck.Id, num, reply.Config)
		ck.leader = currentLeader
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	//args := &QueryArgs{}
	// Your code here.
	args := JoinArgs{BaseArgs{Ticket: UUID(), ClientId: ck.Id}, servers}
	//args.Num = num
	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		src := ck.servers[currentLeader]
		index++

		var reply JoinReply
		ok := src.Call("ShardMaster.Join", &args, &reply)
		if !ok {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, request failed", ck.Id, servers)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.WrongLeader {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, wrong leader", ck.Id, servers)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.Err != "" {
			DPrintf("[Client %s]ShardMaster.Query #%d failed, request failed", ck.Id, servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		DPrintf("[Client %s]ShardMaster.Query #%d successfully, result is %+v", ck.Id, servers)
		ck.leader = currentLeader
		//return reply.Config
	}
}

func (ck *Clerk) Leave(gids []int) {
	DPrintf("[Client %s]ShardMaster.Leaving the group %+v...", ck.Id, gids)
	args := &LeaveArgs{GIDs: gids, BaseArgs: BaseArgs{ClientId: ck.Id, Ticket: UUID()}}

	index := ck.leader
	for {
		currentLeader := index % len(ck.servers)
		srv := ck.servers[currentLeader]
		index++
		// try each known server.
		reply := LeaveReply{}
		ok := srv.Call("ShardMaster.Leave", args, &reply)

		if !ok {
			DPrintf("[Client %s]ShardMaster.Leave the group %+v failed, request failed", ck.Id, gids)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.WrongLeader {
			DPrintf("[Client %s]ShardMaster.Leave the group %+v failed, wrong leader", ck.Id, gids)
			time.Sleep(100 * time.Millisecond)
			continue
		} else if reply.Err != "" {
			DPrintf("[Client %s]ShardMaster.Leave the group %+v failed, %s", ck.Id, gids, reply.Err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		DPrintf("[Client %s]ShardMaster.Leave the group %+v successfully", ck.Id, gids)
		ck.leader = currentLeader
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
