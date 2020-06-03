package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//3A
	leaderId      int
	clientId      int64
	lastRequestId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers //loading all server id into ck.servers
	// You'll have to add code here.
	//3A
	ck.clientId = nrand() // randomly choose somenumber to be the clientID

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//3A
	requestId := ck.lastRequestId + 1

	for {
		args := GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			RequestId: requestId,
		}
		var reply GetReply
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		// if request failed or find wrong leader
		if ok == false || reply.WrongLeader == true {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
			//keep leaderid insider the range of len of servers
			//even though they are larger than the length

		}
		//if request is successfully!
		ck.lastRequestId = requestId
		return reply.Value

	}
	//return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//3A
	requestId := ck.lastRequestId + 1

	for {
		args := PutAppendArgs{
			key,
			value,
			op,
			ck.clientId,
			requestId,
		}
		var reply PutAppendReply
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		// if request failed or find wrong leader
		if ok == false || reply.WrongLeader == true {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		//if request true
		ck.lastRequestId = requestId
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
