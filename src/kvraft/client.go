package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int
	clientId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd, cli int) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = cli
	// You'll have to add code here.
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	//fmt.Printf("======Get(k: %v)======\n", key)
	var val string
	ck.seqId++
	for {
		for index := range ck.servers {
			args := GetArgs{Key: key, SeqId: ck.seqId, ClientId: ck.clientId}
			reply := GetReply{Err: ""}
			ok := ck.servers[index].Call("KVServer.Get", &args, &reply)

			if !ok {
				continue
			}

			if reply.Err == "" {
				val = reply.Value
				return val
			} else if reply.Err == "timeout" {
				break
			} else {
				continue
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	for {
		for index := range ck.servers {
			args := PutAppendArgs{Key: key, Value: value, Op: op, SeqId: ck.seqId, ClientId: ck.clientId}
			reply := PutAppendReply{Err: ""}
			ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)

			if !ok {
				continue
			}

			if reply.Err == "" {
				return
			} else if reply.Err == "timeout" {
				break
			} else if reply.Err == "not a leader" {
				continue
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	//fmt.Printf("======Put(k:%v v:%v)======\n", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	//fmt.Printf("======Append(k:%v v:%v)======\n", key, value)
	ck.PutAppend(key, value, "Append")
}
