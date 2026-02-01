package kvraft

import (
	"crypto/rand"
	"fmt"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
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
	fmt.Printf("======Get(k: %v)======\n", key)
	var val string
	for {
		for index := range ck.servers {
			args := GetArgs{Key: key}
			reply := GetReply{Err: ""}
			ck.servers[index].Call("KVServer.Get", &args, &reply)
			if reply.Err == "" {
				val = reply.Value
				return val
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
	for {
		for index := range ck.servers {
			args := PutAppendArgs{Key: key, Value: value, Op: op}
			reply := PutAppendReply{Err: ""}
			ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
			if reply.Err == "" {
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	fmt.Printf("======Put(k:%v v:%v)======\n", key, value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	fmt.Printf("======Append(k:%v v:%v)======\n", key, value)
	ck.PutAppend(key, value, "Append")
}
