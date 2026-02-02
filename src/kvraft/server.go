package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string
	Key      string
	Value    string
	ClientId int
	SeqId    int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	kvTb         map[string]string
	msgChan      map[int]chan Op
	//wait         map[int]bool
	//getKey       map[string]bool
	seqId map[int]int
	// Your definitions here.
}

func (kv *KVServer) getMsgChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.msgChan[index]
	if !ok {
		kv.msgChan[index] = make(chan Op, 1)
		ch = kv.msgChan[index]
	}
	return ch
}

func (kv *KVServer) deleteMsgChannel(index int) {
	kv.mu.Lock()
	delete(kv.msgChan, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Op: "Get", Key: args.Key, ClientId: args.ClientId, SeqId: args.SeqId}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = "not a leader"
		return
	}

	index, _, _ := kv.rf.Start(op)

	//kv.mu.Lock()
	//kv.wait[index] = true
	//kv.getKey[args.Key] = true
	//kv.mu.Unlock()

	ch := kv.getMsgChannel(index)

	defer kv.deleteMsgChannel(index)
	select {
	case replyOp := <-ch:
		//fmt.Printf("Agreement reached on %+v\n", replyOp)

		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = "wrong op"
			return
		}

		key := replyOp.Key
		kv.mu.Lock()
		value, ok := kv.kvTb[key]

		//fmt.Printf("kv table: %v\n", kv.kvTb)
		//delete(kv.getKey, args.Key)
		kv.mu.Unlock()
		if !ok {
			reply.Err = "no key"
		} else {
			reply.Value = value
		}
		//fmt.Printf("Got value %v for %+v\n", value, replyOp)
		return
	case <-time.After(100 * time.Millisecond):
		kv.mu.Lock()
		//delete(kv.getKey, args.Key)
		kv.mu.Unlock()
		reply.Err = "timeout"
		fmt.Printf("Timeout on %+v\n", op)
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Op: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, SeqId: args.SeqId}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = "not a leader"
		return
	}

	/*for {
		kv.mu.Lock()
		_, ok := kv.getKey[args.Key]
		kv.mu.Unlock()
		if !ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}*/
	index, _, _ := kv.rf.Start(op)

	//kv.mu.Lock()
	//kv.wait[index] = true
	//kv.mu.Unlock()

	ch := kv.getMsgChannel(index)
	defer kv.deleteMsgChannel(index)
	select {
	case replyOp := <-ch:
		if replyOp.ClientId != op.ClientId || replyOp.SeqId != op.SeqId {
			reply.Err = "wrong op"
			return
		}
		fmt.Printf("Agreement reached on %+v(%v)\n", replyOp, index)
		return
	case <-time.After(400 * time.Millisecond):
		fmt.Printf("Timeout on %+v\n", op)
		reply.Err = "timeout"
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ExecuteOp(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//fmt.Printf("++++++server: %v, op: %+v++++++\n", kv.rf.GetId(), op)
	_, ok := kv.kvTb[op.Key]

	if kv.isDuplicate(op.ClientId, op.SeqId) == true {
		return
	}

	kv.seqId[op.ClientId] = op.SeqId
	if op.Op == "Put" {
		kv.kvTb[op.Key] = op.Value
	} else if op.Op == "Append" {
		if ok {
			kv.kvTb[op.Key] += op.Value
		} else {
			kv.kvTb[op.Key] = op.Value
		}
	}
}

func (kv *KVServer) isDuplicate(clientId int, seqId int) bool {
	lastSeqId, ok := kv.seqId[clientId]
	if !ok {
		return false
	}
	return seqId <= lastSeqId
}

func (kv *KVServer) ReadCh() {
	for msg := range kv.applyCh {
		op := msg.Command.(Op)
		if op.Op == "Put" {
			kv.ExecuteOp(&op)
		} else if op.Op == "Append" {
			kv.ExecuteOp(&op)
		}
		//kv.mu.Lock()
		//_, ok := kv.wait[msg.CommandIndex]
		//if ok {
		//	delete(kv.wait, msg.CommandIndex)
		//}
		//kv.mu.Unlock()
		//if ok {
		ch := kv.getMsgChannel(msg.CommandIndex)
		ch <- op
		//}
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvTb = make(map[string]string)
	kv.msgChan = make(map[int]chan Op)

	//kv.wait = make(map[int]bool)
	//kv.getKey = make(map[string]bool)
	kv.seqId = make(map[int]int)

	go kv.ReadCh()
	// You may need initialization code here.

	return kv
}
