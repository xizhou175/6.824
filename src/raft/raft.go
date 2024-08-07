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
//	"bytes"
	"sync"
	"sync/atomic"

//	"6.824/labgob"
	"6.824/labrpc"
    "math/rand"
    "time"
    "fmt"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
    FOLLOWER State = iota
    CANDIDATE
    LEADER
)


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

    state            State

    // Persistent state on all servers
    currentTerm      int
    log              []LogEntry
    votedFor         int
    //voteTerm         int

    // Volatile on all servers
    commitIndex      int
    lastApplied      int

    // Volatile on leaders
    nextIndex        []int
    matchIndex       []int

    heartBeatTicker  int
    lastAE           time.Time    // check if an AppendEntries is received when it timeouts

    logmu            sync.Mutex
    cond             sync.Cond
    applyCh          *chan ApplyMsg

    connected        []bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
    rf.mu.Lock()
	// Your code here (2A).
    term = rf.currentTerm
    if rf.state == LEADER {
        isleader = true
    } else {
        isleader = false
    }
    rf.mu.Unlock()
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type LogEntry struct {
    Data    interface{}
    Term    int
}

type AppendEntriesArgs struct {
    Term         int
    LeaderId     int
    PrevLogIndex int
    PrevLogTerm  int
    Entries      []LogEntry   // empty for heartbeat
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term         int
    Success      bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term         int
    CandidateId  int
    LastLogIndex int
    LastLogTerm  int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    lastLogTerm := rf.log[len(rf.log) - 1].Term
    reply.VoteGranted = false

    //fmt.Printf("server %v term %v, args.term %v\n", rf.me, rf.currentTerm, args.Term)
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return
    }


    // Election restriction
    if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && args.LastLogIndex >= len(rf.log) - 1){
        if args.Term > rf.currentTerm {
            rf.state = FOLLOWER
            rf.votedFor = args.CandidateId
            //rf.currentTerm = args.Term
            reply.VoteGranted = true
        } else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
            rf.state = FOLLOWER
            rf.votedFor = args.CandidateId
            reply.VoteGranted = true
        } else {
            fmt.Printf("server %v refused to vote, has voted for: %v\n", rf.me, rf.votedFor)
        }
    } else {
        //fmt.Printf("election restriction check failed. server %v refused to vote\n", rf.me)
    }

    reply.Term = args.Term
    return
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
    rf.cond.L.Lock()
    defer rf.cond.L.Unlock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
    if rf.state != LEADER {
        isLeader = false
    } else {
        //fmt.Printf(">Start: %v server: %v\n", command, rf.me)
        term = rf.currentTerm
        index = len(rf.log)
        entry := LogEntry{command, term}
        rf.log = append(rf.log, entry)
        rf.cond.Signal()
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) sendLog() {
    for {
        rf.cond.L.Lock()
        rf.cond.Wait()
        rf.sendLogEntries()
        rf.cond.L.Unlock()
    }
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
        rf.mu.Lock()
        state := rf.state
        rf.mu.Unlock()
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

        //DPrintf( "server %v state: %v\n", rf.me, rf.state )
        if state == LEADER {
            rf.sendHeartBeats()
            time.Sleep( time.Duration(rf.heartBeatTicker) * time.Millisecond )
        } else {
            var electionTimeouts = time.Duration((rand.Float32() * 250) + 250) * time.Millisecond
            //time.Duration(rand.Intn(100) + 300) * time.Millisecond
            time.Sleep( electionTimeouts )

            rf.mu.Lock()

            // We have to use rf.state rather than state becuase rf.state might have changed after sleeping
            if rf.state == FOLLOWER {
                sinceLastAE := time.Since(rf.lastAE)
                if sinceLastAE > electionTimeouts {
                    go rf.newElection()
                }
            } else if rf.state == CANDIDATE {
                // start leader election
                go rf.newElection()
            }
            rf.mu.Unlock()
        }
	}
}

func (rf *Raft) HeartBeat() {
    for rf.killed() == false {
        rf.mu.Lock()
        state := rf.state
        rf.mu.Unlock()
        if state == LEADER {
            rf.sendHeartBeats()
            time.Sleep( time.Duration(rf.heartBeatTicker) * time.Millisecond )
        }
    }
}

func (rf *Raft) Election() {
    for rf.killed() == false {
        var electionTimeouts = time.Duration(rand.Intn(100) + 300) * time.Millisecond
        time.Sleep( electionTimeouts )

        rf.mu.Lock()

        if rf.state == FOLLOWER {
            sinceLastAE := time.Since(rf.lastAE)
            if sinceLastAE > electionTimeouts {
                go rf.newElection()
            }
        } else if rf.state == CANDIDATE {
            // start leader election
            go rf.newElection()
        }
        rf.mu.Unlock()
    }
}

func (rf *Raft) newElection() {
    // vote for itself
    numGranted := 1

    rf.mu.Lock()
    rf.state = CANDIDATE
    rf.votedFor = rf.me
    //rf.currentTerm += 1
    term := rf.currentTerm + 1
    //DPrintf( "server %v is starting a leader election for term %v\n", rf.me, rf.currentTerm )

    //fmt.Printf( "server %v is starting a leader election for term %v\n", rf.me, term )
    req := RequestVoteArgs{}
    req.Term = term
    req.CandidateId = rf.me
    req.LastLogTerm = rf.log[len(rf.log) - 1].Term
    req.LastLogIndex = len(rf.log) - 1
    rf.mu.Unlock()

    var voteCh = make(chan int)
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }
        go func (channel chan int, index int) {

            reply := RequestVoteReply{}
            //DPrintf( "server %v requesting vote from server %v for term %v\n", rf.me, index, req.Term )
            ok := rf.sendRequestVote(index, &req, &reply)

            //rf.mu.Lock()
			gotTheVote := ok && reply.VoteGranted && reply.Term == term
			//rf.mu.Unlock()

            //DPrintf( "server %v requesting vote from server %v for term %v, ok: %t\n", rf.me, index, req.Term, gotTheVote )
            if gotTheVote {
                channel <- 1
			} else {
                channel <- 0
			}
        }(voteCh, i)
    }

    //rf.mu.Lock()
    //if rf.state == CANDIDATE {
    for i := 0; i < len(rf.peers); i++ {
        vote := <- voteCh
        numGranted += vote

        // check if we already achieved machority
        if numGranted > len(rf.peers) / 2 {
            rf.mu.Lock()
            //DPrintf("(Raft %v -=Election=-)\t Majority achieved", rf.me)

            fmt.Printf("(Raft %v -=Election=-)\t Majority achieved\n", rf.me)

            rf.state = LEADER
            rf.currentTerm = term
            for j := 0; j < len(rf.peers); j++ {
                rf.nextIndex[j] = len(rf.log)
            }

            rf.mu.Unlock()

            rf.sendHeartBeats()
            break
        }
    }
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    checkLog := false
    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        reply.Success = false
    } else if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.state = FOLLOWER
        checkLog = true
        reply.Success = true
    } else {
        rf.state = FOLLOWER
        checkLog = true
        reply.Success = true
    }

    //fmt.Printf("server %v, lastapplied: %v, check: %t leaderCommit: %v commitIndex: %v\n",
    //                   rf.me, rf.lastApplied, checkLog, args.LeaderCommit, rf.commitIndex)

    if checkLog {
        //fmt.Printf("checkLog\n")
        matchWithLeader := false

        //fmt.Printf("server: %v, log len: %v, arglen: %v prevlogindex: %v, args.term: %v\n",
        //            rf.me, len(rf.log), len(args.Entries), args.PrevLogIndex, args.PrevLogTerm )

        if args.PrevLogIndex == 0 || ( len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm ) {
            reply.Success = true
            matchWithLeader = true
        }
        if matchWithLeader {
            if len(args.Entries) > 0 {
                index := 0
                for j := args.PrevLogIndex + 1; j < len(rf.log) && index < len(args.Entries); j++ {
                    rf.log[j] = args.Entries[index]
                    index++
                }

                for index < len(args.Entries) {
                    rf.log = append(rf.log, args.Entries[index])
                    index += 1
                }
            }
        } else {
            reply.Success = false
        }
        //fmt.Printf("server %v log len: %v reply: %t\n", rf.me, len(rf.log), reply.Success)
    }

    reply.Term = rf.currentTerm
    rf.lastAE = time.Now()

    //DPrintf( "server %v received AppendEntries from leader %v\n", rf.me, args.LeaderId )
    return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func(rf *Raft) sendLogEntries() {
    var commitCh = make(chan int)

    go func() {
        commitNum := 1
        /*connectedServer := len(rf.peers)
        for commitNum <= connectedServer / 2 {
            committed := <- commitCh
            commitNum += committed

            if committed == 0 {
                connectedServer--
            }

            //fmt.Printf("commitNum: %v connected servers: %v\n", commitNum, connectedServer )
        }*/

        connectedServer := 1
        for i := 0; i < len(rf.peers) - 1; i++ {
            committed := <- commitCh

            if committed != 0 {
                commitNum++
                connectedServer++
            }

            if commitNum > len(rf.peers) / 2 {
                break
            }
            //fmt.Printf("commitNum: %v connected servers: %v\n", commitNum, connectedServer )
        }

        //fmt.Printf( "connectedServer: %v server: %v state: %v\n", connectedServer, rf.me, rf.state )
        if connectedServer <= len(rf.peers) / 2 || commitNum <= connectedServer / 2 {
            return
        }

        rf.mu.Lock()
        if rf.state != LEADER {
            rf.mu.Unlock()
            return
        }
        rf.commitIndex = len(rf.log) - 1
        //fmt.Printf("Leader server %v ready to apply messages, commitIndex: %v %v\n", rf.me, rf.commitIndex, len(rf.peers) )
        for rf.lastApplied < rf.commitIndex {
            rf.lastApplied++
            entry := rf.log[rf.lastApplied]
            applyMsg := ApplyMsg{}
            applyMsg.CommandValid = true
            applyMsg.Command = entry.Data
            applyMsg.CommandIndex = rf.lastApplied
            *(rf.applyCh) <- applyMsg
            //fmt.Printf("Leader: server %v applied %v, commitIndex: %v\n", rf.me, rf.log[rf.lastApplied], rf.commitIndex)
        }
        rf.mu.Unlock()
        rf.apply()
    }()

    for i, _ := range rf.peers {
        if i == rf.me {
            continue
        }

        go func(index int) {
            rf.mu.Lock()
            req := AppendEntriesArgs{}
            req.Term = rf.currentTerm
            req.LeaderId = rf.me
            req.LeaderCommit = rf.commitIndex
            rf.mu.Unlock()

            success := false
            for success == false {
                reply := AppendEntriesReply{0, false}
                req.PrevLogIndex = rf.nextIndex[index] - 1
                req.PrevLogTerm = rf.log[req.PrevLogIndex].Term

                req.Entries = rf.log[rf.nextIndex[index]:]
                ok := rf.sendAppendEntries(index, &req, &reply)

                if !ok {
                    //fmt.Printf("sent by server %v, server %v dropped out\n", rf.me, index)
                    commitCh <- 0
                    //rf.mu.Lock()
                    //rf.connected[index] = false
                    //rf.mu.Unlock()
                    break
                }

                rf.mu.Lock()
                //fmt.Printf("server %v connected\n", index)
                //rf.connected[index] = true
                if reply.Term > rf.currentTerm {
                    fmt.Printf( "server %v transitioned to FOLLOWER\n", rf.me )
                    rf.state = FOLLOWER
                    rf.votedFor = -1
                    rf.currentTerm = reply.Term
                    commitCh <- -1
                    rf.mu.Unlock()
                    break
                }


                if reply.Success == false {
                    if rf.nextIndex[index] > 1 {
                        rf.nextIndex[index] -= 1
                    }
                    success = false

                    rf.mu.Unlock()
                } else {
                    rf.nextIndex[index] = len(rf.log)
                    rf.mu.Unlock()
                    //fmt.Printf("server %v committed, commitIndex: %v\n", index, len(rf.log)-1)
                    commitCh <- 1
                    success = true
                }

            }
        }(i)
    }
}

func (rf *Raft) sendHeartBeats() {

    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }

        go func(index int) {

            rf.mu.Lock()
            req := AppendEntriesArgs{}
            req.Term = rf.currentTerm
            req.LeaderId = rf.me
            req.LeaderCommit = rf.commitIndex
            req.PrevLogIndex = rf.nextIndex[index] - 1
            req.PrevLogTerm = rf.log[req.PrevLogIndex].Term

            //fmt.Printf("sending prevlogindex: %v, args.term: %v\n", req.PrevLogIndex, req.PrevLogTerm )
            rf.mu.Unlock()


            reply := AppendEntriesReply{}
            ok := rf.sendAppendEntries(index, &req, &reply)

            if ok {
                rf.mu.Lock()
                if reply.Term > rf.currentTerm {
                    DPrintf( "server %v transitioned to FOLLOWER\n", rf.me )
                    rf.state = FOLLOWER
                    rf.votedFor = -1
                    rf.currentTerm = reply.Term
                }

                //rf.connected[index] = true
                rf.mu.Unlock()
            } else {
                //fmt.Printf("sent by server %v, server %v dropped out: heartbeats\n", rf.me, index)
                //rf.mu.Lock()
                //rf.connected[index] = false
                //rf.mu.Unlock()
            }
        }(i)
    }
}

func (rf *Raft) ApplyMsg(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if rf.commitIndex < args.LeaderCommit {
        if args.LeaderCommit < len(rf.log) - 1 {
            rf.commitIndex = args.LeaderCommit
        } else {
            rf.commitIndex = len(rf.log) - 1
        }
        for rf.lastApplied < rf.commitIndex {
            rf.lastApplied++
            entry := rf.log[rf.lastApplied]
            applyMsg := ApplyMsg{}
            applyMsg.CommandValid = true
            applyMsg.Command = entry.Data
            applyMsg.CommandIndex = rf.lastApplied
            *(rf.applyCh) <- applyMsg
            //fmt.Printf("server %v applied data %v, commitIndex: %v\n", rf.me, rf.log[rf.lastApplied], rf.lastApplied)
        }
    }

    rf.lastAE = time.Now()

}

func (rf *Raft) sendApplyMsg(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.ApplyMsg", args, reply)
	return ok
}

func (rf *Raft) apply() {
    for i := 0; i < len(rf.peers); i++ {
        if i == rf.me {
            continue
        }

        go func(index int) {
            rf.mu.Lock()
            req := AppendEntriesArgs{}
            req.Term = rf.currentTerm
            req.LeaderId = rf.me
            req.LeaderCommit = rf.commitIndex
            req.PrevLogIndex = rf.nextIndex[index] - 1
            req.PrevLogTerm = rf.log[req.PrevLogIndex].Term

            //fmt.Printf("sending prevlogindex: %v, args.term: %v\n", req.PrevLogIndex, req.PrevLogTerm )
            rf.mu.Unlock()


            reply := AppendEntriesReply{}

            rf.sendApplyMsg(index, &req, &reply)
        }(i)
    }
}

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

    fmt.Println( "Who am I? ", me )
	// Your initialization code here (2A, 2B, 2C).
    rf.currentTerm = 0
    rf.state = FOLLOWER
    rf.votedFor = -1
    rf.lastAE = time.Now()
    //rf.voteTerm = 0

    rf.commitIndex = 0
    rf.lastApplied = 0

    rf.heartBeatTicker = 100
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    rf.nextIndex = make([]int, len(peers))
    for i := 0; i < len(peers); i++ {
        rf.nextIndex[i] = 1
    }
    rf.matchIndex = make([]int, len(peers))

    rf.connected = make([]bool, len(peers))
    for i := 0; i < len(peers); i++ {
        rf.connected[i] = true
    }

    rf.cond = *sync.NewCond(&(rf.logmu))
    rf.applyCh = &applyCh
    rf.log = make([]LogEntry, 1)
    //servers = append(servers, rf)
	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.sendLog()
    //go rf.HeartBeat()
    //go rf.Election()

	return rf
}
