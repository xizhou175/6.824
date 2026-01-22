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

	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"math/rand"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state State

	// Persistent state on all servers
	currentTerm int
	log         []LogEntry
	votedFor    int
	//voteTerm         int

	// Volatile on all servers
	commitIndex int
	lastApplied int

	// Volatile on leaders
	nextIndex  []int
	matchIndex []int

	heartBeatTicker int
	lastAE          time.Time // check if an AppendEntries is received when it timeouts

	logmu   sync.Mutex
	cond    sync.Cond
	applyCh *chan ApplyMsg
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("decoding error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
	//rf.PrintPersist()
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	Command interface{}
	Term    int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // empty for heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	XTerm    int
	XIndex   int
	LogLenth int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.VoteGranted = false
	var check_log bool = false
	if args.LastLogIndex >= 0 {
		if len(rf.log) > 0 {
			lastLogIndex := len(rf.log) - 1
			if rf.log[lastLogIndex].Term < args.LastLogTerm || (rf.log[lastLogIndex].Term == args.LastLogTerm && args.LastLogIndex+1 >= len(rf.log)) {
				check_log = true
			}
		} else {
			check_log = true
		}
	} else {
		if len(rf.log) == 0 {
			check_log = true
		}
	}

	if check_log == false {
		//fmt.Printf("no way for %v to vote for %v(last term %v, lastlogindex %v)!\n", rf.me, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.persist()
		reply.VoteGranted = true
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.state = FOLLOWER
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
	} else {
		//fmt.Printf("server %v refused to vote for %v, has voted for: %v\n", rf.me, args.CandidateId, rf.votedFor)
	}

	reply.Term = args.Term
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()

	if rf.state == LEADER {
		term = rf.currentTerm
		//c, ok := command.(string)

		//if !ok {
		//	panic("command cannot be converted to string!")
		//}
		var entry LogEntry = LogEntry{command, term}
		//fmt.Printf("Command(Raft: %v): %+v\n", rf.me, entry)
		rf.log = append(rf.log, entry)
		rf.persist()
		index = len(rf.log)
		//rf.cond.Signal()
		rf.mu.Unlock()
		rf.sendLogEntries()
	} else {
		isLeader = false
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
			time.Sleep(time.Duration(rf.heartBeatTicker) * time.Millisecond)
		} else {
			var electionTimeouts = time.Duration(rand.Intn(200)+300) * time.Millisecond
			time.Sleep(electionTimeouts)

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

func (rf *Raft) sendLog() {
	for {
		rf.mu.Lock()
		if rf.state == LEADER && len(rf.log) > 0 {
			rf.mu.Unlock()
			rf.sendLogEntries()
		} else {
			rf.mu.Unlock()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) newElection() {
	// vote for itself
	numGranted := 1

	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.currentTerm += 1
	//DPrintf("server %v is starting a leader election for term %v\n", rf.me, rf.currentTerm)
	req := RequestVoteArgs{}
	req.Term = rf.currentTerm
	req.CandidateId = rf.me
	req.LastLogIndex = len(rf.log) - 1
	if req.LastLogIndex >= 0 {
		req.LastLogTerm = rf.log[req.LastLogIndex].Term
	}
	rf.persist()
	rf.mu.Unlock()

	var voteCh = make(chan int)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(channel chan int, index int) {

			reply := RequestVoteReply{}
			//DPrintf( "server %v requesting vote from server %v for term %v\n", rf.me, index, req.Term )
			ok := rf.sendRequestVote(index, &req, &reply)

			rf.mu.Lock()
			gotTheVote := ok && reply.VoteGranted && reply.Term == rf.currentTerm
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term + 1
				rf.persist()
			}
			rf.mu.Unlock()

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
		vote := <-voteCh
		numGranted += vote

		// check if we already achieved machority
		if numGranted > len(rf.peers)/2 {
			rf.mu.Lock()
			DPrintf("(Raft %v -=Election(Term %v)=-)\t Majority achieved", rf.me, rf.currentTerm)

			rf.state = LEADER

			for j := 0; j < len(rf.peers); j++ {
				if j == rf.me {
					continue
				}
				rf.nextIndex[j] = len(rf.log)
			}

			rf.mu.Unlock()

			go rf.sendLog()

			rf.sendHeartBeats()
			break
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.persist()
			rf.state = FOLLOWER
		} else {
			rf.state = FOLLOWER
		}
		if args.PrevLogIndex == -1 {
			reply.Success = true
		} else if len(rf.log) > args.PrevLogIndex {
			if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
				reply.Success = true
			}
		}
		if reply.Success == true {
			//fmt.Printf("%v currentTerm: %v  args: %+v\n", rf.me, rf.currentTerm, *args)
			index := 0

			for j := args.PrevLogIndex + 1; j < len(rf.log) && index < len(args.Entries); j++ {
				rf.log[j] = args.Entries[index]
				index++
			}

			for index < len(args.Entries) {
				rf.log = append(rf.log, args.Entries[index])
				rf.persist()
				index += 1
			}

			//fmt.Printf("rf.commitIndex %v leadercommit %v\n", rf.commitIndex, args.LeaderCommit)
			if rf.commitIndex < args.LeaderCommit {
				//fmt.Printf("rf.commitIndex %v leadercommit %v prevlogindex %v\n", rf.commitIndex, args.LeaderCommit, args.PrevLogIndex)
				if args.LeaderCommit < len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.log) - 1
				}
				//fmt.Printf("rf.commitIndex %v lastApplied %v\n", rf.commitIndex, rf.lastApplied)
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					entry := rf.log[rf.lastApplied]
					applyMsg := ApplyMsg{}
					applyMsg.CommandValid = true
					applyMsg.Command = entry.Command
					applyMsg.CommandIndex = rf.lastApplied + 1
					*(rf.applyCh) <- applyMsg
					//fmt.Printf("applied %v(%v) on Raft %v\n", entry.Command, rf.lastApplied+1, rf.me)
				}
			}
		} else if len(args.Entries) != 0 {
			reply.XTerm = -1
			reply.XIndex = -1
			if len(rf.log) <= args.PrevLogIndex {
				reply.LogLenth = len(rf.log)
			} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.XTerm = rf.log[args.PrevLogIndex].Term
				reply.XIndex = args.PrevLogIndex
				for i := args.PrevLogIndex; i >= 0; i-- {
					if rf.log[i].Term == reply.XTerm {
						reply.XIndex = i
					} else {
						break
					}
				}
			}
		}
	}

	reply.Term = rf.currentTerm
	rf.lastAE = time.Now()

	//DPrintf("server %v received AppendEntries from leader %v\n", rf.me, args.LeaderId)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			reply := AppendEntriesReply{}
			req := AppendEntriesArgs{}

			rf.mu.Lock()
			req.Term = rf.currentTerm
			req.LeaderId = rf.me
			req.LeaderCommit = rf.commitIndex
			req.PrevLogIndex = rf.nextIndex[index] - 1
			if req.PrevLogIndex >= 0 {
				req.PrevLogTerm = rf.log[req.PrevLogIndex].Term
			}
			rf.mu.Unlock()

			//fmt.Printf("Raft %v sent out heartbeat to %v req: %+v\n", rf.me, index, req)
			rf.sendAppendEntries(index, &req, &reply)

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				DPrintf("server %v transitioned to FOLLOWER\n", rf.me)
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist()
			}
			rf.mu.Unlock()
		}(i)
	}
}

func (rf *Raft) sendLogEntries() {
	rf.mu.Lock()
	all_req := AppendEntriesArgs{}
	all_req.Term = rf.currentTerm
	all_req.LeaderId = rf.me
	// length of rf.log is at least 1

	all_req.Entries = append(all_req.Entries, rf.log[len(rf.log)-1])

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
	}
	all_req.PrevLogIndex = len(rf.log) - 2
	if all_req.PrevLogIndex >= 0 {
		all_req.PrevLogTerm = rf.log[all_req.PrevLogIndex].Term
	}
	all_req.LeaderCommit = -1
	rf.mu.Unlock()

	var commitCh = make(chan int)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			reply := AppendEntriesReply{}
			reply.Success = false

			req := all_req

			//fmt.Printf("server %v: req: %+v to server %v\n", rf.me, all_req, index)

			for {
				ok := rf.sendAppendEntries(index, &req, &reply)

				if !ok {
					commitCh <- 0
					return
				}
				rf.mu.Lock()
				if reply.Term > req.Term {
					DPrintf("server %v transitioned to FOLLOWER\n", rf.me)
					rf.state = FOLLOWER
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.persist()
					rf.mu.Unlock()
					commitCh <- 0
					break
				} else if reply.Success == true {
					//fmt.Printf("server %v committed\n", index)
					rf.nextIndex[index] = len(rf.log)
					rf.mu.Unlock()
					commitCh <- 1
					break
				} else {
					//fmt.Printf("server %v failed to match. Retrying...\n", index)
					if reply.XTerm == -1 {
						//fmt.Printf("Missing XTerm\n")
						req.PrevLogIndex = reply.LogLenth - 1
					} else {
						//fmt.Printf("Conflicting XTerm\n")
						has_xterm := false
						last_index_xterm := -1
						for j := len(rf.log) - 1; j >= 0; j-- {
							if rf.log[j].Term == reply.XTerm {
								has_xterm = true
								last_index_xterm = j
								break
							}
						}
						if has_xterm == true {
							req.PrevLogIndex = last_index_xterm
						} else {
							req.PrevLogIndex = reply.XIndex - 1
						}
					}
					req.Term = rf.currentTerm
					req.LeaderId = rf.me

					//req.PrevLogIndex -= 1
					nextIndex := req.PrevLogIndex + 1
					rf.nextIndex[index] = nextIndex
					req.Entries = rf.log[nextIndex:]

					if req.PrevLogIndex >= 0 {
						req.PrevLogTerm = rf.log[req.PrevLogIndex].Term
					}
					//fmt.Printf("req: %+v\n", req)
					rf.mu.Unlock()
				}
			}
		}(i)
	}

	go func(commitCh chan int) {
		numCommitted := 0
		for i := 0; i < len(rf.peers); i++ {
			commit := <-commitCh

			numCommitted += commit

			// check if we already achieved machority
			if numCommitted >= len(rf.peers)/2 {

				//DPrintf("(Raft %v -=Commit=-)\t Majority achieved", rf.me)

				rf.mu.Lock()
				rf.commitIndex = len(rf.log) - 1

				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					entry := rf.log[rf.lastApplied]
					applyMsg := ApplyMsg{}
					applyMsg.CommandValid = true
					applyMsg.Command = entry.Command
					applyMsg.CommandIndex = rf.lastApplied + 1
					*(rf.applyCh) <- applyMsg
					//fmt.Printf("Leader applied %v(%v) on Raft %v\n", entry.Command, rf.lastApplied+1, rf.me)
				}
				rf.sendHeartBeats()
				rf.mu.Unlock()

				break
			}
		}
	}(commitCh)
}

func (rf *Raft) PrintPersist() {
	fmt.Printf("=======Service %v persistent state======\n", rf.me)
	fmt.Printf("currentTerm: %v votedFor: %v\n", rf.currentTerm, rf.votedFor)
	fmt.Printf("log: %v\n", rf.log)
	fmt.Printf("=============================\n")
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	//fmt.Println( "Who am I? ", me )
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.lastAE = time.Now()
	//rf.voteTerm = 0

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.applyCh = &applyCh
	rf.log = make([]LogEntry, 0)
	rf.cond = *sync.NewCond(&rf.logmu)

	rf.heartBeatTicker = 100
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.nextIndex = make([]int, len(peers))

	//servers = append(servers, rf)
	// start ticker goroutine to start elections
	go rf.ticker()
	//go rf.sendCmd()
	//go rf.HeartBeat()
	//go rf.Election()

	return rf
}
