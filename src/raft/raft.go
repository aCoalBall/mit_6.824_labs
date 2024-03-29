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

	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	FOLLOWER  = 0
	LEADER    = 1
	CANDIDATE = 2
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

// struct entry contains all information stored in an entry of raft.log
type entry struct {
	index   int         //log index
	term    int         //term when entry was received by leader
	command interface{} //the command it self
}

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

	//Persistant
	currentTerm int      //latest term server has seen (initialled to 0, then increases monotonically)
	votedFor    int      //canditateId that received vote in current term, -1 if none
	log         []*entry //log entries, each entry contains command for state machine, and term when entry was received by leader

	//volatile
	commitIndex  int //index of highest log entry knownn to be commited
	lastApplied  int //index of highest log entry applied to state machine
	currentState int //the current state of a server, follower, candidate or leader?

	//Only initialized on learders
	nextIndex  []int //for each server, index of the next log entry send to that server
	matchIndex []int //for each server, indec of the highest log entry known to be replicated on server

	//Only initialized on candidates
	votes int //the number of votes the candidate got

	//channels
	waitHeartBeat chan int
	appendables   []chan int

	//other variables
	rand int //use for give a random time delay for each follower to call an election
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
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []*entry{}
	rf.log = append(rf.log, &entry{})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentState = FOLLOWER
	rf.waitHeartBeat = make(chan int, 1)
	rf.rand = rand.Intn(400) + 800
	rf.appendables = nil

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//fmt.Println(time.Duration(rf.rand) * time.Millisecond)
	// start ticker goroutine to start elections
	go rf.ticker()
	//go rf.asLeader()
	//go rf.asCandidate()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.currentState == LEADER
}

func (rf *Raft) timeForElection() time.Time {
	return time.Now().Add(time.Duration(rf.rand) * time.Millisecond)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votes = 0
		//rf.waitHeartBeat <- 1
	}
	//check 2 conditions
	//1. is the receiver going to vote the candidate
	//2. is the condidate's log more up-to-date than the receiver
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId &&
		args.LastLogTerm >= rf.currentTerm && args.LastLogIndex >= (len(rf.log)-1) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
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

// #############################################################
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	if args.Term >= rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.votes = 0
		rf.mu.Unlock()
		reply.Success = true
		rf.waitHeartBeat <- 1
	} else {
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Println(ok)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.currentState == FOLLOWER {
			rf.asFollower()
		} else if rf.currentState == CANDIDATE {
			rf.asCandidate()
		} else if rf.currentState == LEADER {
			//fmt.Println("term:", rf.currentTerm, "peer", rf.me, "is leader")
			rf.asLeader()
		} else {
		}
	}
}

func (rf *Raft) asFollower() {
	select {
	case <-rf.waitHeartBeat:
		//fmt.Println("Term:", rf.currentTerm, "peer", rf.me, "received heartbeat")
		rf.currentState = FOLLOWER
	case <-time.After(time.Duration(rf.rand) * time.Millisecond):
		if rf.votedFor == -1 {
			//become a candidate and hold an election
			rf.currentTerm++
			fmt.Println("Term plus 1 *from peer", rf.me, "Current term:", rf.currentTerm)
			rf.currentState = CANDIDATE
			rf.holdElection()
			if rf.votes > (len(rf.peers) * 1 / 2) {
				rf.currentState = LEADER
				fmt.Println("term:", rf.currentTerm, "peer", rf.me, "is leader")
				rf.votes = 0
				rf.votedFor = -1
			}
		}
	}
}

func (rf *Raft) asLeader() {
	time.Sleep(10 * time.Millisecond)
	for server := 0; server < len(rf.peers); server++ {
		time.Sleep(time.Duration(120.0/len(rf.peers)) * time.Millisecond)
		if server != rf.me {
			go func(server int) {
				args := AppendEntriesArgs{
					Term:    rf.currentTerm,
					Entries: nil,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &args, &reply)
				if reply.Term > rf.currentTerm {
					rf.mu.Lock()
					rf.currentState = FOLLOWER
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.votes = 0
					rf.mu.Unlock()
				}
			}(server)
		}
	}

	//time.Sleep(100 * time.Millisecond)
}

func (rf *Raft) asCandidate() {
	//rf.votedFor = -1
	//rf.votes = 0
	select {
	case <-rf.waitHeartBeat:
	case <-time.After(time.Duration(rf.rand) * time.Millisecond):
		rf.mu.Lock()
		rf.currentTerm++
		fmt.Println("Term plus 1 from peer", rf.me, "Reelection")
		rf.mu.Unlock()
		rf.holdElection()
		rf.mu.Lock()
		if rf.votes > (len(rf.peers) * 1 / 2) {
			rf.currentState = LEADER
			fmt.Println("term:", rf.currentTerm, "peer", rf.me, "is leader")
		}
		rf.votes = 0
		rf.votedFor = -1
		rf.mu.Unlock()
	}
}

func (rf *Raft) holdElection() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.votes = 1
	rf.mu.Unlock()
	//fmt.Println("term ", rf.currentTerm, "peer", rf.me, " ", time.Now().UnixMilli())
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].term,
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(server, &args, &reply)
				if reply.VoteGranted {
					rf.mu.Lock()
					rf.votes = rf.votes + 1
					rf.mu.Unlock()
				}
			}(server)
		}
	}
	time.Sleep(10 * time.Millisecond)
}
