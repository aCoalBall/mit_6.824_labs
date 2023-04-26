package raft

type AppendEntriesArgs struct {
	Term         int   //Leader's term
	LeaderID     int   //so follower can redirect clients
	PrevLogIndex int   //index of log entry immediately preceding new ones
	PrevLogTerm  int   //term of prevLogIndex entry
	Entries      []int //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int   //Leader's commitIndex

}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if folower contained entry matching prevLogIndex and prevLogTerm
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}
