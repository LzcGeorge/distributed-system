package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is Committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// Committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// Committed log entry.
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

// Role of Raft peer
type Role string

const (
	Leader    Role = "Leader"
	Candidate Role = "Candidate"
	Follower  Role = "Follower"
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
	role        Role // Leader, Candidate, Follower
	currentTerm int  // current Term
	votedFor    int  // -1 if vote for none

	// log in the Peer's local
	Logs []LogEntry // log entries
	// only used in Leader
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// fields for apply loop
	commitIndex int // index of highest log entry known to be Committed
	lastApplied int // index of highest log entry applied to state machine
	applyCh     chan ApplyMsg
	applyCond   *sync.Cond

	electionStart   time.Time     // time when election was started
	electionTimeout time.Duration // duration of election timeout, random

}

// role transition: becomeFollower
func (rf *Raft) becomeFollower(term int) {
	if term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DDebug, "Can't become  Follower: Term %d < currentTerm %d", term, rf.currentTerm)
		return
	}

	LOG(rf.me, rf.currentTerm, DLog, "%s -> Follower: Term %d -> Term %d", rf.role, rf.currentTerm, term)
	rf.role = Follower
	if term > rf.currentTerm {
		rf.votedFor = -1
	}
	// 是否持久化
	f_persist := rf.currentTerm != term

	// 更新
	rf.currentTerm = term

	if f_persist {
		//rf.mu.Lock() 外面已经有锁了，在becomeFollower 外面加过锁，
		rf.persist() // 持久化 currentTerm and votedFor
		//rf.mu.Unlock()
	}
}

// role transition: becomeCandidate
func (rf *Raft) becomeCandidate() {
	if rf.role == Leader {
		LOG(rf.me, rf.currentTerm, DDebug, "Can't become Candidate: already Leader")
		return
	}

	// a follower increments its current Term and transitions to candidate state, vote for itself
	LOG(rf.me, rf.currentTerm, DLog, "%s -> Candidate: Term %d -> Term %d", rf.role, rf.currentTerm, rf.currentTerm+1)
	rf.currentTerm++
	rf.role = Candidate
	rf.votedFor = rf.me

	rf.persist() // 持久化 currentTerm and votedFor
}

// role transition: becomeLeader
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		LOG(rf.me, rf.currentTerm, DDebug, "Can't become Leader: not Candidate")
		return
	}

	LOG(rf.me, rf.currentTerm, DWarn, "Become Leader: [%s](T%d)", rf.role, rf.currentTerm)
	rf.role = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.Logs)
		rf.matchIndex[i] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be Committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever Committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return 0, 0, false
	}

	rf.Logs = append(rf.Logs, LogEntry{
		Term:         rf.currentTerm,
		Command:      command,
		CommandValid: true,
	})
	rf.persist() // 持久化 Logs
	LOG(rf.me, rf.currentTerm, DDebug, "Leader append log: (%d,%v) in T%d", len(rf.Logs)-1, command, rf.currentTerm)
	return len(rf.Logs) - 1, rf.currentTerm, true
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

const (
	electionTimeoutMin  time.Duration = 250 * time.Millisecond
	electionTimeoutMax  time.Duration = 400 * time.Millisecond
	replicationInterval time.Duration = 70 * time.Millisecond
)

func (rf *Raft) isContextLost(role Role, term int) bool {
	return rf.role != role || rf.currentTerm != term
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

	// 2A: When servers start up, they begin as followers.
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	// 2B
	rf.Logs = append(rf.Logs, LogEntry{}) // a dummy entry
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	// initialize the fields for apply loop
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ElectionTicker()
	go rf.ApplicationTicker()

	return rf
}
