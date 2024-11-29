package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (rf *Raft) resetElectionTimer() {
	rf.electionStart = time.Now()
	randRange := int64(electionTimeoutMax - electionTimeoutMin)
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63()%randRange)
}

func (rf *Raft) isElectionTimeout() bool {
	return time.Since(rf.electionStart) > rf.electionTimeout
}

func (rf *Raft) isMoreUpToDate(candidateIndex, candidateTerm int) bool {
	sz := len(rf.Logs)
	lastIndex, lastTerm := sz-1, rf.Logs[sz-1].Term
	if candidateTerm != lastTerm {
		return lastTerm > candidateTerm
	}
	return lastIndex > candidateIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// 2A
	Term        int // candidate's Term
	CandidateId int // candidate requesting vote
	// 2B
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // Term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// 2A
	Term        int  // current Term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (args *RequestVoteArgs) String() string {
	return fmt.Sprintf("Candidate-%d, T%d, Last: [%d]T%d", args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. Reply false if Term < currentTerm
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d Reject vote, higher Term %d > %d", args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// 2. 当前节点的 Term 比候选人的 Term 小，降级为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// 3. 当前 Term 已经投过票
	if rf.votedFor != -1 {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d Reject vote, already voted for S%d", args.CandidateId, rf.votedFor)
		return
	}

	// 4. 候选人的日志不是最新的
	if rf.isMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d Reject vote, log not up-to-date", args.CandidateId)
		return
	}
	// 投票
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId

	rf.persist() // 持久化 voteFor

	rf.resetElectionTimer()
	LOG(rf.me, rf.currentTerm, DVote, "-> S%d Vote for S%d", args.CandidateId, rf.votedFor)
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

// startElection 只对当前term（任期）进行选举
func (rf *Raft) startElection(term int) {
	vote := 0
	askVoteFromPeer := func(peer int, args *RequestVoteArgs) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DVote, "Ask vote from S%d, Lost or Error", peer)
			return
		}

		// 如果有更高任期（Term）的节点，则降级为 Follower，并退出 election
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		// 查看当前节点 Term 和 role 是否改变, 如果改变则退出 election
		if rf.isContextLost(Candidate, term) {
			LOG(rf.me, rf.currentTerm, DVote, "Context lost, abort RequestVoteReply for S%d", peer)
			return
		}

		// 大多数节点同意选举，则当选为 Leader
		if reply.VoteGranted {
			vote++
			if vote > len(rf.peers)/2 {
				rf.becomeLeader()
				go rf.replicationTicker(term)
			}
		}
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isContextLost(Candidate, term) {
		LOG(rf.me, rf.currentTerm, DVote, "Lost Candidate to %s, abort startElection", rf.role)
		return
	}
	sz := len(rf.Logs)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			vote++ // vote for itself
			continue
		}

		args := &RequestVoteArgs{
			// 2A
			Term:        rf.currentTerm,
			CandidateId: rf.me,
			// 2B
			LastLogIndex: sz - 1,
			LastLogTerm:  rf.Logs[sz-1].Term,
		}
		LOG(rf.me, rf.currentTerm, DVote, "-> S%d, Ask vote,Args = %v", i, args.String())
		// 异步进行要票
		go askVoteFromPeer(i, args)
	}

}

// ElectionTicker The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ElectionTicker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to

		rf.mu.Lock()
		if rf.role != Leader && rf.isElectionTimeout() {
			rf.becomeCandidate()
			// start a new election
			go rf.startElection(rf.currentTerm)
		}
		rf.mu.Unlock()

		//  randomize sleeping time using time.Sleep().
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
