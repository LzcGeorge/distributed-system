package raft

import (
	"fmt"
	"sort"
	"time"
)

// LogEntry
type LogEntry struct {
	Term         int         // Term number received by Leader
	Command      interface{} // command for state machine
	CommandValid bool        // true if it is safe for that entry to be applied to state machines.
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	// 2B
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // Term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex

}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Peer's callback function
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "Reject AppendEntries from %s[T%d], lower Term %d < %d", rf.role, rf.currentTerm, args.Term)
		return
	}

	if args.Term >= rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// 1. Reply false if log doesn’t contain an entry at prevLogIndex whose Term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.Logs) {
		LOG(rf.me, rf.currentTerm, DLog2, "Reject AppendEntries from %s[T%d], PrevLogIndex %d out of range", rf.role, rf.currentTerm, args.PrevLogIndex)
		return
	}

	if rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		LOG(rf.me, rf.currentTerm, DLog2, "Reject AppendEntries from %s[T%d], PrevLogIndex %d, PrevLogTerm %d", rf.role, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
		return
	}

	// 2. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	rf.Logs = append(rf.Logs[:args.PrevLogIndex+1], args.Entries...)
	rf.persist() // 持久化 Logs

	reply.Success = true

	LOG(rf.me, rf.currentTerm, DError, "<- receive args.Entries: %v", len(args.Entries))
	LOG(rf.me, rf.currentTerm, DLog2, "Follower append logs: (%d:%d]", args.PrevLogIndex, args.PrevLogIndex+len(args.Entries))

	//3. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.applyCond.Signal()
		LOG(rf.me, rf.currentTerm, DLog2, "Follower update commitIndex: %d", rf.commitIndex)
	}

	rf.resetElectionTimer()
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *AppendEntriesArgs) String() string {
	return fmt.Sprintf("T%d, Leader: %d,PrevLogIndex: %d, PrevLogTerm: %d, Entries: %v, LeaderCommit: %d", rf.Term, rf.LeaderId, rf.PrevLogIndex, rf.PrevLogTerm, len(rf.Entries), rf.LeaderCommit)
}

func (rf *Raft) getMajorityIndex() int {
	tmp := make([]int, len(rf.peers))
	copy(tmp, rf.matchIndex)
	sort.Ints(sort.IntSlice(tmp))

	// 选取中间值
	majority := tmp[(len(rf.peers)-1)/2]
	return majority
}
func (rf *Raft) startReplication(term int) bool {

	replicateToPeer := func(peer int, args *AppendEntriesArgs) {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok {
			LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Lost or crashed, abort from startReplication", peer)
			return
		}

		// 如果有更高任期（Term）的节点，则降级为 Follower，并退出 replication
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}

		// 查看当前节点 Term 和 role 是否改变, 如果改变则退出 replication
		if rf.isContextLost(Leader, term) {
			LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d], abort startReplication", term, rf.role, rf.currentTerm)
			return
		}
		// 处理 Leader 的 AppendEntriesReply
		if !reply.Success {
			// 会退一个 term
			idx, term := args.PrevLogIndex, args.PrevLogTerm
			for idx > 0 && rf.Logs[idx].Term == term {
				idx--
			}
			rf.nextIndex[peer] = idx + 1
			LOG(rf.me, rf.currentTerm, DLog, "Not match with S%d in %d, nextIndex[%d] = %d", peer, args.PrevLogIndex, rf.nextIndex[peer])
			return
		}

		// 更新 matchIndex 和 nextIndex
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		LOG(rf.me, rf.currentTerm, DLog, "receive args: ", args.String())
		LOG(rf.me, rf.currentTerm, DLog2, "S%d matchIndex: %d, nextIndex: %d args.PrevLogIndex: %d args.Entries: %d", peer, rf.matchIndex[peer], rf.nextIndex[peer], args.PrevLogIndex, len(args.Entries))

		// todo: update commitIndex
		majorityMatched := rf.getMajorityIndex()
		if majorityMatched > rf.commitIndex {
			rf.commitIndex = majorityMatched
			rf.applyCond.Signal()
			LOG(rf.me, rf.currentTerm, DLog2, "Leader update commitIndex: %d to %d", rf.commitIndex, majorityMatched)
		}

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.isContextLost(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d], abort startReplication", term, rf.role, rf.currentTerm)
		return false
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.matchIndex[i] = len(rf.Logs) - 1
			rf.nextIndex[i] = len(rf.Logs)
			continue
		}

		prevIdx := rf.nextIndex[i] - 1
		prevTerm := rf.Logs[prevIdx].Term
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			// 2B
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      rf.Logs[prevIdx+1:],
			LeaderCommit: rf.commitIndex,
		}
		LOG(rf.me, rf.currentTerm, DLog, "Leader send AppendEntries to S%d, args: %v", i, args.String())
		LOG(rf.me, rf.currentTerm, DError, "-> S%d, rf.Logs: %d", i, len(rf.Logs))
		go replicateToPeer(i, args)
	}

	return true
}

// startReplication 仅对当前 Term（任期）进行同步/心跳
func (rf *Raft) replicationTicker(term int) {
	for rf.killed() == false {

		ok := rf.startReplication(term)
		if !ok {
			break
		}
		time.Sleep(replicationInterval)
	}
}
