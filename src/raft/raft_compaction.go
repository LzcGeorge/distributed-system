package raft

import "fmt"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	Term     int
	LeaderId int

	LastIncludedIndex int
	LastIncludedTerm  int

	Snapshot []byte
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	Term int
}

func (args *InstallSnapshotArgs) String() string {
	return fmt.Sprintf("Leader-%d, T%d, Last: [%d]T%d", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (reply *InstallSnapshotReply) String() string {
	return fmt.Sprintf("T%d", reply.Term)
}

// follower
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LOG(rf.me, rf.currentTerm, DDebug, "<-- S%d, RecvSnapshot, Args=%v", args.LeaderId, args.String())

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snapshot, higher Term %d > %d", args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	// 1. 当前节点的 Term 比 Leader 的 Term 小，降级为 Follower
	if args.Term >= rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// 查看是否有 snapshot 存在
	if args.LastIncludedIndex <= rf.Logs.snapLastIndex {
		LOG(rf.me, rf.currentTerm, DSnap, "<- S%d, Reject Snapshot, LastIncludedIndex %d <= snapLastIndex %d", args.LeaderId, args.LastIncludedIndex, rf.Logs.snapLastIndex)
		return
	}

	// 保存 snapshot 在 memory/persister/app layer
	rf.Logs.installSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
	rf.persist()
	rf.snapPending = true
	rf.applyCond.Signal()
}

// leader
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service says it has created a Snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Logs.doSnapshot(index, snapshot)
	rf.persist()
}

func (rf *Raft) insallSnapshotToPeer(peer int, term int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		LOG(rf.me, rf.currentTerm, DLog, "SendSnap to S%d failed", peer)
		return
	}
	LOG(rf.me, rf.currentTerm, DLog, "-> S%d, Append, Reply=%v", peer, reply.String())
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
	}

	if rf.isContextLost(Leader, term) {
		LOG(rf.me, rf.currentTerm, DLog, "Lost Leader[T%d] to %s[T%d], abort startReplication", term, rf.role, rf.currentTerm)
		return
	}
	if args.LastIncludedIndex > rf.nextIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludedIndex
		rf.nextIndex[peer] = args.LastIncludedIndex + 1
	}
}
