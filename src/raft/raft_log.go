package raft

import (
	"6.824/labgob"
	"fmt"
)

type RaftLog struct {
	snapLastIndex int
	snapLastTerm  int
	// [1, snapLastIndex]
	snapshot []byte
	// (snapLastIndex, snapLastIndex + len(tailLog) - 1]
	tailLog []LogEntry
}

func NewLog(snapLastIndex, snapLastTerm int, snapshot []byte, entries []LogEntry) *RaftLog {
	rl := &RaftLog{
		snapLastIndex: snapLastIndex,
		snapLastTerm:  snapLastTerm,
		snapshot:      snapshot,
	}
	rl.tailLog = append(rl.tailLog, LogEntry{
		Term: snapLastTerm,
	})

	return rl
}

func (rl *RaftLog) readPersist(d *labgob.LabDecoder) error {
	var lastIndex, lastTerm int
	if err := d.Decode(&lastIndex); err != nil {
		return fmt.Errorf("Decode last include index faild")
	}
	rl.snapLastIndex = lastIndex

	if err := d.Decode(&lastTerm); err != nil {
		return fmt.Errorf("Decode last include term faild")
	}
	rl.snapLastTerm = lastTerm

	var log []LogEntry
	if err := d.Decode(&log); err != nil {
		return fmt.Errorf("Decode tail log faild")
	}
	rl.tailLog = log
	return nil
}

func (rl *RaftLog) persist(e *labgob.LabEncoder) {
	e.Encode(rl.snapLastIndex)
	e.Encode(rl.snapLastTerm)
	e.Encode(rl.tailLog)
}

func (rl *RaftLog) size() int {
	return rl.snapLastIndex + len(rl.tailLog)
}
func (rl *RaftLog) idx(logicIdx int) int {
	// logic 不在范围 [snapLastIndex, snapLastIndex + len(tailLog) - 1] 内
	if logicIdx < rl.snapLastIndex || logicIdx >= rl.size() {
		panic(fmt.Sprintf("logic index %d out of range [%d, %d]", logicIdx, rl.snapLastIndex, rl.size()-1))
	}
	return logicIdx - rl.snapLastIndex
}
func (rl *RaftLog) at(logicIdx int) LogEntry {
	return rl.tailLog[rl.idx(logicIdx)]
}

func (rl *RaftLog) last() (index, term int) {
	i := len(rl.tailLog) - 1
	return rl.snapLastIndex + i, rl.tailLog[i].Term
}
func (rl *RaftLog) firstLogOfTerm(term int) int {
	for idx, entry := range rl.tailLog {
		if entry.Term == term {
			return idx + rl.snapLastIndex
		} else if entry.Term > term {
			break
		}
	}
	return InvalidIndex
}

func (rl *RaftLog) tail(startIdx int) []LogEntry {
	if startIdx >= rl.size() {
		return nil
	}
	return rl.tailLog[rl.idx(startIdx):]
}

// mutate methods
func (rl *RaftLog) append(e LogEntry) {
	rl.tailLog = append(rl.tailLog, e)
}

func (rl *RaftLog) appendFrom(logicPrevIndex int, entries []LogEntry) {
	idx := rl.idx(logicPrevIndex)
	rl.tailLog = append(rl.tailLog[:idx+1], entries...)
}
func (rl *RaftLog) String() string {
	var terms string
	prevTerm := rl.snapLastTerm
	prevStart := rl.snapLastIndex
	for i := 0; i < len(rl.tailLog); i++ {

		if rl.tailLog[i].Term != prevTerm {
			terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, i+rl.snapLastIndex-1, prevTerm)
			prevTerm = rl.tailLog[i].Term
			prevStart = i
		}
	}
	terms += fmt.Sprintf(" [%d, %d]T%d", prevStart, len(rl.tailLog)+rl.snapLastIndex-1, prevTerm)
	return terms
}

// snapshot in the index
// do checkpoint from the app layer, 从上往下
func (rl *RaftLog) doSnapshot(index int, snapshot []byte) {
	idx := rl.idx(index)
	rl.snapLastIndex = index
	rl.snapLastTerm = rl.tailLog[idx].Term
	rl.snapshot = snapshot

	// make a new log array
	newLog := make([]LogEntry, 0, rl.size()-rl.snapLastIndex)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	newLog = append(newLog, rl.tailLog[idx+1:]...)
	rl.tailLog = newLog
}

// install snapshot from the raft layer，从下往上
func (rl *RaftLog) installSnapshot(index int, term int, snapshot []byte) {
	rl.snapLastIndex = index
	rl.snapLastTerm = term
	rl.snapshot = snapshot

	// make a new log array
	newLog := make([]LogEntry, 0, 1)
	newLog = append(newLog, LogEntry{
		Term: rl.snapLastTerm,
	})
	rl.tailLog = newLog
}
