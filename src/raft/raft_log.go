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
