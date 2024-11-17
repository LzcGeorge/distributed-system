package raft

import (
	"6.824/labgob"
	"bytes"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	var currentTerm int
	var votedFor int
	var Logs []LogEntry

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&currentTerm); err != nil {
		LOG(rf.me, rf.currentTerm, DError, "Decode currentTerm failed,abort from readPersist")
		return
	}
	rf.currentTerm = currentTerm

	if err := d.Decode(&votedFor); err != nil {
		LOG(rf.me, rf.currentTerm, DError, "Decode votedFor failed,abort from readPersist")
		return
	}
	rf.votedFor = votedFor

	if err := d.Decode(&Logs); err != nil {
		LOG(rf.me, rf.currentTerm, DError, "Decode Logs failed,abort from readPersist")
		return
	}
	rf.Logs = Logs

	LOG(rf.me, rf.currentTerm, DPersist, "Read persist data: currentTerm: %d, votedFor: %d, len(Logs): %v", rf.currentTerm, rf.votedFor, len(rf.Logs))
}
