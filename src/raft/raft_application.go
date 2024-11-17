package raft

func (rf *Raft) ApplicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		entries := make([]LogEntry, 0)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.Logs[i])
		}
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: entry.CommandValid,
				Command:      entry.Command,
				CommandIndex: rf.lastApplied + i + 1,
			}
		}

		rf.mu.Lock()
		// rf.lastApplied += len(entries)
		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()
	}
}
