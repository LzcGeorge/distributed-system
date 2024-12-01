package raft

func (rf *Raft) ApplicationTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		rf.applyCond.Wait()

		entries := make([]LogEntry, 0)
		snapPendingApply := rf.snapPending

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			entries = append(entries, rf.Logs.at(i))
		}
		rf.mu.Unlock()

		if !snapPendingApply {
			for i, entry := range entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: entry.CommandValid,
					Command:      entry.Command,
					CommandIndex: rf.lastApplied + i + 1,
				}
			}
		} else {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.Logs.snapshot,
				SnapshotTerm:  rf.Logs.snapLastTerm,
				SnapshotIndex: rf.Logs.snapLastIndex,
			}
		}

		rf.mu.Lock()
		if !snapPendingApply {
			LOG(rf.me, rf.currentTerm, DApply, "Apply log for [%d, %d]", rf.lastApplied+1, rf.lastApplied+len(entries))
			rf.lastApplied += len(entries)
		} else {
			LOG(rf.me, rf.currentTerm, DApply, "Apply snapshot for [0, %d]", rf.Logs.snapLastIndex)
			rf.lastApplied = rf.Logs.snapLastIndex
			if rf.commitIndex < rf.lastApplied {
				rf.commitIndex = rf.lastApplied
			}
			rf.snapPending = false
		}
		// rf.lastApplied += len(entries)
		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()
	}
}
