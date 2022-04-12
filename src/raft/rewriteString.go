package raft

import "fmt"

//这个类主要为了重写每个结构的string方法，便于输出日志
func (a AppendEntries) String() string {
	return fmt.Sprintf("AppendEntries{Term:%d, LeaderId:%d, PrevLogIndex:%d, PrevLogTerm:%d," +
	" Entries:%v, LeaderCommit:%d}", a.Term, a.LeaderId, a.PrevLogIndex, a.PrevLogTerm, a.Entries, a.LeaderCommit)
}

func (e Entry) String() string {
	return fmt.Sprintf("Entry{Term:%d, Command:%v, LogIndex:%d}", e.Term, e.Command, e.LogIndex)
}

func (a AppendEntriesReply) String() string {
	return fmt.Sprintf("AppendEntriesReply{Term:%d, Success:%t, ConflictIndex:%d}", a.Term, a.Success, a.ConflictIndex)
}
func (a RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs{Term:%d, CandidateId:%d, LastLogIndex:%d, LastLogTerm:%d}", a.Term, a.CandidateId, a.LastLogIndex, a.LastLogTerm)
}

func (a ApplyMsg) String() string {
	return fmt.Sprintf("ApplyMsg{CommandValid:%t, Command:%v, CommandIndex:%d, Snapshot:%v, " +
		"LastIncludedIndex:%d, LastIncludedTerm:%d}", a.CommandValid, a.Command, a.CommandIndex, a.Snapshot,
		a.LastIncludedIndex, a.LastIncludedTerm)
}
