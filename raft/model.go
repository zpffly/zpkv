package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type Log struct {
	Command interface{}
	Term    int
	Index   int
}

type RequestVoteArgs struct {
	Term         int    // 候选人的任期号
	CandidateId  string // 请求选票的候选人的 Id
	LastLogIndex int    // 候选人的最后日志条目的索引值
	LastLogTerm  int    // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool // 是否获得节点的选票
}



type AppendEntriesArgs struct {
	Term         int
	LeaderId     string
	PrevLogIndex int // 新增加日志条目之前的日志条目的index
	PrevLogTerm  int // 新增加日志条目之前的日志条目的term
	Logs         []*Log
	LeaderCommit int // leader 已提交的日志index
}

type AppendEntriesReply struct {
	Term          int
	ConflictTerm  int
	ConflictIndex int
	Success       bool
}