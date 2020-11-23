package zpkv

import "sync"

type Raft struct {
	lock sync.Mutex // Lock to protect shared access to this peer's state
	// todo
	//peers     []*labrpc.ClientEnd // RPC end points of all peers
	//persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 所有服务器，持久化状态
	votedFor int    // 当前任期内收到选票的候选者id,没有则为-1
	logs     []*Log // 操作日志
	// 所有服务器，易失状态
	commitIndex int // 已知已提交的最高的日志条目的索引
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引
	// leader 易失状态
	nextIndex  []int // 记录每个follow节点log同步的起始索引
	matchIndex []int // 已经复制到follow节点的log索引

	// 所有服务器
	term          int
	role          int
	leaderId      int
	chanHeartbeat chan bool
	// 选举相关
	voteCount    int
	chanWinElect chan bool
	chanVote     chan bool
	chanApply    chan ApplyMsg
}
