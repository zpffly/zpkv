package raft

import (
	"log"
	"math/rand"
	"net/rpc"
	"sort"
	"sync"
	"time"
	"zpkv/config"
)


const (
	Leader = iota + 1
	Follower
	Candidate
)


func MakeRaftServer(configFile string) {
	rf := &Raft{}
	rf.init(configFile)
	rf.run()
}

type Raft struct {
	lock sync.Mutex
	//me   int
	dead int32
	//persister
	peers    *config.Config

	// 所有服务器，持久化状态
	votedFor string    // 当前任期内收到选票的候选者ip,没有则为空
	logs     []*Log // 操作日志
	// 所有服务器，易失状态
	commitIndex int // 已知已提交的最高的日志条目的索引
	lastApplied int // 已经被应用到状态机的最高的日志条目的索引
	// leader 易失状态
	nextIndex  map[string]int // 记录每个follow节点log同步的起始索引
	matchIndex map[string]int // 已经复制到follow节点的log索引

	// 所有服务器
	term          int
	role          int
	leaderId      string
	chanHeartbeat chan bool
	// 选举相关
	voteCount    int
	chanWinElect chan bool
	chanVote     chan bool
	chanApply    chan ApplyMsg
}


func (rf *Raft) init(configFile string) {
	rf.peers = config.InitConfig(configFile)
	rf.chanHeartbeat = make(chan bool, 10)
	rf.chanWinElect = make(chan bool, 10)
	rf.chanVote = make(chan bool, 10)
	rf.chanApply = make(chan ApplyMsg)
	rf.role = Follower
	rf.votedFor = ""
	rf.lock = sync.Mutex{}
	//rf.readPersist()
}

func (rf *Raft) run() {
	for  {
		switch rf.role {
		case Candidate:
			rf.lock.Lock()
			rf.votedFor = rf.peers.Local
			rf.term++
			rf.voteCount = 1
			rf.persist()
			rf.lock.Unlock()
			go rf.broadcastVote()
			select {
			case <-rf.chanHeartbeat:
				rf.lock.Lock()
				rf.role = Follower
				rf.lock.Unlock()
			case <-rf.chanWinElect:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)): //重新进入candidate，进行新一轮选举
			}

		case Follower:
			select {
			case <-rf.chanHeartbeat:
			case <-rf.chanVote:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.lock.Lock()
				rf.role = Candidate
				rf.lock.Unlock()
			}

		case Leader:
			go rf.broadcastAppend()
			time.Sleep(time.Millisecond * 60)
		}
	}
}



// 持久化
func (rf *Raft) persist() {

}

// 从持久化数据中读取, 恢复宕机之前的状态
func (rf *Raft) readPersist(data []byte) {
}


// 向其他节点请求投票
func (rf *Raft) broadcastVote() {
	rf.lock.Lock()
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.peers.Local,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
	rf.lock.Unlock()

	for _, endpoint := range rf.peers.Peers {
		if rf.role == Candidate {
			go rf.sendRequestVote(endpoint, args, &RequestVoteReply{})
		}
	}
}


// 发送投票请求
func (rf *Raft) sendRequestVote(endpoint string, args *RequestVoteArgs, reply *RequestVoteReply) {

	conn, err := rf.GetConnection(endpoint)
	if err != nil {
		log.Printf("get conn fail, err: %v", err)
		return
	}
	defer conn.Close()

	err = conn.Call("Raft.RequestVote", args, reply)
	if err != nil {
		log.Printf("call Raft.RequestVote fail, err: %v", err)
		return
	}

	rf.lock.Lock()
	defer rf.lock.Unlock()

	// 如果role不对或者不是本轮的选举
	if rf.role != Candidate || args.Term != rf.term {
		return
	}

	if reply.Term > rf.term {
		rf.role = Follower
		rf.term = reply.Term
		rf.leaderId = ""
		// 清空，才可以进行下一轮的选举
		rf.votedFor = ""
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.voteCount++
		// 得到半数及以上的票
		if rf.voteCount >= (len(rf.peers.Peers)+1) /2+1 {
			rf.role = Leader
			rf.nextIndex = make(map[string]int)
			rf.matchIndex = make(map[string]int)

			nextIdx := rf.lastLogIndex() + 1
			// 初始化下一次发送的日志的index为当前节点的最新日志
			for _, endpoint := range rf.peers.Peers {
				rf.nextIndex[endpoint] = nextIdx
			}
			rf.chanWinElect <- true
		}
	}

}




// 心跳响应，同时进行日志同步
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	// 告诉leader当前的term
	reply.Term = rf.term
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term {
		rf.role = Follower
		rf.votedFor = ""
		rf.leaderId = ""
		rf.term = args.Term
		rf.persist()
	}
	rf.chanHeartbeat <- true
	rf.leaderId = args.LeaderId

	// 如果当前follow节点没有待追加日志的前一条日志，返回false
	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.ConflictIndex = rf.lastLogIndex()
		return
	}
	// 存在前一条日志但是term不匹配，返回false
	if args.PrevLogIndex > 0 && rf.logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.logs[args.PrevLogIndex-1].Term
		// 找到冲突日志的term，这个term的日志应该去掉
		// index:  0 1 2 3 4 5 6
		// leader: 1 1 1 2 2 3 4
		// follow: 1 1 1 2 2 2 4
		// conflict index 3
		for i := 1; i <= args.PrevLogIndex; i++ {
			if rf.logs[i-1].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return
	}

	// 直接将leader传过来的日志从PrevLogIndex复制
	// 1,2,3,4,5
	//     8,9,2
	rf.logs = rf.logs[:args.PrevLogIndex]
	rf.logs = append(rf.logs, args.Logs...)
	rf.persist()
	reply.Success = true
	// 更新节点commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		go rf.applyLog()
	}
}


// leader 广播心跳
func (rf *Raft) broadcastAppend() {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	for i := 0; i < len(rf.peers.Peers); i++ {
		if rf.role != Leader {
			continue
		}
		go func(endpoint string) {
			args := AppendEntriesArgs{
				Term:         rf.term,
				LeaderId:     rf.peers.Local,
				LeaderCommit: rf.commitIndex,
			}
			// 前一条日志索引
			args.PrevLogIndex = rf.nextIndex[endpoint] - 1
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.logs[args.PrevLogIndex-1].Term
			}
			args.Logs = append(args.Logs, rf.logs[args.PrevLogIndex:]...)
			rf.sendAppendEntries(endpoint, &args, &AppendEntriesReply{})
		}(rf.peers.Peers[i])
	}
}


// 发送并处理心跳
func (rf *Raft) sendAppendEntries(endpoint string, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	conn, err := rf.GetConnection(endpoint)

	if err != nil {
		log.Printf("get conn fail, err: %v", err)
		return
	}

	defer conn.Close()

	err = conn.Call("Raft.AppendEntries", args, reply)

	if err != nil {
		log.Printf("call Raft.AppendEntries fail, endpoint: %v, err: %v", endpoint, err)
		return
	}

	rf.lock.Lock()
	defer rf.lock.Unlock()

	if rf.role != Leader || rf.term != args.Term {
		return
	}


	if reply.Term > rf.term {
		rf.role = Follower
		rf.term = reply.Term
		rf.votedFor = ""
		rf.leaderId = ""
		rf.persist()
		return
	}

	if reply.Success {
		// 同步成功，更新对应follow同步信息
		// RPC期间无锁，应该根据args的PrevLogIndex进行加减
		rf.nextIndex[endpoint] = args.PrevLogIndex +len(args.Logs) + 1
		rf.matchIndex[endpoint] = rf.nextIndex[endpoint] - 1

		// 更新leader commit
		sortMatchIndex := make([]int, len(rf.matchIndex)+1)
		sortMatchIndex = append(sortMatchIndex, rf.lastLogIndex())
		for _, v := range rf.matchIndex {
			sortMatchIndex = append(sortMatchIndex, v)
		}
		sort.Ints(sortMatchIndex)
		// 中位数
		newCommitIndex := sortMatchIndex[(len(rf.peers.Peers)+1)/2]

		if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex-1].Term == rf.term {
			rf.commitIndex = newCommitIndex
			go rf.applyLog()
		}

	} else { //同步失败，回退对应follow要同步日志的起始点

		conflictTermIndex := -1

		if reply.ConflictTerm != -1 { // follow prev.term != leader prev.term
			for i := args.PrevLogIndex; i >= 1; i-- {
				if rf.logs[i-1].Term == reply.ConflictTerm {
					conflictTermIndex = i
					break
				}
			}

			if conflictTermIndex != -1 {
				rf.nextIndex[endpoint] = conflictTermIndex
			}else  {
				rf.nextIndex[endpoint] = reply.ConflictIndex
			}
		}else {  // follow没有prevIndex位置的日志
			rf.nextIndex[endpoint] = reply.ConflictIndex +1
		}
	}
}






func (rf *Raft) applyLog() {
	rf.lock.Lock()
	defer rf.lock.Unlock()
	// 从上一个apply的log开始apply
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.chanApply <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[i-1].Command,
			CommandIndex: i,
			CommandTerm:  rf.logs[i-1].Term,
		}
	}
	rf.lastApplied = rf.commitIndex
}


func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.lock.Lock()
	defer rf.lock.Unlock()
	term = rf.term
	isLeader = rf.role == Leader
	return term, isLeader
}



func (rf *Raft) lastLogIndex() int {
	return len(rf.logs)
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.logs) <= 0 {
		return 0
	}
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) GetConnection(endpoint string) (*rpc.Client, error) {
	cli, err := rpc.DialHTTP("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

