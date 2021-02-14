package shardmaster

import (
	"bytes"
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
	"zpkv/raft"
	"zpkv/utils"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	muCfg   sync.Mutex
	configs []Config
	lastIdx int
	reflect map[int]chan bool
	taskSeq map[int64]int
}

type Op struct {
	Meth    int
	Client  int64
	Seq     int
	Gid     int
	Shard   int
	Num     int
	Servers map[int][]string
	GIDs    []int
}

func (sm *ShardMaster) StaleTask(client int64, seq int) bool {
	if lastSeq, ok := sm.taskSeq[client]; ok {
		return lastSeq >= seq
	}
	return false
}

func (sm *ShardMaster) DuplicateLog(entry *Op) bool {
	idx, _, isLeader := sm.rf.Start(*entry)
	if !isLeader {
		return false
	}

	sm.mu.Lock()
	ch, ok := sm.reflect[idx]
	if !ok {
		ch = make(chan bool, 1)
		sm.reflect[idx] = ch
	}
	sm.mu.Unlock()

	select {
	case ret := <-ch:
		return ret
	case <-time.After(300 * time.Millisecond):
		return false
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	command := Op{Meth: JOIN, Client: args.Client, Seq: args.Seq, Servers: args.Servers}
	if ok := sm.DuplicateLog(&command); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	command := Op{Meth: LEAVE, Client: args.Client, Seq: args.Seq, GIDs: args.GIDs}
	if ok := sm.DuplicateLog(&command); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	command := Op{Meth: MOVE, Client: args.Client, Seq: args.Seq, Gid: args.GID, Shard: args.Shard}
	if ok := sm.DuplicateLog(&command); !ok {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		reply.Err = OK
	}
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	command := Op{Meth: QUERY, Client: args.Client, Seq: args.Seq, Num: args.Num}
	if ok := sm.DuplicateLog(&command); !ok {
		reply.WrongLeader = true
	} else if !sm.StaleTask(command.Client, command.Seq) {
		reply.WrongLeader = false
		reply.Err = OK
		sm.muCfg.Lock()
		reply.Config = sm.doQuery(args.Num)
		sm.muCfg.Unlock()
	}
	return nil
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func StartServer(configPath string) *ShardMaster {
	sm := new(ShardMaster)

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.MakeRaftServer(configPath, sm.applyCh)

	rpc.Register(sm)
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", sm.rf.Config.ServerMaster)

	if err != nil {
		log.Fatalf("init server fail, server: %v, err: %v", sm.rf.Config.ServerMaster, err)
	}

	go http.Serve(l, nil)


	sm.reflect = make(map[int]chan bool)
	sm.taskSeq = make(map[int64]int)
	sm.lastIdx = 0

	go sm.loop()
	return sm
}

func (sm *ShardMaster) loop() {

	for entry := range sm.applyCh {
		command := entry.Command.(Op)
		utils.DPrintf("commond: %v", command)
		if command.Meth == QUERY || sm.StaleTask(command.Client, command.Seq) {
			goto D
		}

		switch command.Meth {
		case JOIN:
			sm.doJoin(command.Servers)
		case LEAVE:
			sm.doLeave(command.GIDs)
		case MOVE:
			sm.doMove(command.Gid, command.Shard)
			/*case QUERY:
			sm.doQuery(command.Num)*/
		}
		sm.taskSeq[command.Client] = command.Seq
	D:
		sm.mu.Lock()
		ch, ok := sm.reflect[entry.CommandIndex]
		if ok {
			select {
			case <-sm.reflect[entry.CommandIndex]:
			default:
			}
			ch <- true
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) doJoin(servers map[int][]string) {
	cfg := sm.ChangeConfig()
	for gid, srvs := range servers {
		if _, ok := cfg.Groups[gid]; !ok {
			cfg.Groups[gid] = srvs
			sm.rebalance(gid, false)
		}
	}
}

func (sm *ShardMaster) doLeave(gids []int) {
	cfg := sm.ChangeConfig()
	for _, gid := range gids {
		if _, ok := cfg.Groups[gid]; ok {
			delete(cfg.Groups, gid)
			sm.rebalance(gid, true)
		}
	}
}

func (sm *ShardMaster) doMove(gid int, shard int) {
	cfg := sm.ChangeConfig()
	cfg.Shards[shard] = gid
}

func (sm *ShardMaster) doQuery(num int) Config {
	if -1 == num || num > sm.lastIdx {
		return sm.configs[sm.lastIdx]
	} else {
		return sm.configs[num]
	}
}

func (sm *ShardMaster) ChangeConfig() *Config {
	oldCfg := &sm.configs[sm.lastIdx]
	var newCfg Config
	newCfg.Num = oldCfg.Num + 1
	newCfg.Shards = [NShards]int{}
	newCfg.Groups = make(map[int][]string)
	//deep copy
	for i, g := range oldCfg.Shards {
		newCfg.Shards[i] = g
	}

	for g, srvs := range oldCfg.Groups {
		newCfg.Groups[g] = srvs
	}
	sm.muCfg.Lock()
	defer sm.muCfg.Unlock()
	sm.lastIdx++
	sm.configs = append(sm.configs, newCfg)
	return &sm.configs[sm.lastIdx]
}

//snatshot
func (sm *ShardMaster) readSnatshot(data []byte) {
	var lastIncludeIndex, lastIncludeTerm int

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	sm.muCfg.Lock()
	d.Decode(&sm.configs)
	d.Decode(&sm.lastIdx)
	d.Decode(&sm.taskSeq)
	sm.muCfg.Unlock()
}

func (sm *ShardMaster) rebalance(gid int, isLeave bool) {
	c := &sm.configs[sm.lastIdx]
	for i := 0; ; i++ {
		pair := GidMovePair(c)
		if isLeave {
			s := GetShardByGid(gid, c)
			if -1 == s {
				return
			}
			c.Shards[s] = pair.To
		} else {
			if i == NShards/len(c.Groups) {
				return
			}
			s := GetShardByGid(pair.From, c)
			c.Shards[s] = gid
		}
	}
}

func GidMovePair(c *Config) MovePair {
	min_id, min_num, max_id, max_num := 0, int(^uint(0)>>1), 0, -1
	counts := make(map[int]int)
	for g := range c.Groups {
		counts[g] = 0
	}
	for _, g := range c.Shards {
		counts[g]++
	}

	for g := range counts {
		_, ok := c.Groups[g]
		if ok && min_num > counts[g] {
			min_id, min_num = g, counts[g]
		}
		if ok && max_num < counts[g] {
			max_id, max_num = g, counts[g]
		}
	}

	for _, g := range c.Shards {
		if 0 == g {
			max_id = 0
		}
	}
	return MovePair{max_id, min_id}
}

func GetShardByGid(gid int, c *Config) int {
	for s, g := range c.Shards {
		if g == gid {
			return s
		}
	}
	return -1
}
