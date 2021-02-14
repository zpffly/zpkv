package shardkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"net/rpc"
	"sync"
	"time"
	"zpkv/raft"
	"zpkv/shardmaster"
)

type Op struct {
	Meth string
	GetArgc  GetArgs
	PutArgc PutAppendArgs
	ReCfg ReconfigureArgs
}

type Result struct {
	args  interface{}
	reply interface{}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	gid          int

	muMsg	sync.Mutex
	sm      *shardmaster.Clerk
	config  shardmaster.Config
	db      [shardmaster.NShards]map[string]string
	taskSeq map[int64]int
	messages map[int]chan Result
}

func (kv *ShardKV) StaleTask(client int64, seq int) bool {
	if lastseq, ok := kv.taskSeq[client]; ok && lastseq >= seq {
		return true
	}
	kv.taskSeq[client] = seq
	return false
}

func (kv *ShardKV) CheckValidKey(key string) bool {
	shardID := key2shard(key)
	if kv.gid != kv.config.Shards[shardID] {
		return false
	}
	return true
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	idx, _, isLeader := kv.rf.Start(Op{Meth: "Get", GetArgc:*args})
	if !isLeader {
		reply.WrongLeader = true
		return nil
	}

	kv.muMsg.Lock()
	ch, ok := kv.messages[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.messages[idx] = ch
	}
	kv.muMsg.Unlock()

	select {
	case msg := <- ch:
		if ret, ok := msg.args.(GetArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.Client != ret.Client || args.Seq != ret.Seq {
				reply.WrongLeader = true
			} else {
				*reply = msg.reply.(GetReply)
				reply.WrongLeader = false
			}
		}
	case <- time.After(400 * time.Millisecond):
		reply.WrongLeader = true
	}
	return nil
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	idx, _, isLeader := kv.rf.Start(Op{Meth: "PutAppend", PutArgc:*args})
	if !isLeader {
		reply.WrongLeader = true
		return nil
	}
	kv.muMsg.Lock()
	ch, ok := kv.messages[idx]
	if !ok {
		ch = make(chan Result, 1)
		kv.messages[idx] = ch
	}
	kv.muMsg.Unlock()

	select {
	case msg := <- ch:
		if ret, ok := msg.args.(PutAppendArgs); !ok {
			reply.WrongLeader = true
		} else {
			if args.Client != ret.Client || args.Seq != ret.Seq {
				reply.WrongLeader = true
			} else {
				reply.Err = msg.reply.(PutAppendReply).Err
				reply.WrongLeader = false
			}
		}
	case <- time.After(400 * time.Millisecond):
		reply.WrongLeader = true
	}
	return nil
}

func (kv *ShardKV) SyncConfigure(args ReconfigureArgs) bool {
	for i := 0; i < 3; i++ {
		idx, _, isLeader := kv.rf.Start(Op{Meth:"Reconfigure", ReCfg:args})
		if !isLeader {
			return false
		}

		kv.muMsg.Lock()
		ch, ok := kv.messages[idx]
		if !ok {
			ch = make(chan Result, 1)
			kv.messages[idx] = ch
		}
		kv.muMsg.Unlock()

		select {
		case msg := <- ch:
			if ret, ok := msg.args.(ReconfigureArgs); !ok {
				return ret.Cfg.Num == args.Cfg.Num
			}
		case <- time.After(150 * time.Millisecond):
			continue
		}
	}
	return false
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.config.Num < args.CfgNum {
		reply.Err = ErrNotReady
		return nil
	}

	reply.Err = OK
	reply.TaskSeq = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		reply.Content[i] = make(map[string]string)
	}

	for _, shardIdx := range args.Shards {
		for k, v := range kv.db[shardIdx] {
			reply.Content[shardIdx][k] = v
		}
	}

	for cli := range kv.taskSeq {
		reply.TaskSeq[cli] = kv.taskSeq[cli]
	}

	return nil
}


func StartServer(raftConfigPath, sharkMasterClerkPath string, gid int) *ShardKV {

	gob.Register(Op{})

	kv := new(ShardKV)
	kv.gid = gid
	kv.applyCh = make(chan raft.ApplyMsg, 32)
	kv.rf = raft.MakeRaftServer(raftConfigPath, kv.applyCh)
	kv.messages = make(map[int]chan Result)
	kv.taskSeq = make(map[int64]int)
	kv.sm = shardmaster.MakeClerk(sharkMasterClerkPath)
	//kv.config = kv.sm.Query(-1)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}

	//go kv.checkLoop(masters)
	go kv.loop()
	go kv.pollConfig()
	return kv
}

func (kv *ShardKV) loop() {
	for entry := range kv.applyCh {
		request := entry.Command.(Op)
		var result Result
		switch request.Meth {
		case "Get":
			result.args = request.GetArgc
			result.reply = kv.ApplyGet(request.GetArgc)
		case "PutAppend":
			result.args = request.PutArgc
			result.reply = kv.ApplyPutAppend(request.PutArgc)
		case "Reconfigure":
			result.args = request.ReCfg
			result.reply = kv.ApplyReconfigure(request.ReCfg)
		}

		kv.sendMessage(entry.CommandIndex, result)
	}
}

func (kv *ShardKV) sendMessage(index int, result Result) {
	kv.muMsg.Lock()
	defer kv.muMsg.Unlock()

	if _, ok := kv.messages[index]; !ok {
		kv.messages[index] = make(chan Result, 1)
	} else {
		select {
		case <-kv.messages[index]:
		default:
		}
	}
	kv.messages[index] <- result
}


func (kv *ShardKV) readSnatshot(data []byte) {
	var lastIncludeIndex, lastIncludeTerm int

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&lastIncludeIndex)
	d.Decode(&lastIncludeTerm)
	kv.mu.Lock()
	d.Decode(&kv.db)
	d.Decode(&kv.taskSeq)
	d.Decode(&kv.config)
	kv.mu.Unlock()
}

func (kv *ShardKV) ApplyGet(args GetArgs) GetReply {
	var reply GetReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}
	kv.mu.Lock()
	if value, ok := kv.db[key2shard(args.Key)][args.Key]; ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.Unlock()
	return reply
}

func (kv *ShardKV) ApplyPutAppend(args PutAppendArgs) PutAppendReply {
	var reply PutAppendReply
	if !kv.CheckValidKey(args.Key) {
		reply.Err = ErrWrongGroup
		return reply
	}

	kv.mu.Lock()
	if !kv.StaleTask(args.Client, args.Seq) {
		if args.Op == "Put" {
			kv.db[key2shard(args.Key)][args.Key] = args.Value
		} else {
			kv.db[key2shard(args.Key)][args.Key] += args.Value
		}
	}
	reply.Err = OK
	kv.mu.Unlock()
	return reply
}

func (kv *ShardKV) ApplyReconfigure(args ReconfigureArgs) ReconfigureReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var reply ReconfigureReply
	if args.Cfg.Num > kv.config.Num {
		for shardIdx, data := range args.Content {
			for k, v := range data {
				kv.db[shardIdx][k] = v
			}
		}

		for cli := range args.TaskSeq {
			if seq, exist := kv.taskSeq[cli]; !exist || seq < args.TaskSeq[cli] {
				kv.taskSeq[cli] = args.TaskSeq[cli]
			}
		}
		kv.config = args.Cfg
		reply.Err = OK
	}
	return reply
}

func (kv *ShardKV) pollConfig() {
	var timeoutChan <-chan time.Time
	for true {
		if _, isLeader := kv.rf.GetState(); isLeader {
			newCfg := kv.sm.Query(-1)
			for i := kv.config.Num + 1; i <= newCfg.Num; i++ {
				if !kv.Reconfigure(kv.sm.Query(i)) {
					break
				}
			}
		}
		timeoutChan = time.After(100 * time.Millisecond)
		<-timeoutChan
	}
}

func (kv *ShardKV) pullShard(gid int, args *GetShardArgs, reply *GetShardReply) bool {
	for _, server := range kv.config.Groups[gid] {
		conn, err := kv.getConnection(server)
		if err != nil {
			log.Printf("get conn fail, err: %v", err)
			return false
		}
		err = conn.Call("ShardKV.GetShard", args, reply)
		if err == nil {
			if reply.Err == OK {
				return true
			} else if reply.Err == ErrNotReady {
				return false
			}
		}
	}
	return false
}

func (kv *ShardKV) Reconfigure(newCfg shardmaster.Config) bool {
	ret := ReconfigureArgs{Cfg:newCfg}
	ret.TaskSeq = make(map[int64]int)
	for i := 0; i < shardmaster.NShards; i++ {
		ret.Content[i] = make(map[string]string)
	}
	isOK := true

	mergeShards := make(map[int][]int)
	for i := 0; i < shardmaster.NShards; i++ {
		if newCfg.Shards[i] == kv.gid && kv.config.Shards[i] != kv.gid {
			gid := kv.config.Shards[i]
			if gid != 0 {
				if _, ok := mergeShards[gid]; !ok {
					mergeShards[gid] = []int{i}
				} else {
					mergeShards[gid] = append(mergeShards[gid], i)
				}
			}
		}
	}

	var retMu sync.Mutex
	var wait sync.WaitGroup
	for gid, value := range mergeShards {
		wait.Add(1)
		go func(gid int, value []int) {
			defer wait.Done()
			var reply GetShardReply

			if kv.pullShard(gid, &GetShardArgs{CfgNum: newCfg.Num, Shards:value}, &reply) {
				retMu.Lock()
				for shardIdx, data := range reply.Content {
					for k, v := range data {
						ret.Content[shardIdx][k] = v
					}
				}

				for cli := range reply.TaskSeq {
					if seq, exist := ret.TaskSeq[cli]; !exist || seq < reply.TaskSeq[cli] {
						ret.TaskSeq[cli] = reply.TaskSeq[cli]
					}
				}
				retMu.Unlock()
			} else {
				isOK = false
			}
		} (gid, value)
	}
	wait.Wait()
	return isOK && kv.SyncConfigure(ret)
}


func (kv *ShardKV) getConnection(endpoint string) (*rpc.Client, error) {
	cli, err := rpc.DialHTTP("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	return cli, nil
}