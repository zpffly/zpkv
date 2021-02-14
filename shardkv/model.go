package shardkv

import "zpkv/shardmaster"

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrStaleKey   = "ErrStaleKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNotReady   = "ErrNotReady"
	ErrBadConfig  = "ErrBadConfig"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Op     string // "Put" or "Append"
	Client int64
	Seq    int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key    string
	Client int64
	Seq    int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type GetShardArgs struct {
	Shards []int // 希望获取的shard列表
	CfgNum int   // 希望获取到的配置文件编号
}

type GetShardReply struct {
	WrongLeader bool
	Err         Err
	Content     [shardmaster.NShards]map[string]string // 分片对应的数据
	TaskSeq     map[int64]int
}

type ReconfigureArgs struct {
	Cfg     shardmaster.Config
	Content [shardmaster.NShards]map[string]string
	TaskSeq map[int64]int
}

type ReconfigureReply struct {
	Err Err
}
