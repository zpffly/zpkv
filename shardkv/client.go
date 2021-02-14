package shardkv

import (
	"log"
	"net/rpc"
	"sync"
	"time"
	"zpkv/shardmaster"
)

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	identity 	int64
	seq 		int
	mu 			sync.Mutex
}

// 初始化client
func MakeClerk(configPath string) *Clerk {
	ck := new(Clerk)
	// TODO 初始化配置集群
	ck.sm = shardmaster.MakeClerk(configPath)
	ck.config = ck.sm.Query(-1)
	ck.identity = nrand()
	ck.seq = 0
	return ck
}


func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		//fmt.Printf("cur key: %s shard %d and gid %d\n", key, shard, gid)
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				conn, err := ck.getConnection(servers[si])
				if err != nil {
					log.Printf("get conn fail, err: %v", err)
					return ""
				}
				var reply GetReply
				err = conn.Call("ShardKV.Get", &args, &reply)

				if err == nil && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey){
					return reply.Value
				}
				if err == nil && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
		//fmt.Printf("NUM:%d, %s,  %s\n", ck.config.Num, ck.config.Shards, ck.config.Groups)
	}

}


func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				conn, err := ck.getConnection(servers[si])
				if err != nil {
					log.Printf("get conn fail, err: %v", err)
					return
				}
				var reply PutAppendReply
				err = conn.Call("ShardKV.PutAppend", &args, &reply)
				if err == nil && reply.WrongLeader == false && reply.Err == OK {
					return
				}
				if err == nil && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}


func (ck *Clerk) getConnection(endpoint string) (*rpc.Client, error) {
	cli, err := rpc.DialHTTP("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	return cli, nil
}