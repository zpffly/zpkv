package shardmaster

import (
	"crypto/rand"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/big"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Clerk struct {
	servers []string
	lastLeader	int
	identity	int64
	seq			int
	mu 			sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(configPath string) *Clerk {
	ck := new(Clerk)
	var servers []string

	f, err := os.Open(configPath)
	if err != nil {
		log.Fatal(err)
	}

	bytes, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}

	err = json.Unmarshal(bytes, &servers)
	if err != nil {
		log.Fatal(err)
	}
	ck.servers = servers
	ck.lastLeader = 0
	ck.identity = nrand()
	ck.seq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	args.Num = num
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		// try each known server.
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply QueryReply
			conn, err := ck.getConnection(ck.servers[i])
			if err != nil {
				log.Printf("shard client get conn fail, server: %v, err: %v", ck.servers[i], err)
				continue
			}
			err = conn.Call("ShardMaster.Query", args, &reply)
			if err == nil && reply.WrongLeader == false {
				ck.lastLeader = i
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	args.Servers = servers
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply JoinReply
			conn, err := ck.getConnection(ck.servers[i])
			if err != nil {
				log.Printf("shard client get conn fail, server: %v, err: %v", ck.servers[i], err)
				continue
			}
			err = conn.Call("ShardMaster.Join", args, &reply)
			if err == nil && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	args.GIDs = gids
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()
	for {
		// try each known server.
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply LeaveReply
			conn, err := ck.getConnection(ck.servers[i])
			if err != nil {
				log.Printf("shard client get conn fail, server: %v, err: %v", ck.servers[i], err)
				continue
			}
			err = conn.Call("ShardMaster.Leave", args, &reply)
			if err == nil && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.Client = ck.identity
	ck.mu.Lock()
	args.Seq = ck.seq
	ck.seq++
	ck.mu.Unlock()

	for {
		// try each known server.
		for i, n := ck.lastLeader, len(ck.servers); ; i = (i + 1) % n {
			var reply MoveReply
			conn, err := ck.getConnection(ck.servers[i])
			if err != nil {
				log.Printf("shard client get conn fail, server: %v, err: %v", ck.servers[i], err)
				continue
			}
			err = conn.Call("ShardMaster.Move", args, &reply)
			if err == nil && reply.WrongLeader == false {
				ck.lastLeader = i
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}


func (ck *Clerk) getConnection(endpoint string) (*rpc.Client, error) {
	cli, err := rpc.DialHTTP("tcp", endpoint)
	if err != nil {
		return nil, err
	}
	return cli, nil
}