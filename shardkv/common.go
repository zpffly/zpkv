package shardkv

import (
	"crypto/rand"
	"math/big"
	"zpkv/shardmaster"
)

// 计算key属于哪个分片
func key2shard(key string) int {
	shardId := 0
	if len(key) > 0 {
		shardId = int(key[0])
	}
	shardId %= shardmaster.NShards
	return shardId
}


// 随机生成int
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
