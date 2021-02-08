package main

import (
	"time"
	"zpkv/raft"
)

func main() {

	apply := make(chan raft.ApplyMsg, 10)
	raft.MakeRaftServer("C:\\Users\\16678\\zpkv\\config\\c3.json", apply)

	time.Sleep(time.Hour)
}
