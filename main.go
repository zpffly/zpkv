package main

import (
	"fmt"
	"time"
	"zpkv/shardmaster"
)

func main() {

	//apply := make(chan raft.ApplyMsg, 10)
	//raft.MakeRaftServer("C:\\Users\\16678\\zpkv\\config\\c3.json", apply)
	//shardmaster.StartServer("C:\\Users\\16678\\zpkv\\config\\c3.json")

	clerk := shardmaster.MakeClerk("C:\\Users\\16678\\zpkv\\config\\master_server.json")
	//clerk.Join(map[int][]string{1: {"11", "22"}})
	clerk.Leave([]int{1})
	clerk.Move(1, 2)

	fmt.Println("success")

	time.Sleep(time.Hour)
}
