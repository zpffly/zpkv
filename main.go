package main

import (
	"time"
	"zpkv/shardmaster"
)

func main() {


	shardmaster.StartServer("config\\shardmaster_c1.json")

	//clerk := shardmaster.MakeClerk("C:\\Users\\16678\\zpkv\\config\\master_server.json")
	//clerk.Join(map[int][]string{1: {"11", "22"}})
	//clerk.Leave([]int{1})
	//clerk.Move(1, 2)

	//fmt.Println("success")

	time.Sleep(time.Hour * 6)
}
