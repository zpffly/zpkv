package main

import (
	"fmt"
	"log"
	"time"
	"zpkv/shardkv"
)

func main() {

	log.SetFlags(log.Lshortfile)

	//shardmaster.StartServer("config\\shardmaster_c3.json")
	//shardkv.StartServer("config\\shardkv_c3.json", "config\\master_server.json", 1)

	//clerk := shardmaster.MakeClerk("C:\\Users\\16678\\zpkv\\config\\master_server.json")
	//query := clerk.Query(-1)
	//fmt.Println(query)

	clerk := shardkv.MakeClerk("config\\master_server.json")
	//clerk.Put("111", "222")
	get := clerk.Get("111")
	fmt.Printf(get)

	fmt.Println("\nsuccess")

	time.Sleep(time.Hour * 6)
}
