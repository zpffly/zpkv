package zpkv

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

type Log struct {
	Command interface{}
	Term    int
	Index   int
}
