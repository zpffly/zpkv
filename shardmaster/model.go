package shardmaster

const NShards = 10

type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[], server: ip
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Client  int64
	Seq     int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs   []int
	Client int64
	Seq    int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard  int
	GID    int
	Client int64
	Seq    int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num    int // desired config number
	Client int64
	Seq    int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

const (
	JOIN = iota
	LEAVE
	MOVE
	QUERY
)

type MovePair struct {
	From int
	To   int
}
