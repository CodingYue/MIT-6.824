package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "reflect"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	lastApply int
	configs   []Config // indexed by config num
}

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	Operation string
	Args      interface{}
}

func (sm *ShardMaster) Rebalance(config *Config, deleteGID int64) {

	// groupUsed := make(map[int64]int)
	// for k, _ := range config.Groups {
	// 	groupUsed[k] = 0
	// }
	// for i := 0; i < NShards; i++ {
	// 	used := -1
	// 	best := int64(-1)
	// 	for k, v := range groupUsed {
	// 		if best == -1 || used > v {
	// 			used = v
	// 			best = k
	// 		}
	// 	}
	// 	config.Shards[i] = best
	// 	groupUsed[best]++
	// }
	nGroup := len(config.Groups)
	limit := NShards / nGroup

	for i := 0; i < NShards; i++ {
		if config.Shards[i] == deleteGID {
			config.Shards[i] = 0
		}
	}

	counts := map[int64]int{}
	for i := 0; i < NShards; i++ {
		counts[config.Shards[i]]++
	}

	for i := 0; i < NShards; i++ {
		if config.Shards[i] == 0 || counts[config.Shards[i]] > limit {
			for k := range config.Groups {
				if counts[k] < limit {
					counts[config.Shards[i]]--
					counts[k]++
					config.Shards[i] = k
				}
			}
		}
	}

}

func (sm *ShardMaster) Apply(op Op) {
	lastConfig := sm.configs[sm.lastApply]
	var newConfig Config
	newConfig.Num = lastConfig.Num
	newConfig.Groups = make(map[int64][]string)
	for k, v := range lastConfig.Groups {
		newConfig.Groups[k] = v
	}
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = lastConfig.Shards[i]
	}

	switch op.Operation {
	case Join:
		joinArgs := op.Args.(JoinArgs)
		newConfig.Groups[joinArgs.GID] = joinArgs.Servers
		newConfig.Num++
		sm.Rebalance(&newConfig, 0)

	case Leave:
		leaveArgs := op.Args.(LeaveArgs)
		delete(newConfig.Groups, leaveArgs.GID)
		newConfig.Num++
		sm.Rebalance(&newConfig, leaveArgs.GID)

	case Move:
		moveArgs := op.Args.(MoveArgs)
		newConfig.Shards[moveArgs.Shard] = moveArgs.GID
		newConfig.Num++
	case Query:
		sm.px.Done(sm.lastApply)
	default:
		break
	}
	sm.configs = append(sm.configs, newConfig)
	sm.lastApply++
}

func (sm *ShardMaster) Wait(seq int) Op {
	sleepTime := 10 * time.Microsecond
	for {
		decided, value := sm.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op)
		}
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
}

func (sm *ShardMaster) Propose(op Op) {
	// log.Printf("%v", op)
	seq := sm.lastApply + 1
	for {
		sm.px.Start(seq, op)
		value := sm.Wait(seq)
		sm.Apply(value)
		if reflect.DeepEqual(value, op) {
			break
		}
		seq++
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Args: *args, Operation: Join}
	sm.Propose(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Args: *args, Operation: Leave}
	sm.Propose(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Args: *args, Operation: Move}
	sm.Propose(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op := Op{Args: *args, Operation: Query}
	sm.Propose(op)
	for i := 0; i < sm.lastApply; i++ {
		if sm.configs[i].Num == args.Num {
			reply.Config = sm.configs[i]
			return nil
		}
	}
	reply.Config = sm.configs[sm.lastApply]
	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
