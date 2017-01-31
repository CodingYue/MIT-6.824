package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "reflect"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key       string
	Value     string
	Operation string
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid    int64 // my replica group ID
	config shardmaster.Config

	lastApply int
	database  map[string]string
}

func (kv *ShardKV) Apply(op Op) {
	switch op.Operation {
	case "Get":
		break
	case "Put":
		kv.database[op.Key] = op.Value
	case "Append":
		value := kv.database[op.Key]
		kv.database[op.Key] = value + op.Value
	default:
		break
	}
	kv.lastApply++
}

func (kv *ShardKV) Wait(seq int) Op {
	sleepTime := 10 * time.Microsecond
	for {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op)
		}
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
}

func (kv *ShardKV) Propose(op Op) {
	for seq := kv.lastApply; ; seq++ {
		if seq == 0 {
			continue
		}
		kv.px.Start(seq, op)
		value := kv.Wait(seq)
		if seq > kv.lastApply {
			kv.Apply(value)
		}
		if reflect.DeepEqual(value, op) {
			break
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}
	op := Op{Operation: "Get", Key: args.Key, Value: ""}
	kv.Propose(op)
	value, ok := kv.database[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = "NO KEY"
	} else {
		reply.Value = value
		reply.Err = OK
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}
	op := Op{Operation: args.Op, Key: args.Key, Value: args.Value}
	kv.Propose(op)
	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newConfig := kv.sm.Query(-1)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.config.Shards[i] == kv.gid && kv.config.Shards[i] != newConfig.Shards[i] {
			for k, v := range kv.database {
				//log.Printf("send %v from %v to %v", k, kv.config.Shards[i], newConfig.Shards[i])
				if key2shard(k) == i {
					log.Printf("send %v from %v to %v", k, kv.config.Shards[i], newConfig.Shards[i])
					for _, srv := range kv.config.Groups[newConfig.Shards[i]] {
						args := &PutAppendArgs{Key: k, Value: v, Op: "Put"}
						reply := PutAppendReply{}
						ok := call(srv, "ShardKV.PutAppend", args, &reply)
						if ok && reply.Err == OK {
							break
						}
					}
				}
			}
		}
	}
	kv.config = newConfig
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}
	kv.database = make(map[string]string)

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
