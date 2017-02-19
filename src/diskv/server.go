package diskv

import (
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"persistence"
	"reflect"
	"shardmaster"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardState struct {
	MaxClientSeq map[int64]int
	Database     map[string]string
}

type Op struct {
	Operation string
	Value     interface{}
}

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	config     shardmaster.Config
	lastApply  int
	shardState map[int]*ShardState
	received   map[int]bool

	modifiedShardBuffer map[int]bool
	enablePersistence   bool
}

type disKVstatus struct {
	Config    shardmaster.Config
	LastApply int
	Received  map[int]bool
}

func (kv *DisKV) persistenceStatus() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.enablePersistence {
		return
	}
	success := persistence.ReadTransactionSuccess(kv.dir)
	if err := persistence.SyncTempfile(kv.dir, success); err != nil {
		panic(err)
	}
	persistence.WriteFile(kv.dir, "transaction_success", false)

	status := disKVstatus{
		Config:    kv.config,
		LastApply: kv.lastApply,
		Received:  kv.received}

	persistence.WriteTempFile(kv.dir, "kv_status", status)
	DPrintf("Persist shard : %v, gid %v, me %v", kv.modifiedShardBuffer, kv.gid, kv.me)
	for shard, isModified := range kv.modifiedShardBuffer {
		if !isModified {
			continue
		}
		DPrintf("persist shard status gid %v, me %v, %v", kv.gid, kv.me, kv.shardState[shard])
		persistence.WriteTempFile(kv.dir, "shard_state-"+strconv.Itoa(shard),
			kv.shardState[shard])
	}
	persistence.WriteFile(kv.dir, "transaction_success", true)
	kv.modifiedShardBuffer = map[int]bool{}
	kv.px.Done(kv.lastApply)
}

func MakeShardState() *ShardState {
	shardState := ShardState{}
	shardState.Database = make(map[string]string)
	shardState.MaxClientSeq = make(map[int64]int)
	return &shardState
}

func (kv *DisKV) Apply(op Op, seq int) {

	DPrintf("Apply %v, gid %v, me %v, seq %v", op, kv.gid, kv.me, seq)
	switch op.Operation {
	case "Get":
		if op.Value != nil {
			args := op.Value.(GetArgs)
			DPrintf("Get %v, %v", args.Key, kv.shardState[args.Shard].Database[args.Key])

			if args.Seq > kv.shardState[args.Shard].MaxClientSeq[args.ID] {
				kv.shardState[args.Shard].MaxClientSeq[args.ID] = args.Seq
				kv.modifiedShardBuffer[args.Shard] = true
			}
		}
	case "Put":
		args := op.Value.(PutAppendArgs)
		stateMachine := kv.shardState[args.Shard]
		stateMachine.Database[args.Key] = args.Value

		if args.Seq > kv.shardState[args.Shard].MaxClientSeq[args.ID] {
			kv.shardState[args.Shard].MaxClientSeq[args.ID] = args.Seq
		}
		kv.modifiedShardBuffer[args.Shard] = true
	case "Append":
		args := op.Value.(PutAppendArgs)
		stateMachine := kv.shardState[args.Shard]

		value, ok := stateMachine.Database[args.Key]
		if !ok {
			value = ""
		}
		stateMachine.Database[args.Key] = value + args.Value

		DPrintf("After append, %v", kv.shardState[args.Shard].Database[args.Key])

		if args.Seq > kv.shardState[args.Shard].MaxClientSeq[args.ID] {
			kv.shardState[args.Shard].MaxClientSeq[args.ID] = args.Seq
		}
		kv.modifiedShardBuffer[args.Shard] = true
	case "Update":
		args := op.Value.(UpdateArgs)
		stateMachine := kv.shardState[args.Shard]

		kv.received[args.Shard] = true
		DPrintf("Update Recieved, config num %v, shard %d, gid %d, me %d",
			kv.config.Num, args.Shard, kv.gid, kv.me)
		stateMachine.Database = args.Database
		stateMachine.MaxClientSeq = args.MaxClientSeq

		if args.Seq > kv.shardState[args.Shard].MaxClientSeq[args.ID] {
			kv.shardState[args.Shard].MaxClientSeq[args.ID] = args.Seq
		}
		kv.modifiedShardBuffer[args.Shard] = true
	default:
		break
	}
	kv.lastApply++
	DPrintf("After apply modified shard buffer : %v", kv.modifiedShardBuffer)
}

func (kv *DisKV) Wait(seq int) (Op, error) {
	sleepTime := 10 * time.Microsecond
	for iters := 0; iters < 15; iters++ {
		decided, value := kv.px.Status(seq)
		if decided == paxos.Decided {
			return value.(Op), nil
		}
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
	return Op{}, errors.New("DisKV, Wait for too long")
}

func (kv *DisKV) Propose(op Op) error {
	for seq := kv.lastApply + 1; ; seq++ {
		kv.px.Start(seq, op)
		value, err := kv.Wait(seq)
		if err != nil {
			return err
		}
		if seq > kv.lastApply {
			kv.Apply(value, seq)
		}
		if reflect.DeepEqual(value, op) {
			break
		}
	}
	return nil
}

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}
	// if args.Seq > kv.shardState[args.Shard].maxClientSeq[args.ID] {
	op := Op{Operation: "Get", Value: *args}
	if err := kv.Propose(op); err != nil {
		return err
	}
	// }
	value, ok := kv.shardState[args.Shard].Database[args.Key]
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
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// DPrintf("config num %v, args %v", kv.config.Num, *args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}
	if args.Seq <= kv.shardState[args.Shard].MaxClientSeq[args.ID] {
		reply.Err = OK
		return nil
	}
	op := Op{Operation: args.Op, Value: *args}
	if err := kv.Propose(op); err != nil {
		return err
	}
	reply.Err = OK
	return nil
}

func (kv *DisKV) Update(args *UpdateArgs, reply *UpdateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	//DPrintf("args %v", *args)

	if args.Seq <= kv.shardState[args.Shard].MaxClientSeq[args.ID] {
		reply.Err = OK
		return nil
	}
	if kv.config.Num != args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}
	op := Op{Operation: "Update", Value: *args}
	if err := kv.Propose(op); err != nil {
		return err
	}
	reply.Err = OK
	return nil
}

func (kv *DisKV) Send(shard int, newConfig shardmaster.Config) {

	args := &UpdateArgs{
		Shard:        shard,
		ID:           kv.gid,
		Seq:          kv.config.Num,
		ConfigNum:    kv.config.Num,
		Database:     kv.shardState[shard].Database,
		MaxClientSeq: kv.shardState[shard].MaxClientSeq}
	reply := UpdateReply{}

	gid := newConfig.Shards[shard]
	servers, ok := newConfig.Groups[gid]
	for {

		if ok {
			for _, srv := range servers {
				DPrintf("Send shard %d to gid %d, srv %v, args %v", shard, gid, srv, args)
				ok := call(srv, "DisKV.Update", args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					DPrintf("Success: shard %d to gid %d, srv %v, args %v", shard, gid, srv, args)
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					DPrintf("Err group: shard %d to gid %d, srv %v, args %v", shard, gid, srv, args)
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	newConfig := kv.sm.Query(kv.config.Num + 1)

	if newConfig.Num != kv.config.Num {
		isProducer := false
		isConsumer := false

		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.config.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
				isProducer = true
			}
			if kv.config.Shards[shard] != 0 &&
				kv.config.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
				isConsumer = true
			}
		}
		op := Op{Operation: "Get"}

		if isProducer {
			kv.Propose(op)
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
					kv.Send(shard, newConfig)
				}
			}
		}
		if isConsumer {
			kv.Propose(op)
			allRecieved := true
			for shard := 0; shard < shardmaster.NShards; shard++ {
				if kv.config.Shards[shard] != 0 && kv.config.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
					if !kv.received[shard] {
						allRecieved = false
						DPrintf("gid %v, me %v, shard %v not recieved, Config %v", kv.gid, kv.me, shard, kv.config.Num)
						break
					}
				}
			}
			if !allRecieved {
				return
			}
		}

		DPrintf("gid %d, me %d Config promote %v -> %v", kv.gid, kv.me, kv.config.Num, newConfig.Num)
		for shard := 0; shard < shardmaster.NShards; shard++ {
			if kv.config.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
				kv.shardState[shard] = MakeShardState()
				kv.modifiedShardBuffer[shard] = true
			}
		}
		kv.config = newConfig
		kv.received = make(map[int]bool)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

func (kv *DisKV) HandleRecovery(args *RecoveryArgs, reply *RecoveryReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.KV = disKVstatus{
		Config:    kv.config,
		LastApply: kv.lastApply,
		Received:  kv.received}
	reply.KVShardState = map[int]ShardState{}
	for k, v := range kv.shardState {
		reply.KVShardState[k] = *v
	}
	reply.Paxos, reply.PaxosInstance = kv.px.GetRecoveryStatus()
	// DPrintf("reply paxos donemax %v", reply.Paxos.DoneMax)
	DPrintf("Reply shard, lastapply %v, status %v", kv.lastApply, reply.KVShardState)
	DPrintf("Reply paxos instance %v", reply.PaxosInstance)
	return nil
}

func (kv *DisKV) restoreReplica(status disKVstatus, shardStatus map[int]ShardState) {
	kv.config = status.Config
	kv.lastApply = status.LastApply
	kv.received = status.Received
	// for k, v := range shardStatus {
	// 	kv.shardState[k] = &v
	// }
	for shard, status := range shardStatus {
		newStatus := status
		kv.shardState[shard] = &newStatus
		// kv.shardState[shard].Database = status.Database
		// kv.shardState[shard].MaxClientSeq = status.MaxClientSeq
	}
	DPrintf("Restore replica gid %v, me %v, last apply %v, shard %v", kv.gid, kv.me, kv.lastApply, shardStatus)
	kv.persistenceStatus()
}

//
// Start a DisKV server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {
	gob.Register(Op{})
	gob.Register(PutAppendArgs{})
	gob.Register(UpdateArgs{})
	gob.Register(GetArgs{})

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir + "/key_value"
	kv.modifiedShardBuffer = map[int]bool{}
	kv.enablePersistence = true
	kv.shardState = map[int]*ShardState{}
	for shard := 0; shard < shardmaster.NShards; shard++ {
		kv.shardState[shard] = MakeShardState()
	}
	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	var hasInited bool
	if err := persistence.ReadFile(dir, "has_inited", &hasInited); err != nil {
		hasInited = false
	}
	kv.px = paxos.MakeWithOptions(servers, me, rpcs, dir+"/paxos", restart, hasInited)
	if !restart {
		os.RemoveAll(kv.dir)
		kv.config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}
		kv.shardState = make(map[int]*ShardState)
		for shard := 0; shard < shardmaster.NShards; shard++ {
			kv.shardState[shard] = MakeShardState()
		}
	} else {
		DPrintf("Restart server gid %v, me %v, dir %v", gid, me, kv.dir)
		if hasInited {
			success := persistence.ReadTransactionSuccess(kv.dir)
			if err := persistence.SyncTempfile(kv.dir, success); err != nil {
				panic(err)
			}
			var kvstatus disKVstatus
			if err := persistence.ReadFile(kv.dir, "kv_status", &kvstatus); err != nil {
				panic(err)
			}
			kv.config = kvstatus.Config
			kv.lastApply = kvstatus.LastApply
			kv.received = kvstatus.Received
			DPrintf("restart kv status %v", kvstatus)
			for shard := 0; shard < shardmaster.NShards; shard++ {
				filename := "shard_state-" + strconv.Itoa(shard)
				if _, err := os.Stat(kv.dir + "/" + filename); err == nil {
					status := ShardState{}
					if err := persistence.ReadFile(kv.dir, filename, &status); err != nil {
						panic(err)
					}
					DPrintf("restart shard state gid %v, me %v, %v", kv.gid, kv.me, status)
					kv.shardState[shard] = &status
				}
			}
		} else {
			// remote restore replica
			os.RemoveAll(dir)

			isBannedServers := map[int]bool{}
			isBannedServers[kv.me] = true

			for count := 0; count < len(servers)/2+1; {

				// DPrintf("Check infinite loop, restore replica, count %v, gid %v, me %v", count, gid, me)

				for idx, srv := range servers {
					if isBannedServers[idx] {
						continue
					}
					args := &RecoveryArgs{}
					reply := RecoveryReply{}
					if ok := call(srv, "DisKV.HandleRecovery", args, &reply); ok {
						isBannedServers[idx] = true
						count++
						if count >= len(servers)/2+1 {
							kv.px.RestoreReplica(reply.Paxos, reply.PaxosInstance)
							kv.restoreReplica(reply.KV, reply.KVShardState)
							break
						}
					}
				}
			}
			// time.Sleep(3 * time.Second)
		}
	}
	persistence.WriteFile(dir, "has_inited", true)

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
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

	go func() {
		for kv.isdead() == false {
			kv.persistenceStatus()
			time.Sleep(1 * time.Second)
		}
	}()

	return kv
}
