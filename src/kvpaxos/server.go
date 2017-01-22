package kvpaxos

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
	ClientID  int64
	Seq       int
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVPaxos struct {
	mu           sync.Mutex
	l            net.Listener
	me           int
	dead         int32 // for testing
	unreliable   int32 // for testing
	px           *paxos.Paxos
	lastApply    int
	KVMap        map[string]string
	lastApplyLog Op
	maxClientSeq map[int64]int
}

func (kv *KVPaxos) Wait(seq int) interface{} {
	sleepTime := 10 * time.Microsecond
	for {
		decided, decidedValue := kv.px.Status(seq)
		if decided == paxos.Decided {
			return decidedValue
		}
		if decided == paxos.Forgotten {
			break
		}
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
	}
	return nil
}

func (kv *KVPaxos) Apply(op Op) interface{} {
	if op.Operation == "Get" {
		return kv.KVMap[op.Key]
	} else if op.Operation == "Put" {
		kv.KVMap[op.Key] = op.Value
	} else {
		value, ok := kv.KVMap[op.Key]
		if !ok {
			value = ""
		}
		kv.KVMap[op.Key] = value + op.Value
	}
	if maxSeq, ok := kv.maxClientSeq[op.ClientID]; !ok || maxSeq < op.Seq {
		kv.maxClientSeq[op.ClientID] = op.Seq
	}
	// kv.lastApplyLog = op
	// kv.isLogApply[op] = true
	return nil
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Get key %s", args.Key)

	v := Op{Key: args.Key, Operation: "Get", ClientID: args.ClientID, Seq: args.Seq}
	clientID := args.ClientID
	if maxSeq, ok := kv.maxClientSeq[clientID]; ok && args.Seq <= maxSeq {
		reply.Err = OK
		reply.Value = kv.KVMap[v.Key]
		return nil
	}
	seq := kv.lastApply + 1
	for {
		kv.px.Start(seq, v)
		decidedValue := kv.Wait(seq)
		if v == decidedValue {
			break
		}
		seq++
	}
	for ; kv.lastApply+1 < seq; kv.lastApply++ {
		decidedValue := kv.Wait(kv.lastApply + 1)
		kv.Apply(decidedValue.(Op))
	}

	reply.Value = kv.KVMap[args.Key]
	reply.Err = OK
	kv.px.Done(kv.lastApply)
	//log.Printf("Reply %v", reply)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("PutAppend key %s, value %s, op %s", args.Key, args.Value, args.Op)

	v := Op{Key: args.Key, Operation: args.Op, Value: args.Value, ClientID: args.ClientID, Seq: args.Seq}
	clientID := args.ClientID
	if maxSeq, ok := kv.maxClientSeq[clientID]; ok && args.Seq <= maxSeq {
		// if isApply, ok := kv.isLogApply[v]; ok && isApply {
		reply.Err = OK
		return nil
	}
	seq := kv.lastApply + 1
	for {
		kv.px.Start(seq, v)
		decidedValue := kv.Wait(seq)
		DPrintf("PutAppend decidedValue %v", decidedValue)
		if v == decidedValue {
			break
		}
		seq++
	}
	reply.Err = OK
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.KVMap = make(map[string]string)
	kv.maxClientSeq = make(map[int64]int)
	// kv.isLogApply = make(map[Op]bool)
	kv.lastApply = -1

	// Your initialization code here.

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
