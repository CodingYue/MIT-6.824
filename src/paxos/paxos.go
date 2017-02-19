package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"persistence"
	"strconv"
	"strings"
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

const TIMES_PER_RPC = 10

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu                sync.Mutex
	l                 net.Listener
	dead              int32 // for testing
	unreliable        int32 // for testing
	rpcCount          int32 // for testing
	peers             []string
	me                int // index into peers[]
	dir               string
	enablePersistence bool

	instance  map[int]*InstanceStatus
	seqMax    int
	doneMax   []int
	deleteSeq int
	// Your data here.
}
type InstanceStatus struct {
	PrepareMax   int
	AcceptMax    int
	AcceptValue  interface{}
	DecidedValue interface{}
}

type PaxosStatus struct {
	SeqMax    int
	DoneMax   []int
	DeleteSeq int
}

type Result int

const (
	Ok Result = iota + 1
	Reject
)

type PrepareArgs struct {
	Seq        int
	ProposalID int
}

type PrepareReply struct {
	Reply      Result
	ProposalID int
	Value      interface{}
}

type AccpetArgs struct {
	Seq        int
	ProposalID int
	Value      interface{}
}

type AcceptReply struct {
	Reply Result
	Value interface{}
}

type DecidedArgs struct {
	Seq     int
	Value   interface{}
	From    int
	DoneMax int
}

type DecidedReply struct {
	Reply Result
}

func (px *Paxos) GetRecoveryStatus() (PaxosStatus, map[int]InstanceStatus) {
	px.mu.Lock()
	defer px.mu.Unlock()
	paxos := PaxosStatus{
		SeqMax:    px.seqMax,
		DoneMax:   px.doneMax,
		DeleteSeq: px.deleteSeq}
	instances := map[int]InstanceStatus{}
	for k, v := range px.instance {
		instances[k] = *v
	}
	return paxos, instances
}

func (px *Paxos) persistState(seqs []int) {
	if !px.enablePersistence {
		return
	}
	// DPrintf("persist state seqs %v", seqs)

	success := persistence.ReadTransactionSuccess(px.dir)
	if err := persistence.SyncTempfile(px.dir, success); err != nil {
		panic(err)
	}
	persistence.WriteFile(px.dir, "transaction_success", false)

	paxos := PaxosStatus{
		SeqMax:    px.seqMax,
		DoneMax:   px.doneMax,
		DeleteSeq: px.deleteSeq}

	// log.Printf("paxos persist me %v, status %v", px.me, paxos)
	for _, seq := range seqs {
		// DPrintf("persist instance %v seq, dir %v, value %v", seq, px.dir, *px.getInstance(seq))
		// log.Printf("paxos persist me %v, instance[%v]=%v", px.me, seq, *px.getInstance(seq))
		if err := persistence.WriteTempFile(px.dir, "instance-"+strconv.Itoa(seq), *px.getInstance(seq)); err != nil {
			panic(err)
		}
	}
	persistence.WriteTempFile(px.dir, "paxos_status", paxos)

	persistence.WriteFile(px.dir, "transaction_success", true)
}

func (px *Paxos) getInstance(seq int) *InstanceStatus {
	instance, ok := px.instance[seq]
	if !ok {
		instance = &InstanceStatus{AcceptMax: -1, PrepareMax: -1, AcceptValue: nil, DecidedValue: nil}
		px.instance[seq] = instance
	}
	return instance
}

func (px *Paxos) HandlePrepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	defer px.persistState([]int{args.Seq})

	instance := px.getInstance(args.Seq)
	if args.ProposalID > instance.PrepareMax {
		instance.PrepareMax = args.ProposalID
		reply.Value = instance.AcceptValue
		reply.ProposalID = instance.AcceptMax
		reply.Reply = Ok
	} else {
		reply.Reply = Reject
	}
	return nil
}

func (px *Paxos) HandleAccpet(args *AccpetArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	defer px.persistState([]int{args.Seq})

	instance := px.getInstance(args.Seq)
	if args.ProposalID >= instance.PrepareMax {
		instance.AcceptMax = args.ProposalID
		instance.PrepareMax = args.ProposalID
		instance.AcceptValue = args.Value

		reply.Reply = Ok
		reply.Value = instance.AcceptValue
	} else {
		reply.Reply = Reject
	}
	return nil
}

func (px *Paxos) HandleDecide(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	defer px.persistState([]int{args.Seq})
	px.getInstance(args.Seq).DecidedValue = args.Value
	px.doneMax[args.From] = args.DoneMax
	return nil
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//

func rpcCall(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		// log.Printf("unix error %v", errx)
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	for TIMES := 0; TIMES < TIMES_PER_RPC; TIMES++ {
		if ok := rpcCall(srv, rpcname, args, reply); ok {
			return true
		}
	}
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	currentMin := px.Min()
	px.mu.Lock()
	defer px.mu.Unlock()
	defer px.persistState([]int{seq})

	if seq > px.seqMax {
		px.seqMax = seq
	}
	if seq < currentMin {
		return
	}
	if decided := px.getInstance(seq).DecidedValue; decided != nil {
		return
	}
	go px.Propose(seq, v)
}

func (px *Paxos) PreparePhase(seq int, v interface{}, proposalID int) (bool, interface{}) {
	peerLen := len(px.peers)
	prepareReply := make(chan PrepareReply)
	done := make(chan bool)

	count := 0
	maxProposalID := -1
	var value interface{}
	received := 0
	go func() {
		for {
			reply, more := <-prepareReply
			if !more {
				done <- true
				break
			}
			received++
			if reply.Reply == Ok {
				count++
				if reply.ProposalID > maxProposalID {
					maxProposalID = reply.ProposalID
					value = reply.Value
				}
			}
		}
		if value == nil {
			value = v
		}
	}()

	for _, peer := range px.peers {
		args := &PrepareArgs{Seq: seq, ProposalID: proposalID}
		reply := PrepareReply{}
		if peer == px.peers[px.me] {
			px.HandlePrepare(args, &reply)
		} else {
			if ok := call(peer, "Paxos.HandlePrepare", args, &reply); !ok {
				continue
			}
		}
		prepareReply <- reply
	}
	close(prepareReply)
	<-done
	DPrintf("accept %d of received %d, total %d", count, received, len(px.peers))
	return count >= peerLen/2+1, value
}

func (px *Paxos) AccpetPhase(seq int, proposalID int, value interface{}) bool {
	peerLen := len(px.peers)

	acceptReply := make(chan AcceptReply)
	done := make(chan bool)
	count := 0
	go func() {
		for {
			reply, more := <-acceptReply
			if !more {
				done <- true
				break
			}
			if reply.Reply == Ok {
				count++
			}
		}
	}()
	for _, peer := range px.peers {
		args := &AccpetArgs{Seq: seq, ProposalID: proposalID, Value: value}
		reply := AcceptReply{}
		if peer == px.peers[px.me] {
			px.HandleAccpet(args, &reply)
		} else {
			if ok := call(peer, "Paxos.HandleAccpet", args, &reply); !ok {
				continue
			}
		}
		acceptReply <- reply
	}
	close(acceptReply)
	<-done
	return count >= peerLen/2+1
}

func (px *Paxos) DecidePhase(seq int, value interface{}) {
	for _, peer := range px.peers {
		go func(peer string) {
			args := &DecidedArgs{Seq: seq, Value: value, From: px.me, DoneMax: px.doneMax[px.me]}
			reply := DecidedReply{}
			if peer == px.peers[px.me] {
				px.HandleDecide(args, &reply)
			} else {
				for {
					if ok := call(peer, "Paxos.HandleDecide", args, &reply); !ok {
						time.Sleep(50 * time.Millisecond)
						continue
					}
					break
				}
			}
		}(peer)
	}
}

func (px *Paxos) Propose(seq int, v interface{}) {

	sleepTime := 10 * time.Millisecond
	for {
		time.Sleep(sleepTime)
		if sleepTime < 10*time.Second {
			sleepTime *= 2
		}
		if px.isdead() {
			return
		}
		peerLen := len(px.peers)
		px.mu.Lock()
		maxSeen := px.getInstance(seq).PrepareMax
		px.mu.Unlock()
		proposalID := (maxSeen+peerLen)/peerLen*peerLen + px.me
		DPrintf("Prepare seq %v, v %v, proposal id %v, me %v", seq, v, proposalID, px.peers[px.me])
		prepareOK, value := px.PreparePhase(seq, v, proposalID)
		if !prepareOK {
			continue
		}
		DPrintf("Accept seq %v, v %v, proposal id %v, me %v", seq, v, proposalID, px.peers[px.me])
		acceptOK := px.AccpetPhase(seq, proposalID, value)
		if !acceptOK {
			continue
		}
		DPrintf("Decide seq %v, v %v, proposal id %v, me %v", seq, v, proposalID, px.peers[px.me])
		px.DecidePhase(seq, value)
		// log.Printf("Check infinite loop, restore replica")
		break
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	defer px.persistState([]int{})
	// log.Printf("me %v, doneMax %v", px.me, px.doneMax)
	if seq > px.doneMax[px.me] {
		px.doneMax[px.me] = seq
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.seqMax
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	seqs := []int{}
	min := px.doneMax[px.me]
	for _, v := range px.doneMax {
		if v < min {
			min = v
		}
	}
	for ; px.deleteSeq <= min; px.deleteSeq++ {
		delete(px.instance, px.deleteSeq)
		seqs = append(seqs, px.deleteSeq)
	}
	px.persistState(seqs)
	return min + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	currentMin := px.Min()
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < currentMin {
		return Forgotten, nil
	}
	if decidedValue := px.getInstance(seq).DecidedValue; decidedValue != nil {
		return Decided, decidedValue
	}

	return Pending, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

func (px *Paxos) RestoreReplica(paxos PaxosStatus, instances map[int]InstanceStatus) {
	px.seqMax = paxos.SeqMax
	px.deleteSeq = paxos.DeleteSeq
	px.doneMax = paxos.DoneMax
	seqs := []int{}
	for k, v := range instances {
		status := v
		px.instance[k] = &status
		seqs = append(seqs, k)
	}
	log.Printf("Restore replica donemax : %v, instances %v", px.doneMax, instances)
	px.persistState(seqs)
}

func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	return MakeWithOptions(peers, me, rpcs, "", false, false)
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func MakeWithOptions(peers []string, me int, rpcs *rpc.Server,
	dir string, restart bool, hasInited bool) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	if dir == "" {
		px.enablePersistence = false
	} else {
		px.dir = dir + "/paxos-" + strconv.Itoa(px.me)
		px.enablePersistence = true
	}

	px.instance = make(map[int]*InstanceStatus)
	npaxos := len(peers)
	px.doneMax = make([]int, npaxos)
	px.deleteSeq = 0
	for i := 0; i < npaxos; i++ {
		px.doneMax[i] = -1
	}
	px.seqMax = -1
	if !restart {
		if dir != "" {
			os.RemoveAll(px.dir)
		}
	} else {
		if hasInited {
			success := persistence.ReadTransactionSuccess(px.dir)

			if err := persistence.SyncTempfile(px.dir, success); err != nil {
				panic(err)
			}
			var paxos PaxosStatus
			if err := persistence.ReadFile(px.dir, "paxos_status", &paxos); err != nil {
				panic(err)
			}
			// log.Printf("paxos restarts, me %v, status %v", px.me, paxos)
			px.doneMax = paxos.DoneMax
			px.deleteSeq = paxos.DeleteSeq
			px.seqMax = paxos.SeqMax
			files, _ := ioutil.ReadDir(px.dir)
			for _, file := range files {
				if file.IsDir() {
					continue
				}
				if strings.HasPrefix(file.Name(), "instance-") {
					var instance InstanceStatus
					if err := persistence.ReadFile(px.dir, file.Name(), &instance); err != nil {
						panic(err)
					}
					seq, err := strconv.Atoi(file.Name()[len("instance-"):])
					if err != nil {
						panic(err)
					}
					px.instance[seq] = &instance
					// log.Printf("paxos restarts instance, me %v, status[%v]=%v", px.me, seq, instance)
				}
			}
		}

	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
