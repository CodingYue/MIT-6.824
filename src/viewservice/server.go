package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu             sync.Mutex
	l              net.Listener
	dead           int32 // for testing
	rpccount       int32 // for testing
	me             string
	view           View
	lastPingTime   map[string]time.Time
	hasAcknowledge bool
	idleServer     string
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) ChangeView(primary string, backup string) {
	vs.view = View{vs.view.Viewnum + 1, primary, backup}
	vs.hasAcknowledge = false
}

func debugView(view View) {
	log.Printf("viewNum %d, Primary %s, Backup %s", view.Viewnum, view.Primary, view.Backup)
}

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()

	client := args.Me
	viewNum := args.Viewnum

	vs.lastPingTime[client] = time.Now()

	switch client {
	case vs.view.Primary:
		if viewNum == 0 {
			vs.ChangeView(vs.view.Backup, "")
		} else if vs.view.Viewnum == viewNum {
			vs.hasAcknowledge = true
		}
	case vs.view.Backup:
		if viewNum == 0 && vs.hasAcknowledge {
			vs.ChangeView(vs.view.Primary, vs.idleServer)
		}
	default:
		//log.Printf("client %s, viewNum %d\n", client, vs.view.Viewnum)
		if vs.view.Viewnum == 0 {
			vs.ChangeView(client, "")
		} else {
			vs.idleServer = client
		}
		//log.Printf("client %s, viewNum %d\n", client, vs.view.Viewnum)
	}
	reply.View = vs.view

	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}

func (vs *ViewServer) isOverTime(t time.Time, expectedTime time.Time) bool {
	return t.Sub(expectedTime) >= DeadPings*PingInterval
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//

func (vs *ViewServer) tick() {
	vs.mu.Lock()
	if vs.isOverTime(time.Now(), vs.lastPingTime[vs.idleServer]) {
		vs.idleServer = ""
	}
	if vs.isOverTime(time.Now(), vs.lastPingTime[vs.view.Backup]) {
		if vs.hasAcknowledge {
			vs.ChangeView(vs.view.Primary, vs.idleServer)
			vs.idleServer = ""
		}
	}
	if vs.isOverTime(time.Now(), vs.lastPingTime[vs.view.Primary]) {
		if vs.hasAcknowledge {
			vs.ChangeView(vs.view.Backup, vs.idleServer)
			vs.idleServer = ""
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.view = View{Primary: "", Backup: "", Viewnum: 0}
	vs.lastPingTime = make(map[string]time.Time)
	vs.hasAcknowledge = false

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
