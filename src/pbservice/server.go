package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	view       viewservice.View
	keyValue   map[string]string
	lastArgs   map[string]PutAppendArgs
}

func (pb *PBServer) UpdateView() {
	currentView, _ := pb.vs.Ping(pb.view.Viewnum)
	if currentView.Viewnum != pb.view.Viewnum {

		if currentView.Primary == pb.me && pb.view.Backup != currentView.Backup && currentView.Backup != "" {
			for k, v := range pb.keyValue {
				//log.Printf("key : %s, value : %s, backup %s\n", k, v, currentView.Backup)
				args := &PutAppendArgs{Key: k, Value: v, Operation: Put, From: pb.me}
				reply := PutAppendReply{}
				ok := call(currentView.Backup, "PBServer.PutAppend", args, &reply)
				for !ok || reply.Err != OK {
					ok = call(currentView.Backup, "PBServer.PutAppend", args, &reply)
				}
			}
		}

		pb.view = currentView
	}
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}
	if value, ok := pb.keyValue[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	reply.Err = OK
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if *args == pb.lastArgs[args.From] {
		reply.Err = OK
		return nil
	}
	if pb.view.Primary == pb.me && pb.view.Backup != "" {
		ok := call(pb.view.Backup, "PBServer.PutAppend", args, reply)
		if !ok {
			return errors.New("Backup server connection error")
		}
	}
	if value, ok := pb.keyValue[args.Key]; args.Operation == Append && ok {
		pb.keyValue[args.Key] = value + args.Value
	} else {
		pb.keyValue[args.Key] = args.Value
	}
	pb.lastArgs[args.From] = *args
	reply.Err = OK
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//

func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.UpdateView()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.keyValue = make(map[string]string)
	pb.lastArgs = make(map[string]PutAppendArgs)
	pb.view = viewservice.View{Primary: "", Backup: "", Viewnum: 0}

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
