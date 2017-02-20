# MIT-6.824

MIT distributed system sprint 2015

 * [Lab1 : MapReduce](#lab1--mapreduce)
	 * [Part A](#part-a)
	 * [Part B & Part C](#part-b--part-c)
 * [Lab2 : Primary/Backup Key/Value Service](#lab2--primarybackup-keyvalue-service)
	 * [Part A](#part-a-1)
	 * [Part B](#part-b)
 * [Lab 3 : Paxos](#lab-3--paxos)
	 * [Part A](#part-a-2)
	 * [Part B: Paxos-based Key/Value Server](#part-b-paxos-based-keyvalue-server)
 * [Lab4: Sharded Key/Value Service](#lab4-sharded-keyvalue-service)
	 * [Part A: The Shard Master](#part-a-the-shard-master)
	 * [Part B: Sharded Key/Value Server](#part-b-sharded-keyvalue-server)
 * [Lab5: Persistence](#lab5--persistence)
    * [Persistence](#persistence)
    * [Transaction File Operations](#transaction-file-operations)
    * [Replica Failover](#replica-failover)

## Lab1 : MapReduce

### Part A

Map function maps string, and split the string. <br>
For each word in the string, generate KeyValue{word, "1"}.<br>
Reduce function's input are key string and KeyValue List. This key corresponds to Map Function gernerated KEY.<br>
Sum the KeyValue's Value and return answer.<br>

### Part B & Part C

First of all, split data sets, and store them to nMap files.<br>
Initialize master and workers. When initializing workers, each worker will register itself to master.<br>
Use go channel to maintain available workers. <br>
Once master get registeration notice from workers, it will continously add it to available worker channel.<br>
Worker possibly crash or cannot connects to master, so when finish jobs or fail to send RPC request, master must re-add it to available worker channel.<br>
Map operations must be predeceds Reduce operations. 

## Lab2 : Primary/Backup Key/Value Service

A naive replication system - Primary/Backup<br>
ViewServer maintains a view that shows which server is Primary & Backup.<br>
When Primary finds that Backup exists, Primary must sync its data to Backup.

### Part A

Viewservice directory. 

client.go in this part describes Primary or Backup server.<br>
Each client has its view sent by ViewServer. 

Client use heart beat method tell ViewServer that it it alive(PING ViewServer every PingInterval)<br>
server.go in this part describes ViewServer.

It will call tick() evrey PingInterval. In tick() it will determines whether backcup and primary are dead.
It will also handle PING request. 


### Part B

Maintains K-V model.

Client sends request(Get, Append, Put) to server. Server try to send operations to Primary.
If fail, server will retrieve view from ViewServer.

And Primary will also try to send operations to backup if backup exists.<br>
Primary will return SUCCESS FLAG if and only if backup return SUCCESS FLAG.


## Lab 3 : Paxos

In lab2, we designed view service application, which leads our K, V having limited fault tolerance.
Data will be lost if view server crashes or disconnectes. Also, primary and backup server crash at the same time will lead to 
data loss.

Paxos algorithm was designed to solve consensus problem which means making all replicas have the same content without center(viewserver)
while system sufferring from [partition](https://en.wikipedia.org/wiki/Network_partition), crashing.

Paxos algorithm key idea is that if a proposal's value has been accepted by majotiry(more then half) of all servers,
then that value is final value. To archive that, Paxos's divides the whole process into two phase : Prepare Phase, Accept Phase.

In prepare phase, proposer will ask highest proposal ID which they have seen, and get the value they have accepted.
The protocol is that once accpetor see a proposal ID, then it cannot accpet the value proposed by proposer that has a higher than proposal ID.
After proposer get responses with (highest proposal ID, accpeted value) of majotiry, proposer will choose value of the pair which has the highest proposal ID.
If value is null, then proposer use original value otherwise use the value. Finally, proposer send accpet request with value to all servers.

### Part A
#### Overview
We implement paxos protocol in this part.

Interfaces:

    px = paxos.Make(peers []string, me int)
    px.Start(seq int, v interface{}) // start agreement on new instance
    px.Status(seq int) (fate Fate, v interface{}) // get info about an instance
    px.Done(seq int) // ok to forget all instances <= seq
    px.Max() int // highest instance seq known, or -1
    px.Min() int // instances before this have been forgotten

`px.Start(seq int, v interface{})` means for specificated `seq` number given by client, paxos will propose a value `v`.
After call `px.Start()`, all paxos servers will have eventually the same value for specificated `seq` number.<br>
`px.Status()` return whether `seq` is decided, pending or forgotten.<br>
`px.Max()` return maximum nubmer `seq` recieved by `px.Start()`<br>
In order to shrink logs, we must implement two interfaces `px.Min(), px.Done()`. <br>
`px.Done(seq int)` tells server that application doesn't need `seq` result anymore. (eg. kv applicationa has applied log with `seq` to database.)<br>
`px.Min()` query the minimum number of all paxos servers' `seq` recieved by `px.Done()`<br>
paxos servers can delete logs which's `seq` number is less than `px.Min()`.<br>

#### Details

pseudo-code

    proposer(v):
    while not decided:
        choose n, unique and higher than any n seen so far
        send prepare(n) to all servers including self
        if prepare_ok(n, n_a, v_a) from majority:
        v' = v_a with highest n_a; choose own v otherwise
        send accept(n, v') to all
        if accept_ok(n) from majority:
            send decided(v') to all

    acceptor's state:
    n_p (highest prepare seen)
    n_a, v_a (highest accept seen)

    acceptor's prepare(n) handler:
    if n > n_p
        n_p = n
        reply prepare_ok(n, n_a, v_a)
    else
        reply prepare_reject

    acceptor's accept(n, v) handler:
    if n >= n_p
        n_p = n
        n_a = n
        v_a = v
        reply accept_ok(n)
    else
        reply accept_reject

How make sure proposal ID unique?<br>
Each server has its number `me`. Each server's proposalID = n * (number of servers) + me<br>
I maintain vector `doneMax`, indicating `Done()` value recieved by each server.<br>
I piggyback `Done()` value when send decided message to other servers.

### Part B: Paxos-based Key/Value Server

The key is how to choose `seq`? Simply, loop `seq` from 1 to infinite, propose it and wait for `seq` decided.
Once decided, check if decided value equals to the value given by application. If they are equal, the seq is final value,
otherwise continue increasing `seq`.

Multiple the same operations sent by client, or due to server unreliable, how to find out two operations identical (not append twice)?

Each client maintains a unique client ID and client Seq. 
Before client sends a request to servers, it increase its client Seq.

If we call `Done()` try to forget some operations, how can we choose correct `seq`? (Under the circumstance, some `seqs` are forgotten,
 which means we cannot compare two values.) Solution is that, each server maintains a `maxClientSeq map[int64]int`.
When a request's `maxClientSeq[ClientID] <= ClientSeq`, the request has been processed, should be passed out.

## Lab4: Sharded Key/Value Service

More practical example than lab3 - Introduction of shard.

### Part A: The Shard Master

Shard Master maintains that each shard is assigned to who groups.<br>
Shard Master should be fault tolerance.<br>

Multiple replicas method achieves fault tolerance while paxos protocol holds consistency.<br>
Interfaces `Join(), Leave(), Move(), Query()`.<br>

The algorithm rebalance is that, first assign those shard which assigned group leaves to zero(invalid group).<br>
For each shard if it it assigned to invalid or assigning group was assigend two many times(more than number of shard  / number of group),<br>
then it should be reassigned to the group which has the minimum assigning times (one constraint is that difference should be more than 1).<br>

### Part B: Sharded Key/Value Server

Very tough. Many unexpected situations occur in last test case.

#### State Machine Design

    type ShardState struct {
        maxClientSeq map[int64]int
        database     map[string]string
    }

    // Server state machine
	lastApply  int
	shardState map[int]*ShardState
	isRecieved map[int]bool

I use map structure - shardState, to maintain each shard state, in other words, I group database and maxClientSeq by
shard number. <br>
This was designed for the specificated situation :

Client A send `{APPEND x, y, ID:A, Seq:1}` to server, operation applied but client doesn't get response(unreliable). <br>
So Client A will continously send request. But meanwhile, server's tick() function holds the mutex lock.<br>
Server SA update its config, and move all (k, v) pairs to new assigning group SB without `maxClientSeq` infomations.<br>
Server SA handles client A request, and response to client A with ErrWrongGroup.<br>
Client A send request to SB, because SB hasn't `maxClientSeq` infomations, append was called twice.

#### Update Config Info

Server calls tick() every 250 ms, and attempting to update its config.<br>
Server will ask shardmaster server: what is next config? (`Query(config.Num + 1)`)<br>
If two configs are not identical, server try to update its config<br>
Under the circumstances, Server X has three types: new shard assign to group which X belongs to, shard not longer assigned to that group and otherwise.<br>
Shardmaster modification on config makes sure that a group can only be one of three types.<br>

##### new shard assign to group which X belongs to

Server X cannot update its config until it recieved all new shards' infomation from other group.<br>
So `isRecieved map[int]bool` records whether shard has been recieved.<br>
If not recieved, then exit tick().<br>
Just because `isRecieved` will only be updated in `Apply()` function, before whole process, server X must get newest State Machine (propose a new `Get() Opeartion`)

##### shard not longer assigned to that group

Server sends its shard infomation to other groups. Also, need to update its state machine, propose a new `Get()`.

#### Other Details.

If duplicated request was sending to server, server must response SUCCESS message to client.<br>
Because, an operation was successfully applied but server didn't response with SUCCESS message (unreliable). Under this circumstance, 
server meets the same operation should response SUCCESS message otherwise client cannot move on.

## Lab5 : Persistence

### Persistence

#### File Operations

```
func WriteFile(dir string, name string, content interface{}) error 
func ReadFile(dir string, name string, content interface{}) error 
```

WriteFile's content is data needed to be persisted. <br>
ReadFile's content is data address needed to be restored from file.<br>
I use `gob` to encode and decode content.<br>

#### Paxos 

Refine paxos with two status : InstanceStatus, PaxosStatus.<br>
```
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
```
Each seq was mapped to an InstanceStatus.
And every paxos server contains map[int]InstanceStatus, PaxosStatus

Paxos needs save its state before it responses to statemachine.

#### DisKV

Refin KV with two status : disKVstatus, ShardState.<br>
```
type ShardState struct {
	MaxClientSeq map[int64]int
	Database     map[string]string
}
type disKVstatus struct {
	Config    shardmaster.Config
	LastApply int
	Received  map[int]bool
}
```

Similarly, each shard was mapped into a ShardState.

Due to performance reason, I didn't save DisKV state frequently. 
Just because if DisKV didn't tell paxos to forget some instances, the instance will be 
persisted in paxos servers. Saving state is more likely a checkpoint of replica's status, 
after save the checkpoint, we can delete the instances which was applied to state machine from paxos servers.

### Transaction File Operations

Saving status possiblely results in mutilple files edit. To ensure state machines correctness, 
transaction files write should be taken into consideration.

I created `transaction_success` file, indicates whether previous transation was successfully commited.<br>
Writing/reading `transactoin_success` are considered atomicity.
Failure during processes show the result inconsistency of file, which also means transaction oepration failed.<br>

File write during transaction process, will use API: <br>
`func WriteTempFile(dir string, name string, content interface{}) error `<br>
which is similar to `WriteFile` but add prefix `"temp-"` to `name`.

API `func SyncTempfile(dir string, success bool) error `<br>
if `success` is `true`:<br>
rename all files which have name with `"temp-"` prefix to filename without `"temp-"`.
delete all files which have name with `"temp"`


Transaction write example:
```
    read transaction_success.
    SyncTempfile(dir, transaction_success)
    write transaction_success false
    WriteTempFile ...
    WriteTempFile ...
    ...
    write transaction_success true
```

Read example:
```
    read transaction_success.
    SyncTempfile(dir, transaction_success)
    wrte transaction_success false
    ReadFile ...
    ReadFile ...
    ...
```

### Replica Failover

#### Local recovery (disk not loss)
Trivial.

#### Remote recovery (disk loss)
Ask for all other servers about their status, and restore it. <br>
In addition, replica must wait for majority of replicas' response and restore replica.
Because, if replica restores some insufficiently up-to-date paxos status, increase the nubmer 
of insufficiently up-to-date servers, reach to a majority, which will result instances' loss.

After recoevry, must writle `has_inited` file to prevent failing again during recovery process. 
(eg. zhalf of data persisted.)