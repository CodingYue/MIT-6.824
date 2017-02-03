# MIT-6.824

MIT distributed system

## Lab1 : MapReduce

### Part A

Map function maps string, and split the string. 

For each word in the string, generate KeyValue{word, "1"}.

Reduce function's input are key string and KeyValue List. This key corresponds to Map Function gernerated KEY.

Sum the KeyValue's Value and return answer.

### Part B & Part C

First of all, split data sets, and store them to nMap files.

Initialize master and workers. When initializing workers, each worker will register itself to master.

Use go channel to maintain available workers. 

Once master get registeration notice from workers, it will continously add it to available worker channel.

Worker possibly crash or cannot connects to master, so when finish jobs or fail to send RPC request, master must re-add it to available worker channel.

Map operations must be predeceds Reduce operations. 

## Lab2 : Primary/Backup Key/Value Service

A naive replication system - Primary/Backup

ViewServer maintains a view that shows which server is Primary & Backup.

When Primary finds that Backup exists, Primary must sync its data to Backup.

### Part A

Viewservice directory. 

client.go in this part describes Primary or Backup server.
Each client has its view sent by ViewServer. 

Client use heart beat method tell ViewServer that it it alive(PING ViewServer every PingInterval)

server.go in this part describes ViewServer.

It will call tick() evrey PingInterval. In tick() it will determines whether backcup and primary are dead.
It will also handle PING request. 


### Part B

Maintains K-V model.

Client sends request(Get, Append, Put) to server. Server try to send operations to Primary.
If fail, server will retrieve view from ViewServer.

And Primary will also try to send operations to backup if backup exists.
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
After call `px.Start()`, all paxos servers will have eventually the same value for specificated `seq` number.

`px.Status()` return whether `seq` is decided, pending or forgotten.

`px.Max()` return maximum nubmer `seq` recieved by `px.Start()`

In order to shrink logs, we must implement two interfaces `px.Min(), px.Done()`. 

`px.Done(seq int)` tells server that application doesn't need `seq` result anymore. (eg. kv applicationa has applied log with `seq` to database.)

`px.Min()` query the minimum number of all paxos servers' `seq` recieved by `px.Done()`

paxos servers can delete logs which's `seq` number is less than `px.Min()`.

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

How make sure proposal ID unique?

Each server has its number `me`. Each server's proposalID = n * (number of servers) + me

I maintain vector `doneMax`, indicating `Done()` value recieved by each server.

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

