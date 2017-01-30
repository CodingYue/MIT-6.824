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
Primary will return SUCCESS FLAG if and only backup return SUCCESS FLAG.
