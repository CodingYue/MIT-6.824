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

