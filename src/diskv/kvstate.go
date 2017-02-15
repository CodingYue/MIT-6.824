package diskv

import (
	"log"
	"shardmaster"
)

type DisKVState struct {
	gid int64
	me  int
	dir string

	lastApply int
	config    shardmaster.Config

	database     map[int]map[string]string
	maxClientSeq map[int]map[string]int
	received     map[int]bool
}

func MakeDisKVState(gid int64, me int, dir string, estart bool) *DisKVState {
	state := &DisKVState{}
	state.dir = dir
	state.gid = gid
	state.me = me

	state.lastApply = 0
	state.config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}

	state.database = map[int]map[string]string{}
	state.maxClientSeq = map[int]map[string]int{}
	state.received = map[int]bool{}
	for shard := 0; shard < shardmaster.NShards; shard++ {
		state.database[shard] = map[string]string{}
		state.maxClientSeq[shard] = map[string]int{}
	}
	return state
}

func (state *DisKVState) getLastApply() int {
	return state.lastApply
}

func (state *DisKVState) setLastApply(lastApply int) {
	state.lastApply = lastApply
}

func (state *DisKVState) setConfig(config shardmaster.Config) {
	state.config = config
}

func (state *DisKVState) getConfig() shardmaster.Config {
	return state.config
}

func (state *DisKVState) putKeyValue(shard int, key string, value string) {
	state.database[shard][key] = value
}

func (state *DisKVState) appendKeyValue(shard int, key string, value string) {
	state.database[shard][key] = state.database[shard][key] + value
}

func (state *DisKVState) getValueByKey(shard int, key string) (string, bool) {
	value, ok := state.database[shard][key]
	return value, ok
}

func (state *DisKVState) getSeqByID(shard int, ID string) int {
	return state.maxClientSeq[shard][ID]
}

func (state *DisKVState) updateSeq(shard int, ID string, seq int) {
	oldSeq := state.maxClientSeq[shard][ID]
	if oldSeq < seq {
		state.maxClientSeq[shard][ID] = seq
	}
}

func (state *DisKVState) setMaxClientSeq(shard int, maxClientSeq map[string]int) {
	state.maxClientSeq[shard] = maxClientSeq
}

func (state *DisKVState) getMaxClientSeq(shard int) map[string]int {
	return state.maxClientSeq[shard]
}

func (state *DisKVState) resetReceived() {
	state.received = map[int]bool{}
}

func (state *DisKVState) setReceived(shard int, received bool) {
	state.received[shard] = received
}

func (state *DisKVState) getReceived(shard int) bool {
	return state.received[shard]
}

func (state *DisKVState) setDatabase(shard int, database map[string]string) {
	state.database[shard] = database
}

func (state *DisKVState) getDatabase(shard int) map[string]string {
	return state.database[shard]
}

func (state *DisKVState) applyOperation(op Op, seq int) {
	log.Printf("Apply %v, gid %v, me %v", op, state.gid, state.me)

	switch op.Operation {
	case "Get":
		args := op.Value.(GetArgs)
		state.updateSeq(args.Shard, args.ID, args.Seq)

	case "Put":
		args := op.Value.(PutAppendArgs)
		state.putKeyValue(args.Shard, args.Key, args.Value)
		state.updateSeq(args.Shard, args.ID, args.Seq)

	case "Append":
		args := op.Value.(PutAppendArgs)
		state.appendKeyValue(args.Shard, args.Key, args.Value)
		state.updateSeq(args.Shard, args.ID, args.Seq)

	case "Update":
		args := op.Value.(UpdateArgs)

		log.Printf("Update Recieved, config num %v, shard %d, gid %d, me %d",
			state.config.Num, args.Shard, state.gid, state.me)
		state.setDatabase(args.Shard, args.Database)
		state.setMaxClientSeq(args.Shard, args.MaxClientSeq)
		state.setReceived(args.Shard, true)
		state.updateSeq(args.Shard, args.ID, args.Seq)
	case "Tick":
		break
	default:
		panic("Wrong operation type")
	}
	state.lastApply++
}
