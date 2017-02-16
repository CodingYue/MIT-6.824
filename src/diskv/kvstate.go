package diskv

import (
	"bytes"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"shardmaster"
	"time"
)

type DisKVState struct {
	gid int64
	me  int
	dir string

	LastApply int
	Config    shardmaster.Config

	Database     map[int]map[string]string
	MaxClientSeq map[int]map[string]int
	Received     map[int]bool
}

func (state *DisKVState) encodeState() string {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(*state)
	if err != nil {
		panic(err)
	}
	return string(buffer.Bytes())
}

func (state *DisKVState) decodeState(buf string) {
	buffer := bytes.NewBuffer([]byte(buf))
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(state)
	if err != nil {
		panic(err)
	}
}

func (state *DisKVState) restoreState() error {
	fullname := state.dir + "/checkpoint"
	content, err := ioutil.ReadFile(fullname)
	if err != nil {
		log.Printf("Checkpoint file not found %v", fullname)
		return err
	}
	state.decodeState(string(content))
	return nil
}

func (state *DisKVState) saveState() {
	fullname := state.dir + "/checkpoint"
	tempname := state.dir + "/temp-checkpoint"
	buffer := state.encodeState()
	for {
		if err := ioutil.WriteFile(tempname, []byte(buffer), 0666); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if err := os.Rename(tempname, fullname); err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		break
	}
}

func MakeDisKVState(gid int64, me int, dir string, restart bool) *DisKVState {
	gob.Register(DisKVState{})
	state := &DisKVState{}
	state.dir = dir
	state.gid = gid
	state.me = me

	if restart {
		err := state.restoreState()
		if err == nil {
			return state
		}
	}

	state.LastApply = 0
	state.Config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}

	state.Database = map[int]map[string]string{}
	state.MaxClientSeq = map[int]map[string]int{}
	state.Received = map[int]bool{}
	for shard := 0; shard < shardmaster.NShards; shard++ {
		state.Database[shard] = map[string]string{}
		state.MaxClientSeq[shard] = map[string]int{}
	}

	return state
}

func (state *DisKVState) getLastApply() int {
	return state.LastApply
}

func (state *DisKVState) setLastApply(LastApply int) {
	state.LastApply = LastApply
}

func (state *DisKVState) setConfig(Config shardmaster.Config) {
	state.Config = Config
}

func (state *DisKVState) getConfig() shardmaster.Config {
	return state.Config
}

func (state *DisKVState) putKeyValue(shard int, key string, value string) {
	state.Database[shard][key] = value
}

func (state *DisKVState) appendKeyValue(shard int, key string, value string) {
	state.Database[shard][key] = state.Database[shard][key] + value
}

func (state *DisKVState) getValueByKey(shard int, key string) (string, bool) {
	value, ok := state.Database[shard][key]
	return value, ok
}

func (state *DisKVState) getSeqByID(shard int, ID string) int {
	return state.MaxClientSeq[shard][ID]
}

func (state *DisKVState) updateSeq(shard int, ID string, seq int) {
	oldSeq := state.MaxClientSeq[shard][ID]
	if oldSeq < seq {
		state.MaxClientSeq[shard][ID] = seq
	}
}

func (state *DisKVState) setMaxClientSeq(shard int, MaxClientSeq map[string]int) {
	state.MaxClientSeq[shard] = MaxClientSeq
}

func (state *DisKVState) getMaxClientSeq(shard int) map[string]int {
	return state.MaxClientSeq[shard]
}

func (state *DisKVState) resetReceived() {
	state.Received = map[int]bool{}
}

func (state *DisKVState) setReceived(shard int, Received bool) {
	state.Received[shard] = Received
}

func (state *DisKVState) getReceived(shard int) bool {
	return state.Received[shard]
}

func (state *DisKVState) setDatabase(shard int, Database map[string]string) {
	state.Database[shard] = Database
}

func (state *DisKVState) getDatabase(shard int) map[string]string {
	return state.Database[shard]
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

		log.Printf("Update Recieved, Config num %v, shard %d, gid %d, me %d",
			state.Config.Num, args.Shard, state.gid, state.me)
		state.setDatabase(args.Shard, args.Database)
		state.setMaxClientSeq(args.Shard, args.MaxClientSeq)
		state.setReceived(args.Shard, true)
		state.updateSeq(args.Shard, args.ID, args.Seq)
	case "Tick":
		break
	default:
		panic("Wrong operation type")
	}
	state.LastApply++
}
