package diskv

import (
	"bytes"
	"encoding/base32"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"shardmaster"
	"strconv"
	"strings"
)

type DisKVState struct {
	gid int64
	me  int
	dir string

	// lastApply int
	// config    shardmaster.Config

	// database     map[int]map[string]string
	// maxClientSeq map[int]map[string]int
	// received     map[int]bool
}

func (kv *DisKV) MakeDisKVState(servers []string, restart bool) {
	state := &DisKVState{}
	kv.state = state
	state.dir = kv.dir
	state.gid = kv.gid
	state.me = kv.me
	gob.Register(shardmaster.Config{})

	if !restart {
		os.RemoveAll(state.dir)
		state.setConfig(shardmaster.Config{Num: 0, Groups: map[int64][]string{}})
		state.setLastApply(0)
		state.resetReceived()
	} else {
		log.Printf("Server restarts gid %v, me %v, lastApply %v", kv.gid, kv.me, state.getLastApply())
		if state.getLastApply() == -1 {
			log.Printf("Disk loss")
			if err := os.MkdirAll(state.dir, 0777); err != nil {
				log.Fatalf("Mkdir(%v): %v", state.dir, err)
			}
			kv.diskLossRecovery(servers)
		}
	}
}

func (state *DisKVState) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (state *DisKVState) decodeKey(filename string) string {
	key, err := base32.StdEncoding.DecodeString(filename)
	if err != nil {
		panic(err)
	}
	return string(key)
}

func (state DisKVState) filePut(shard int, key string, value string) {
	fullname := state.shardDir(shard) + "/" + state.encodeKey(key)
	tempname := state.shardDir(shard) + "/temp-" + state.encodeKey(key)
	for {
		if err := ioutil.WriteFile(tempname, []byte(value), 0666); err != nil {
			continue
		}
		if err := os.Rename(tempname, fullname); err != nil {
			continue
		}
		break
	}
}

func (state DisKVState) fileGet(shard int, key string) (string, error) {
	fullname := state.shardDir(shard) + "/" + state.encodeKey(key)
	value, err := ioutil.ReadFile(fullname)
	return string(value), err
}

func (state *DisKVState) shardDir(shard int) string {
	var d string
	if shard == -1 {
		d = state.dir + "/"
	} else if shard == -2 {
		d = state.dir + "/received/"
	} else {
		d = state.dir + "/shard-" + strconv.Itoa(shard) + "/"
	}
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

func encode(v interface{}) string {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(v); err != nil {
		panic(err)
	}
	return string(w.Bytes())
}

func decode(buf string, v interface{}) {
	r := bytes.NewBuffer([]byte(buf))
	d := gob.NewDecoder(r)
	if err := d.Decode(v); err != nil {
		panic(err)
	}
}

func (state *DisKVState) setLastApply(lastApply int) {
	// state.lastApply = lastApply
	state.filePut(-1, "LastApply", encode(lastApply))
}

func (state *DisKVState) getLastApply() int {
	// return state.lastApply
	content, err := state.fileGet(-1, "LastApply")
	if err != nil {
		return -1
	}
	var value int
	decode(content, &value)
	return value
}

func (state *DisKVState) setConfig(config shardmaster.Config) {
	// state.config = config
	// log.Printf("Encode config result : %v", encode(config))
	state.filePut(-1, "Config", encode(config))
}

func (state *DisKVState) getConfig() shardmaster.Config {
	// return state.config
	value, err := state.fileGet(-1, "Config")
	if err != nil {
		return shardmaster.Config{Num: 0, Groups: map[int64][]string{}}
	}
	config := shardmaster.Config{}
	decode(value, &config)
	return config
}

func (state *DisKVState) putKeyValue(shard int, key string, value string) {
	// state.database[shard][key] = value
	state.filePut(shard, "KeyValue-"+key, encode(value))
}

func (state *DisKVState) appendKeyValue(shard int, key string, value string) {
	// state.database[shard][key] = state.database[shard][key] + value
	content, err := state.fileGet(shard, "KeyValue-"+key)
	var currentValue string
	if err == nil {
		decode(content, &currentValue)
	} else {
		currentValue = ""
	}
	state.filePut(shard, "KeyValue-"+key, encode(currentValue+value))
}

func (state *DisKVState) getValueByKey(shard int, key string) (string, bool) {
	content, err := state.fileGet(shard, "KeyValue-"+key)
	if err != nil {
		return "", false
	}
	var value string
	decode(content, &value)
	return value, true
}

func (state *DisKVState) getSeqByID(shard int, ID string) int {
	value, err := state.fileGet(shard, "Client-"+ID)
	if err != nil {
		return 0
	}
	var seq int
	decode(value, &seq)
	return seq
}

func (state *DisKVState) updateSeq(shard int, ID string, seq int) {
	oldSeq := state.getSeqByID(shard, ID)
	if oldSeq < seq {
		state.filePut(shard, "Client-"+ID, encode(seq))
	}
}

func (state *DisKVState) fileReplaceShard(shard int, database map[string]string,
	maxClientSeq map[string]int) {
	d := state.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range database {
		state.putKeyValue(shard, k, v)
	}
	for k, v := range maxClientSeq {
		state.updateSeq(shard, k, v)
	}
}

func (state *DisKVState) resetReceived() {
	d := state.shardDir(-2)
	os.RemoveAll(d)
}

func (state *DisKVState) setReceived(shard int, received bool) {
	state.filePut(-2, "Received-"+strconv.Itoa(shard), encode(received))
}

func (state *DisKVState) getReceived(shard int) bool {
	value, err := state.fileGet(-2, "Received-"+strconv.Itoa(shard))
	if err != nil {
		return false
	}
	var received bool
	decode(value, &received)
	return received
}

func (state *DisKVState) fileReadShard(shard int) (
	database map[string]string, maxClientSeq map[string]int) {
	database = map[string]string{}
	maxClientSeq = map[string]int{}
	d := state.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		name := state.decodeKey(fi.Name())
		if strings.HasPrefix(name, "Client-") {
			key := name[len("Client-"):]
			value := state.getSeqByID(shard, key)
			maxClientSeq[key] = value
		} else if strings.HasPrefix(name, "KeyValue-") {
			key := name[len("KeyValue-"):]
			value, _ := state.getValueByKey(shard, key)
			database[key] = value
		}
	}
	return database, maxClientSeq
}

func (state *DisKVState) applyOperation(op Op, seq int) {

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

		// log.Printf("Update Recieved, config num %v, shard %d, gid %d, me %d",
		// 	state.config.Num, args.Shard, state.gid, state.me)
		state.fileReplaceShard(args.Shard, args.Database, args.MaxClientSeq)
		state.setReceived(args.Shard, true)
		state.updateSeq(args.Shard, args.ID, args.Seq)
	case "Tick":
		break
	default:
		panic("Wrong operation type")
	}
	state.setLastApply(state.getLastApply() + 1)
	log.Printf("Apply %v, gid %v, me %v, seq %v", op, state.gid, state.me, seq)
}
