package paxos

import (
	"bytes"
	"encoding/base32"
	"encoding/gob"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

type StateMachine struct {
	dir         string
	npaxos      int
	me          int
	PrepareMax  map[int]int
	AcceptMax   map[int]int
	AcceptValue map[int]PaxosLog
	// DecidedValue map[int]PaxosLog
	// SeqMax       int
	// DoneMax      []int
	// DeleteSeq    int
}

type PaxosLogWrapper struct {
	Value PaxosLog
}

func (paxos *StateMachine) deleteFile(seq int) {
	// os.RemoveAll(paxos.getDir("PrepareMax"))
	// os.RemoveAll(paxos.getDir("AcceptValue"))
	os.RemoveAll(paxos.getDir("DecidedValue"))
	// os.RemoveAll(paxos.getDir("AccetpMax"))
	delete(paxos.PrepareMax, seq)
	delete(paxos.AcceptMax, seq)
	delete(paxos.AcceptValue, seq)
	// delete(paxos.DecidedValue, seq)
}

func (paxos *StateMachine) getDir(name string) string {
	dir := paxos.dir + "/" + name
	if _, err := os.Stat(dir); err != nil {
		if err := os.Mkdir(dir, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", dir, err)
		}
	}
	return dir
}

func (paxos *StateMachine) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (paxos *StateMachine) decodeKey(filename string) string {
	key, err := base32.StdEncoding.DecodeString(filename)
	if err != nil {
		panic(err)
	}
	return string(key)
}

func (paxos *StateMachine) filePut(name string, key string, value string) {
	fullname := paxos.getDir(name) + "/" + paxos.encodeKey(key)
	tempname := paxos.getDir(name) + "/temp-" + paxos.encodeKey(key)
	for {
		if err := ioutil.WriteFile(tempname, []byte(value), 0666); err != nil {
			log.Printf("Write file error %v", err)
			continue
		}
		if err := os.Rename(tempname, fullname); err != nil {
			log.Printf("Renmae error %v", err)
			continue
		}
		break
	}
}

func (paxos *StateMachine) fileGet(name string, key string) (string, error) {
	fullname := paxos.getDir(name) + "/" + paxos.encodeKey(key)
	value, err := ioutil.ReadFile(fullname)
	return string(value), err
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

func (paxos *StateMachine) setDeleteSeq(seq int) {
	paxos.filePut("", "DeleteSeq", encode(seq))
}

func (paxos *StateMachine) getDeleteSeq() int {
	content, err := paxos.fileGet("", "DeleteSeq")
	if err != nil {
		return 0
	}
	var deleteSeq int
	decode(content, &deleteSeq)
	return deleteSeq
}

func (paxos *StateMachine) setDoneMax(from int, done int) {
	paxos.filePut("DoneMax", strconv.Itoa(from), encode(done))
}

func (paxos *StateMachine) getDoneMax(who int) int {
	content, err := paxos.fileGet("DoneMax", strconv.Itoa(who))
	if err != nil {
		return -1
	}
	var doneMax int
	decode(content, &doneMax)
	return doneMax
}

func (paxos *StateMachine) setSeqMax(seq int) {
	paxos.filePut("", "SeqMax", encode(seq))
}

func (paxos *StateMachine) getSeqMax() int {
	content, err := paxos.fileGet("", "SeqMax")
	if err != nil {
		return -1
	}
	var seqMax int
	decode(content, &seqMax)
	return seqMax
}

func (paxos *StateMachine) setDecidedValue(seq int, v PaxosLog) {
	paxos.filePut("DecidedValue", strconv.Itoa(seq), encode(PaxosLogWrapper{v}))
	// paxos.DecidedValue[seq] = v
}

func (paxos *StateMachine) getDecidedValue(seq int) (PaxosLog, bool) {
	content, err := paxos.fileGet("DecidedValue", strconv.Itoa(seq))
	if err != nil {
		return nil, false
	}
	decidedValue := PaxosLogWrapper{}
	decode(content, &decidedValue)
	return decidedValue.Value, true
	// value, ok := paxos.DecidedValue[seq]
	// if !ok {
	// 	value = nil
	// }
	// return value, ok
}

func (paxos *StateMachine) setAcceptorValue(seq int, v PaxosLog) {
	// paxos.filePut("AcceptValue", strconv.Itoa(seq), encode(PaxosLogWrapper{v}))
	paxos.AcceptValue[seq] = v
}

func (paxos *StateMachine) getAcceptorValue(seq int) PaxosLog {
	// content, err := paxos.fileGet("AcceptValue", strconv.Itoa(seq))
	// if err != nil {
	// 	return nil
	// }
	// acceptValue := PaxosLogWrapper{}
	// decode(content, &acceptValue)
	// return acceptValue.Value
	acceptValue, ok := paxos.AcceptValue[seq]
	if !ok {
		acceptValue = nil
	}
	return acceptValue
}

func (paxos *StateMachine) setPrepareMax(seq int, proposalID int) {
	// paxos.filePut("PrepareMax", strconv.Itoa(seq), encode(proposalID))
	paxos.PrepareMax[seq] = proposalID
}

func (paxos *StateMachine) getPrepareMax(seq int) int {
	// content, err := paxos.fileGet("PrepareMax", strconv.Itoa(seq))
	// if err != nil {
	// 	return -1
	// }
	// var prepareMax int
	// decode(content, &prepareMax)
	// return prepareMax
	value, ok := paxos.PrepareMax[seq]
	if !ok {
		value = -1
	}
	return value
}

func (paxos *StateMachine) setAcceptMax(seq int, proposalID int) {
	// paxos.filePut("AcceptMax", strconv.Itoa(seq), encode(proposalID))
	paxos.AcceptMax[seq] = proposalID
}

func (paxos *StateMachine) getAcceptMax(seq int) int {
	// content, err := paxos.fileGet("AcceptMax", strconv.Itoa(seq))
	// if err != nil {
	// 	return -1
	// }
	// var acceptMax int
	// decode(content, &acceptMax)
	// return seq
	value, ok := paxos.AcceptMax[seq]
	if !ok {
		value = -1
	}
	return value
}

func (paxos *StateMachine) getMinimumDoneMax() int {
	min := paxos.getDoneMax(0)
	for idx := 1; idx < paxos.npaxos; idx++ {
		v := paxos.getDoneMax(idx)
		if v < min {
			min = v
		}
	}
	deleteSeq := paxos.getDeleteSeq()
	for ; deleteSeq <= min; deleteSeq++ {
		paxos.deleteFile(deleteSeq)
	}
	paxos.setDeleteSeq(deleteSeq)
	// for ; paxos.DeleteSeq <= min; paxos.DeleteSeq++ {
	// 	paxos.deleteFile(paxos.Dele)
	// }
	return min
}
