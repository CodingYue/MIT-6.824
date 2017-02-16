package paxos

type StateMachine struct {
	// Acceptor     map[int]AcceptorState
	PrepareMax   map[int]int
	AcceptMax    map[int]int
	AccpetValue  map[int]interface{}
	DecidedValue map[int]interface{}
	SeqMax       int
	DoneMax      []int
	DeleteSeq    int
}

func MakePaxosStateMachine(npaxos int) *StateMachine {
	paxos := &StateMachine{}
	paxos.PrepareMax = map[int]int{}
	paxos.AcceptMax = map[int]int{}
	paxos.AccpetValue = map[int]interface{}{}
	paxos.DecidedValue = map[int]interface{}{}
	paxos.DoneMax = make([]int, npaxos)
	for i := 0; i < npaxos; i++ {
		paxos.DoneMax[i] = -1
	}
	paxos.SeqMax = -1
	return paxos
}

func (paxos *StateMachine) getDeleteSeq() int {
	return paxos.DeleteSeq
}

func (paxos *StateMachine) setDeleteSeq(seq int) {
	paxos.DeleteSeq = seq
}

func (paxos *StateMachine) setDoneMax(from int, done int) {
	paxos.DoneMax[from] = done
}

func (paxos *StateMachine) getDoneMax(who int) int {
	return paxos.DoneMax[who]
}

func (paxos *StateMachine) setSeqMax(seq int) {
	paxos.SeqMax = seq
}

func (paxos *StateMachine) getSeqMax() int {
	return paxos.SeqMax
}

func (paxos *StateMachine) setDecidedValue(seq int, v interface{}) {
	paxos.DecidedValue[seq] = v
}

func (paxos *StateMachine) getDecidedValue(seq int) (interface{}, bool) {
	decidedValue, ok := paxos.DecidedValue[seq]
	if !ok {
		decidedValue = nil
	}
	return decidedValue, ok
}

func (paxos *StateMachine) setPrepareMax(seq int, proposalID int) {
	paxos.PrepareMax[seq] = proposalID
}

func (paxos *StateMachine) getPrepareMax(seq int) int {
	value, ok := paxos.PrepareMax[seq]
	if !ok {
		value = -1
	}
	return value
}

func (paxos *StateMachine) setAcceptMax(seq int, proposalID int) {
	paxos.AcceptMax[seq] = proposalID
}

func (paxos *StateMachine) getAccpetMax(seq int) int {
	value, ok := paxos.AcceptMax[seq]
	if !ok {
		value = -1
	}
	return value
}

func (paxos *StateMachine) setAccpetorValue(seq int, v interface{}) {
	paxos.AccpetValue[seq] = v
}

func (paxos *StateMachine) getAccpetorValue(seq int) interface{} {
	value, ok := paxos.AccpetValue[seq]
	if !ok {
		return nil
	}
	return value
}

func (paxos *StateMachine) getMinimumDoneMax() int {
	min := paxos.DoneMax[0]
	for _, v := range paxos.DoneMax {
		if v < min {
			min = v
		}
	}
	for ; paxos.DeleteSeq <= min; paxos.DeleteSeq++ {
		delete(paxos.DecidedValue, paxos.DeleteSeq)
		delete(paxos.AcceptMax, paxos.DeleteSeq)
		delete(paxos.PrepareMax, paxos.DeleteSeq)
		delete(paxos.AccpetValue, paxos.DeleteSeq)
	}
	return min
}
