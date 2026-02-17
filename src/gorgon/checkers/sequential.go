package checkers

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

type CheckResult string

const (
	Unknown CheckResult = "Unknown" // timed out
	Ok      CheckResult = "Ok"
	Illegal CheckResult = "Illegal"
)

func CheckSeqnuentialConsistency(m gorgon.Model, history [][]gorgon.Operation, timeout time.Duration) (result CheckResult, info []int) {
	if timeout <= 0 {
		return checkSeqnuentialConsistency(m, history, nil)
	}
	stop := &atomic.Bool{}
	resultChan := make(chan bool)
	timeoutChan := time.After(timeout)
	go func() {
		result, info = checkSeqnuentialConsistency(m, history, stop)
		resultChan <- true
	}()
	for {
		select {
		case <-resultChan:
			return result, info
		case <-timeoutChan:
			stop.Store(true)
		}
	}
}

func checkSeqnuentialConsistency(m gorgon.Model, history [][]gorgon.Operation, stop *atomic.Bool) (CheckResult, []int) {
	//Represents a stack frame in the DFS algorithm which uses heap allocated stack.
	type stackFrame struct {
		progress []int //This is an int array that stores the count of operations performed by each thread
		state    gorgon.State
		threads  []int
		seqLen   int  //length of the stack
		checked  int  // How many threads have been checked for the current stack frame
		mutable  bool // mutable phase and the non mutable phase
	}

	//create a cache item to store the state and progress of the threads
	type cacheItem struct {
		progress []int
		state    gorgon.State
	}

	//create a cache to store the states and progress of the threads
	cache := NewCache(
		8_000_000, //size of the cache
		func(a any) uint64 { //hash function to hash the state and progress
			item := a.(*cacheItem)
			h := m.Hash(item.state)
			for _, p := range item.progress {
				h = h*0x100000001b3 + uint64(p)
			}
			return h
		},
		func(a, b any) bool { //equality function to check if the cached state is the same.
			ia := a.(*cacheItem)
			ib := b.(*cacheItem)
			if len(ia.progress) != len(ib.progress) {
				return false
			}
			for i := range ia.progress {
				if ia.progress[i] != ib.progress[i] {
					return false
				}
			}
			return m.Equal(ia.state, ib.state)
		})

	//calculate the total number of operations in the history
	numOps := 0
	for _, ops := range history {
		numOps += len(ops)
	}

	//create the constraints map to implement the heuristics
	constraints := findReadWriteConstraints(history, m.Values)

	//Init the stack as a slice of stack frames with the initial stack frame.
	stack := []stackFrame{{
		progress: make([]int, len(history)),
		state:    m.Init()[0]}}
	stack[0].threads = sortedThreads(history, stack[0].progress, -1)
	var result []int
	var sequence []int
loop:
	for {
		if stop != nil && stop.Load() {
			cache.Clear()
			return Unknown, result
		}
		frame := &stack[len(stack)-1]

		//If all threads have been checked for the current stack frame, backtrack.
		if frame.checked >= len(history) {
			frame = nil
			stack = stack[:len(stack)-1]
			if len(stack) == 0 {
				break
			}

			//Truncate the sequence to the length of that of previous stack frame.
			sequence = sequence[:stack[len(stack)-1].seqLen]
			continue
		}

		var nextProgress []int
		var nextState gorgon.State
		lastThread := -1

		//branch to take when the frame is in mutable phase
		if frame.mutable {
			//Select the next thread to be checked from the threads array.
			thread := frame.threads[frame.checked]
			//increment the count of checked
			frame.checked++

			//idx stores the number of operations already performed by the thread
			idx := frame.progress[thread]

			//If the thread has performed all its operations, continue to the next thread.
			if idx >= len(history[thread]) {
				continue
			}

			//Fetch the next operation to be performed by the thread.
			op := history[thread][idx]

			//Step the model to get the next states for the operation.
			nextStates := m.Step(frame.state, op.Input, op.Output)

			//continue to the next thread if the number of next states is not 1 or the next state is equal to the current state
			if len(nextStates) != 1 {
				continue
			}
			if m.Equal(frame.state, nextStates[0]) {
				continue
			}

			//create a new progress slice by copying and incrementing the progress value for the partciular thread
			nextProgress = make([]int, len(frame.progress))
			copy(nextProgress, frame.progress)
			nextProgress[thread]++

			//
			if m.ValuesOvewritten != nil {
				//the return value of the function ValuesOvewritten is a slice of KeyValueInt objects that represent the keys and values that have been overwritten by the operation.
				changed := m.ValuesOvewritten(frame.state, op.Input, op.Output)
				for _, c := range changed { //iterate over each key value object in the changed slice

					//iterate ove the constraints map with one rWconstraint per thread index t.
					//Dont perform the loop if the last read index is greater than or equal to the next progress value for the thread.
					//Heuristic 1
					for t, constr := range constraints[c] {
						if constr.lastRead >= nextProgress[t] {
							continue loop
						}
					}
				}
			}
			if m.Values != nil {
				_, writes := m.Values(op.Input, op.Output)
				for _, w := range writes {
					for t, constr := range constraints[w] {
						if constr.lastRead > nextProgress[t] && constr.prevWrite >= nextProgress[t] {
							continue loop
						}
					}
				}
			}
			lastThread = thread
			nextState = nextStates[0]
			sequence = append(sequence, thread)
		} else { //This is the immutable phase where we batch no-op or get operations and apply them at once
			frame.mutable = true
			nextProgress = make([]int, len(frame.progress))
			copy(nextProgress, frame.progress)

			//For each thread in the history
			for thread := range history {
				for { //This is the inner loop that processes operations for a single thread
					//Process all operations till thread either runs out of operations or a no-op.
					idx := nextProgress[thread]

					//If all of a threads ops are done, move on to the next thread.
					if idx >= len(history[thread]) {
						break
					}

					//Fetch the next operation to be performed by the thread.
					op := history[thread][idx]

					//Step the model to get the next states for the operation.
					nextStates := m.Step(frame.state, op.Input, op.Output)

					//continue to the next thread if the number of next states is not 1 or the next state is equal to the current state
					if len(nextStates) != 1 {
						break
					}

					//If the next state is not equal to the current state, break out of the loop as we are looking for no-ops only
					if !m.Equal(frame.state, nextStates[0]) {
						break
					}

					//update the last thread and the next progress value for the thread
					lastThread = thread
					nextProgress[thread]++
					sequence = append(sequence, thread)
				}
			}
			//LastThread value stays -1 if no-ops were not found for any of the threads.
			if lastThread < 0 {
				continue
			}
			nextState = frame.state //The next state is the current state as we are applying no-ops only
		}

		//success case, when history is sequential consistent
		if len(sequence) == numOps {
			cache.Clear()
			return Ok, sequence
		}

		item := &cacheItem{nextProgress, nextState}
		prevLen := cache.Len()

		//Cache insert returned true
		if cache.Insert(item) {
			if prevLen != cache.Len() && cache.Len()%1_000_000 == 0 {
				log.Info("Sequential consistency cache size: %d", cache.Len())
			}
			if len(sequence) > len(result) {
				result = make([]int, len(sequence))
				copy(result, sequence)
			}
			frame = nil
			stack = append(stack, stackFrame{
				progress: nextProgress,
				state:    nextState,
				threads:  sortedThreads(history, nextProgress, lastThread),
				seqLen:   len(sequence)})
		} else { //Cache insert returned false, so the state and progress are already in the cache
			//Backtrack and try next thread in the current frame
			sequence = sequence[:frame.seqLen]
		}
	}

	//Cache insert returned false for all threads in the current frame, so the history is not sequential consistent
	//We still return Illegal and the result slice which is the best sequence found.
	cache.Clear()
	return Illegal, result
}

// This function is used to sort the threads based on the progress and the index for a particular stack frame
func sortedThreads(history [][]gorgon.Operation, progress []int, index int) []int {
	threads := make([]int, len(history))
	//This fills the slice with initial ordering of the threads
	//This places the previously ran thread towards the end of the array and the next thread to be run at the beginning of the array
	for i := range threads {
		index++
		if index >= len(threads) {
			index = 0
		}
		threads[i] = index
	}

	//custom comparator to sort the threads on the basis of the call order of the operations
	sort.Slice(threads, func(i, j int) bool {
		i = threads[i]
		j = threads[j]
		pi := progress[i]
		pj := progress[j]
		if pi >= len(history[i]) {
			return false
		}
		if pj >= len(history[j]) {
			return true
		}
		return history[i][pi].Call < history[j][pj].Call
	})
	return threads
}

type rwConstraint struct {
	lastRead  int
	prevWrite int
}

// This function is used to create a map that is used to implement the heuristics
func findReadWriteConstraints(
	history [][]gorgon.Operation,
	values func(input gorgon.Instruction, output gorgon.Output) ([]gorgon.KeyValueInt, []gorgon.KeyValueInt),
) map[gorgon.KeyValueInt][]rwConstraint {

	//creates a mapping between (key, value) object and (last read index, last write index) object
	constraints := make(map[gorgon.KeyValueInt][]rwConstraint)
	if values == nil {
		return constraints
	}

	//iterate over each thread and each operation of the thread
	for t, ops := range history {
		lastWrites := make(map[string]int)
		for i, op := range ops {
			reads, writes := values(op.Input, op.Output)
			for _, r := range reads {
				if _, ok := constraints[r]; !ok {
					v := make([]rwConstraint, len(history))
					for j := range v {
						v[j] = rwConstraint{-1, -1}
					}
					constraints[r] = v
				}
				prevWrite := -1
				if w, ok := lastWrites[r.Key]; ok {
					prevWrite = w
				}
				constraints[r][t] = rwConstraint{i, prevWrite}
			}
			for _, w := range writes {
				lastWrites[w.Key] = i
			}
		}
	}
	return constraints
}
