package workloads

import (
	"sort"

	"github.com/couchbaselabs/gorgon/src/gorgon"
)

// This is the partition function that the porcupine library expects
func PartitionByKey(history []gorgon.Operation) (ret [][]gorgon.Operation) {
	operations := make(map[string][]gorgon.Operation)
	//only operations with a key are partitioned
	for _, op := range history {
		instr, ok := op.Input.(interface{ GetKey() string })
		if !ok {
			continue
		}
		key := instr.GetKey()
		operations[key] = append(operations[key], op)
	}

	//create a list of keyOps Structs and sort them in place by key
	type keyOps struct {
		key string
		ops []gorgon.Operation
	}
	var list []keyOps
	for key, ops := range operations {
		list = append(list, keyOps{key, ops})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].key < list[j].key })
	for _, ops := range list {
		ret = append(ret, ops.ops)
	}
	return
	//Return a list of lists of operations, each list is a partition of the history.
}
