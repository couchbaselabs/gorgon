package workloads

import (
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/generators"
)

// GetSetWorkload returns a workload that performs get and set operations on a harcoded set of keys.
func GetSetWorkload() gorgon.Workload {
	keys := []string{"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7"}

	//it returns a basic workload with a staggered getset generator that generates get and set instructions on the specified keys
	return gorgon.Workload{
		Model:      GetSetModel(),
		Generators: []gorgon.Generator{generators.Stagger(generators.NewGetSetGenerator(keys), time.Millisecond)},
	}
}


//This is an implementation of the model interface which is a component within the workload 
func GetSetModel() gorgon.Model {
	return gorgon.Model{
		//all the functions are implemented in place except the DescribeOperation and Partition functions
		Init: func() []gorgon.State { return []gorgon.State{gorgon.IntMap{}} },
		Hash: func(state gorgon.State) uint64 {
			return state.(gorgon.IntMap).Hash()
		},
		Equal: func(s1, s2 gorgon.State) bool {
			return s1.(gorgon.IntMap).Equals(s2.(gorgon.IntMap))
		},
		DescribeState: func(state gorgon.State) string {
			return state.(gorgon.IntMap).String()
		},
		DescribeOperation: DescribeOperation,      //this is defined in a separate file in the same package
		Partition:         PartitionByKey,         //this is defined in a separate file in the same package
		Step: func(state gorgon.State, input gorgon.Instruction, output gorgon.Output) []gorgon.State {
			stateMap := state.(gorgon.IntMap)
			switch instr := input.(type) {
			case *generators.GetInstruction:
				if _, ok := output.(error); ok {
					return []gorgon.State{state}
				}
				if val, ok := stateMap.Get(instr.Key); ok {
					if i, ok := output.(int); ok && val == i {
						return []gorgon.State{state}
					}
					return nil
				}
				if output == nil {
					return []gorgon.State{state}
				}
				return nil
			case *generators.SetInstruction:
				stateMap = stateMap.Put(instr.Key, instr.Value)
				if output != nil {
					if _, ok := output.(error); ok {
						return []gorgon.State{state, stateMap}
					}
					return nil
				}
				return []gorgon.State{stateMap}
			}
			return nil
		},
		Values: func(input gorgon.Instruction, output gorgon.Output) (reads []gorgon.KeyValueInt, writes []gorgon.KeyValueInt) {
			switch instr := input.(type) {
			case *generators.GetInstruction:
				if i, ok := output.(int); ok {
					return []gorgon.KeyValueInt{{Key: instr.Key, Value: i}}, nil
				}
			case *generators.SetInstruction:
				if _, ok := output.(error); !ok {
					return nil, []gorgon.KeyValueInt{{Key: instr.Key, Value: instr.Value}}
				}
			}
			return nil, nil
		},
		ValuesOvewritten: func(state gorgon.State, input gorgon.Instruction, output gorgon.Output) []gorgon.KeyValueInt {
			if _, ok := output.(error); ok {
				return nil
			}
			if instr, ok := input.(*generators.SetInstruction); ok {
				if i, ok := state.(gorgon.IntMap).Get(instr.Key); ok {
					return []gorgon.KeyValueInt{{Key: instr.Key, Value: i}}
				}
			}
			return nil
		},
	}
}
