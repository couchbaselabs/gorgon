package nemeses

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/jrpc"
	"github.com/couchbaselabs/gorgon/src/gorgon/rpcs"
	"github.com/couchbaselabs/gorgon/src/gorgon/splitmix"
)

// Exportable instruction struct, will be sent over the wire
type KillAndClearInstruction struct {
	//instruction objects for both kill and lazyfs clear cache
	KillInstruction   *rpcs.KillInstruction
	LazyFsInstruction *rpcs.LazyFsInstruction
}

// Instruction Interface defines 2 functions
func (instruction *KillAndClearInstruction) ForSelf() bool {
	return true
}

func (instruction *KillAndClearInstruction) String() string {
	return "KillAndClearLazyfs"
}

type killAndClear struct {
	//Not sure what to put here
	client  *rpc.Client //the client that will use this generator,will be done with jrpc dial
	next    time.Time   //Recurring nemesis, need to track when to run this recurringly
	process string      //init info for kill instruction
	fault   string      //init info for lazyfs Instruction
	node    string
}

// probs takes input here
func NewKillAndClearNemesis(process, fault string) *killAndClear {
	return &killAndClear{
		process: process,
		fault:   fault,
	}
}

func (nemesis *killAndClear) SetUp(opt *gorgon.Options) error {
	nemesis.node = opt.Nodes[splitmix.Rand.Intn(len(opt.Nodes))]
	client, err := jrpc.Dial(fmt.Sprintf("%s:%d", nemesis.node, opt.RpcPort), []byte(opt.RpcPassword))
	if err != nil {
		return err
	}
	nemesis.client = client
	nemesis.next = time.Now().Add(4 * time.Second)
	return nil
}

// Returns the next instruction, next step is invoke
func (nemesis *killAndClear) Next(client int) (gorgon.Instruction, error) {
	if client >= 0 || time.Until(nemesis.next) > 0 {
		return nil, nil
	}
	//time.Time has a method to add to the time variable .Add(time in seconds/hours)
	nemesis.next = nemesis.next.Add(8 * time.Second)
	return &KillAndClearInstruction{
		KillInstruction: &rpcs.KillInstruction{
			Process: nemesis.process,
			Signal:  9,
		},
		LazyFsInstruction: &rpcs.LazyFsInstruction{
			Fault: nemesis.fault,
			Node:  nemesis.node,
		},
	}, nil
}

func (nemesis *killAndClear) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	if instr, ok := instruction.(*KillAndClearInstruction); ok {
		var reply string
		err := nemesis.client.Call("KillRpc.Pkill", instr.KillInstruction, &reply)
		if err != nil {
			return getTime(), err
		}
		err = nemesis.client.Call("LazyFsRpc.LazyFsRpc", instr.LazyFsInstruction, &reply)
		return getTime(), err
	}
	return -1, gorgon.ErrUnsupportedInstruction
}

func (nemesis *killAndClear) Name() string {
	return fmt.Sprintf("Kill(%s)andLazyfs", nemesis.process)
}

func (nemesis *killAndClear) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (nemesis *killAndClear) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

func (nemesis *killAndClear) TearDown() error {
	if nemesis.client == nil {
		return nil
	}
	return nemesis.client.Close()
}
