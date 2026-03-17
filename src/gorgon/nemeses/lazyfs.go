package nemeses

import (
	"fmt"
	"net/rpc"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/jrpc"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
	"github.com/couchbaselabs/gorgon/src/gorgon/rpcs"
	"github.com/couchbaselabs/gorgon/src/gorgon/splitmix"
)

//Instruction struct defined in the rpcs package

// Implements the generator interface
// We make this struct private and we return the gorgon Generator object which is the wrapper over it
type lazyFsNemesis struct {
	node      string
	nodeIdx   int
	fault     string
	client    *rpc.Client //init in the setup function with jrpc.Dial
	done      bool        //boolean to check if the lazyfs nemesis has been called already
	faultTime time.Time
}

// constructor that returns a new object of the nemesis type
// We return the Generator obejct because we call this function within Add, which takes only a generator
// object, which is a wrapper over different types of generators
// we return a pointer to the constructed struct, as its heap allocated and we wanna continue having reference of it
// even after this function ends
func NewLazyFsNemesis(fault string) gorgon.Generator {
	return &lazyFsNemesis{
		fault: fault,
	}
}

func (nemesis *lazyFsNemesis) Name() string {
	//Sprintf formats the string and returns the formatted string
	return fmt.Sprintf("(%s)-lazyfs_fault", nemesis.fault)
}

// Setup function, this is used to create the nemesis object and setup anything else thats required
// A generator's setup method is called from within the Runner's setup, which runs per workload
func (nemesis *lazyFsNemesis) SetUp(opt *gorgon.Options) error {
	//Selecting a random node to fault
	nemesis.nodeIdx = splitmix.Rand.Intn(len(opt.Nodes))

	//set the node field of the nemesis object with this node from the opt.Nodes
	nemesis.node = opt.Nodes[nemesis.nodeIdx]

	//jrpc.Dial takes 2 inputs in the form (address:port, array of bytes password)
	client, err := jrpc.Dial(fmt.Sprintf("%s:%d", nemesis.node, opt.RpcPort), []byte(opt.RpcPassword))
	if err != nil {
		return err
	}

	//node field is set when object is created
	nemesis.client = client

	//Setting the fault_time field to 1/2 of the time
	now := time.Now()
	nemesis.faultTime = now.Add(opt.WorkloadDuration / 2) //setting the time for fault to be 30 seconds
	return nil
}

// Invoke function
func (nemesis *lazyFsNemesis) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	//type assertion
	if instr, ok := instruction.(*rpcs.LazyFsInstruction); ok {
		var reply string
		//rpc call
		err := nemesis.client.Call("LazyFsRpc.InvokeLazyFsRpc", instr, &reply)
		if err != nil {
			log.Info("rpc call for the lazyfs fault failed")
		}
		return getTime(), err
	}
	return -1, gorgon.ErrUnsupportedInstruction
}

func (nemesis *lazyFsNemesis) Next(client int) (gorgon.Instruction, error) {
	if client >= 0 {
		return nil, nil
	}
	if nemesis.done {
		return nil, nil
	}
	if time.Until(nemesis.faultTime) > 0 {
		return nil, nil
	}
	nemesis.done = true
	return &rpcs.LazyFsInstruction{Fault: nemesis.fault, Node: nemesis.node}, nil
}

// no-op
func (*lazyFsNemesis) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (*lazyFsNemesis) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

// teardown the gen when the rpcClient becomes nil
func (nemesis *lazyFsNemesis) TearDown() error {
	if nemesis.client == nil {
		return nil
	}
	return nemesis.client.Close()
}
