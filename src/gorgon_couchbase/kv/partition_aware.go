package kv

import (
	"math/rand"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/generators"
	"github.com/couchbaselabs/gorgon/src/gorgon/nemeses"
	"github.com/couchbaselabs/gorgon/src/gorgon/splitmix"
)

func NewPartitionAwareGetSetGenerator() gorgon.Generator {
	return generators.Stagger(&partitionAwareGenerator{
		keys: []string{"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7"},
		rand: splitmix.NewRand()}, 10*time.Millisecond)
}

type partitionAwareGenerator struct {
	keys     []string
	rand     *rand.Rand
	numNodes int
	node     int
	val      int
	start    time.Time
}

// Implementation of the Generator interface's Next function.
func (gen *partitionAwareGenerator) Next(client int) (gorgon.Instruction, error) {
	//Guard conditions for the generator to return nil.
	if client < 0 || gen.numNodes == 0 || gen.node < 0 || time.Until(gen.start) > 0 {
		return nil, nil
	}

	//generate a random key and fetch the vbucket id for the key
	key := gen.keys[gen.rand.Intn(len(gen.keys))]
	vb := getVbid([]byte(key), 1024)

	//if the vbucket id is not in the node's vbucket range, return nil.
	if vb < gen.node*1024/gen.numNodes || vb >= (gen.node+1)*1024/gen.numNodes {
		return nil, nil
	}

	//if the client is attached to the node, we implement a get Instruction.
	//We dont wanna issue get commands from bound clients
	if client%gen.numNodes == gen.node {
		return &generators.GetInstruction{Key: key}, nil
	}

	//Otherwise we write a Set Instruction with the value val.
	gen.val--
	return &generators.SetInstruction{Key: key, Value: gen.val}, nil
}

func (*partitionAwareGenerator) Name() string {
	return "PartitionAwareGetSet"
}

// Sets up the generator object, with the inital partition state as Not Partitioned represented by gen.node as -1.
func (gen *partitionAwareGenerator) SetUp(opt *gorgon.Options) error {
	gen.numNodes = len(opt.Nodes)
	gen.node = -1
	return nil
}

func (*partitionAwareGenerator) TearDown() error {
	return nil
}

// The worker never calls the Inboke for this generator
// The worker's Client.Invoke is called for instructions like this
// This is a no-op with an Error returned as safety.
func (*partitionAwareGenerator) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	return -1, gorgon.ErrUnsupportedInstruction
}

// No-op as no pre work has to be done to run the instruction for this generator
func (*partitionAwareGenerator) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

// A generator's On return function is called by the Worker
// For this generator, it sets the partition state based on the instruction.
func (gen *partitionAwareGenerator) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	if instr, ok := instruction.(*nemeses.PartitionNodeInstruction); ok {
		if instr.Heal {
			gen.node = -1
		} else {
			//If the node just got partitioned, set the partition state to the node id and start issuing instructions to it after 20secs.
			gen.node = instr.Node
			gen.start = time.Now().Add(20 * time.Second)
		}
	}
	return nil
}
