package nemeses

import (
	"fmt"
	"net/rpc"
	"strconv"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/jrpc"
	"github.com/couchbaselabs/gorgon/src/gorgon/splitmix"
)

// This is the instruction object for network partition.
// An instruction (either a get-set instruction or a nemesis instruction) is produced by the .Next() function and passed into the .Invoke() method
type PartitionNodeInstruction struct {
	Node int
	Heal bool
}

// The below two functions are implementations of the gorgon.Instruction interface.
// This is the string representation of the instruction object, makes the instruction readable.
func (instr *PartitionNodeInstruction) String() string {
	if instr.Heal {
		return fmt.Sprintf("HealNode(%d)", instr.Node)
	}
	return fmt.Sprintf("PartitionNode(%d)", instr.Node)
}

// This is a method on the instruction object that returns true if the instruction is meant to be executed by the generator itself.
func (*PartitionNodeInstruction) ForSelf() bool {
	return true
}

func NewNetworkPartitionNemesis(allowedPorts ...int) gorgon.Generator {
	return &networkPartition{allowedPorts: allowedPorts}
}

// This is a struct that is equivalent to a generator for nemesis operations.
// It implements the gorgon.Generator interface, and becomes a part of the generator list of the workload.
type networkPartition struct {
	allowedPorts  []int
	client        *rpc.Client
	node          string
	nodeIdx       int
	partitioned   bool
	healed        bool
	partitionTime time.Time
	healTime      time.Time
}

func (*networkPartition) Name() string {
	return "NetworkPartition"
}

func (*networkPartition) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (*networkPartition) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

func (nemesis *networkPartition) Next(client int) (gorgon.Instruction, error) {
	//if the client is a normal client, return nil.
	if client >= 0 {
		return nil, nil
	}
	//if the client is nemesis client
	if !nemesis.partitioned {
		if time.Until(nemesis.partitionTime) > 0 {
			return nil, nil
		}
		nemesis.partitioned = true
		return &PartitionNodeInstruction{Node: nemesis.nodeIdx, Heal: false}, nil
	}
	if !nemesis.healed {
		if time.Until(nemesis.healTime) > 0 {
			return nil, nil
		}
		nemesis.healed = true
		return &PartitionNodeInstruction{Node: nemesis.nodeIdx, Heal: true}, nil
	}
	return nil, nil
}

// Setting up other fields of the nemesis object, configured via the opt object
func (nemesis *networkPartition) SetUp(opt *gorgon.Options) error {
	now := time.Now()
	nemesis.nodeIdx = splitmix.Rand.Intn(len(opt.Nodes))
	nemesis.node = opt.Nodes[nemesis.nodeIdx]
	//time to partition is 1/4 of the total duration
	nemesis.partitionTime = now.Add(opt.WorkloadDuration / 4)
	//time to heal the partition is 3/4 of the total
	nemesis.healTime = now.Add(opt.WorkloadDuration * 3 / 4)
	client, err := jrpc.Dial(fmt.Sprintf("%s:%d", nemesis.node, opt.RpcPort), []byte(opt.RpcPassword))
	if err != nil {
		return err
	}
	nemesis.client = client
	err = nemesis.iptables("-A", "INPUT", "-p", "tcp", "--dport", strconv.Itoa(opt.RpcPort), "-j", "ACCEPT")
	if err != nil {
		return err
	}
	return nemesis.iptables("-A", "OUTPUT", "-p", "tcp", "--sport", strconv.Itoa(opt.RpcPort), "-j", "ACCEPT")
}

func (nemesis *networkPartition) TearDown() error {
	if nemesis.client == nil {
		return nil
	}
	defer nemesis.client.Close()
	err := nemesis.iptables("-P", "INPUT", "ACCEPT")
	if err != nil {
		return err
	}
	err = nemesis.iptables("-P", "OUTPUT", "ACCEPT")
	if err != nil {
		return err
	}
	return nemesis.iptables("-F")
}

func (nemesis *networkPartition) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	heal := false
	if instr, ok := instruction.(*PartitionNodeInstruction); ok {
		if instr.Node != nemesis.nodeIdx {
			return -1, fmt.Errorf("NetworkPartition: invalid node index %d, expected %d", instr.Node, nemesis.nodeIdx)
		}
		heal = instr.Heal
	} else {
		return -1, gorgon.ErrUnsupportedInstruction
	}
	if heal {
		if err := nemesis.iptables("-P", "INPUT", "ACCEPT"); err != nil {
			return getTime(), err
		}
		if err := nemesis.iptables("-P", "OUTPUT", "ACCEPT"); err != nil {
			return getTime(), err
		}
		if err := nemesis.iptables("-F"); err != nil {
			return getTime(), err
		}
	} else {
		for _, port := range nemesis.allowedPorts {
			err := nemesis.iptables("-A", "INPUT", "-p", "tcp", "--dport", strconv.Itoa(port), "-j", "ACCEPT")
			if err != nil {
				return getTime(), err
			}
			err = nemesis.iptables("-A", "OUTPUT", "-p", "tcp", "--sport", strconv.Itoa(port), "-j", "ACCEPT")
			if err != nil {
				return getTime(), err
			}
		}
		if err := nemesis.iptables("-A", "INPUT", "-j", "DROP"); err != nil {
			return getTime(), err
		}
		if err := nemesis.iptables("-A", "OUTPUT", "-j", "DROP"); err != nil {
			return getTime(), err
		}
	}
	return getTime(), nil
}

func (nemesis *networkPartition) iptables(args ...string) error {
	var reply string
	return nemesis.client.Call("IpTablesRpc.IpTables", &args, &reply)
}
