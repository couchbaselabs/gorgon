package kv

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/jrpc"
	"github.com/couchbaselabs/gorgon/src/gorgon/rpcs"
)

type rebalanceGenerator struct {
	db             *database
	addNode        string
	removeNode     string
	mode           string
	apiNode        string   // Node to send the REST API requests
	nodes          []string // Set of nodes in the cluster in current rebalance scenario
	invokeTime     time.Time
	done           bool   // Dual purpose - also an indicator of a bad rebalance config
	crashLater     bool   // Only set to true when a crash process is specified
	partitionLater bool   // Only set to true when a single node is specified implying network partition
	process        string // Only set when rebalance is followed by a process kill
	targetNode     string // Set in all rebalance + process crash cases
}

type RebalanceInInstruction struct {
	addNode string
}

func (instr *RebalanceInInstruction) String() string {
	return "RebalanceIn(" + instr.addNode + ")"
}

func (instr *RebalanceInInstruction) ForSelf() bool {
	return true
}

type RebalanceOutInstruction struct {
	removeNode string
}

func (instr *RebalanceOutInstruction) String() string {
	return "RebalanceOut(" + instr.removeNode + ")"
}

func (instr *RebalanceOutInstruction) ForSelf() bool {
	return true
}

type SwapRebalanceInstruction struct {
	addNode    string
	removeNode string
}

func (instr *SwapRebalanceInstruction) String() string {
	return "SwapRebalance(" + instr.addNode + ", " + instr.removeNode + ")"
}

func (instr *SwapRebalanceInstruction) ForSelf() bool {
	return true
}

func NewRebalanceGenerator(db *database, addNode, removeNode string, args ...string) gorgon.Generator {
	var mode string

	if addNode != "" && removeNode != "" {
		mode = "swap"
	} else if addNode != "" {
		mode = "rebalance-in"
	} else {
		mode = "rebalance-out"
	}

	// return stub generator to signify bad rebalance config
	if (mode == "swap" || mode == "rebalance-in") && addNode != db.options.AdditionalNodes[0] {
		return &rebalanceGenerator{done: true}
	}

	var partitionLater bool
	var crashLater bool
	var targetNode string
	var processName string

	if len(args) == 1 { // only node specified in variadic arguments i.e partition test
		partitionLater = true
		targetNode = args[0]
	} else if len(args) == 2 { // process and node specified in the variadic arguments i.e process-crash test
		crashLater = true
		processName = args[0]
		targetNode = args[1]
	}
	return &rebalanceGenerator{
		db:             db,
		addNode:        addNode,
		removeNode:     removeNode,
		mode:           mode,
		process:        processName,
		crashLater:     crashLater,
		partitionLater: partitionLater,
		targetNode:     targetNode,
	}
}

func (rebalance *rebalanceGenerator) SetUp(opt *gorgon.Options) error {
	if rebalance.done { // early error return in case of bad config
		return errors.New("bad rebalance config provided")
	}
	rebalance.invokeTime = time.Now().Add(20 * time.Second)
	rebalance.nodes = make([]string, len(rebalance.db.options.Nodes))
	copy(rebalance.nodes, rebalance.db.options.Nodes)
	// If rebalance-in configuration
	if rebalance.mode == "rebalance-in" {
		rebalance.apiNode = rebalance.db.options.Nodes[0]
		return nil
	}
	// if rebalance-out or swap rebalance
	for _, node := range rebalance.db.options.Nodes {
		if node != rebalance.removeNode {
			rebalance.apiNode = node
			return nil
		}
	}
	return errors.New("apiNode not found")
}

func (rebalance *rebalanceGenerator) Next(client int) (gorgon.Instruction, error) {
	if client >= 0 {
		return nil, nil
	}
	if rebalance.done || time.Until(rebalance.invokeTime) > 0 {
		return nil, nil
	}
	rebalance.done = true
	switch rebalance.mode {
	case "swap":
		return &SwapRebalanceInstruction{
			addNode:    rebalance.addNode,
			removeNode: rebalance.removeNode,
		}, nil
	case "rebalance-in":
		return &RebalanceInInstruction{
			addNode: rebalance.addNode,
		}, nil
	case "rebalance-out":
		return &RebalanceOutInstruction{
			removeNode: rebalance.removeNode,
		}, nil
	default:
		return nil, errors.New("Rebalance operation not of the supported type")
	}
}

func (rebalance *rebalanceGenerator) killProcess(node string) error {
	client, err := jrpc.Dial(fmt.Sprintf("%s:%d", node, rebalance.db.options.RpcPort), []byte(rebalance.db.options.RpcPassword))
	if err != nil {
		return err
	}
	defer client.Close()
	var reply string
	return client.Call("KillRpc.Pkill", &rpcs.KillInstruction{Process: rebalance.process, Signal: 9}, &reply)
}

func (rebalance *rebalanceGenerator) createPartition(node string) error {
	client, err := jrpc.Dial(fmt.Sprintf("%s:%d", node, rebalance.db.options.RpcPort), []byte(rebalance.db.options.RpcPassword))
	if err != nil {
		return err
	}
	defer client.Close()
	iptables := func(args ...string) error {
		var reply string
		return client.Call("IpTablesRpc.IpTables", &args, &reply)
	}
	rpcPort := strconv.Itoa(rebalance.db.options.RpcPort)
	if err := iptables("-A", "INPUT", "-p", "tcp", "--dport", rpcPort, "-j", "ACCEPT"); err != nil {
		return err
	}
	if err := iptables("-A", "OUTPUT", "-p", "tcp", "--sport", rpcPort, "-j", "ACCEPT"); err != nil {
		return err
	}
	if err := iptables("-A", "INPUT", "-j", "DROP"); err != nil {
		return err
	}
	return iptables("-A", "OUTPUT", "-j", "DROP")
}

func (rebalance *rebalanceGenerator) healPartition(node string) error {
	client, err := jrpc.Dial(fmt.Sprintf("%s:%d", node, rebalance.db.options.RpcPort), []byte(rebalance.db.options.RpcPassword))
	if err != nil {
		return err
	}
	defer client.Close()
	iptables := func(args ...string) error {
		var reply string
		return client.Call("IpTablesRpc.IpTables", &args, &reply)
	}
	if err := iptables("-P", "INPUT", "ACCEPT"); err != nil {
		return err
	}
	if err := iptables("-P", "OUTPUT", "ACCEPT"); err != nil {
		return err
	}
	return iptables("-F")
}

func (rebalance *rebalanceGenerator) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	var ejected []string
	var err error

	switch instr := instruction.(type) {
	case *RebalanceInInstruction:
		err = rebalance.db.requestAddNode(rebalance.apiNode, instr.addNode)
		if err != nil {
			return getTime(), err
		}
		rebalance.nodes = append(rebalance.nodes, instr.addNode)
		ejected = []string{}
	case *RebalanceOutInstruction:
		ejected = []string{instr.removeNode}
	case *SwapRebalanceInstruction:
		err = rebalance.db.requestAddNode(rebalance.apiNode, instr.addNode)
		if err != nil {
			return getTime(), err
		}
		rebalance.nodes = append(rebalance.nodes, instr.addNode)
		ejected = []string{instr.removeNode}
	default:
		return -1, gorgon.ErrUnsupportedInstruction
	}
	err = rebalance.db.rebalance(rebalance.apiNode, rebalance.nodes, ejected)
	if err != nil {
		return getTime(), err
	}
	time.Sleep(3 * time.Second) // rebalance takes a while before starting
	if rebalance.partitionLater {
		if err = rebalance.createPartition(rebalance.targetNode); err != nil {
			return getTime(), err
		}
		// sleep till auto failover kicks-in
		time.Sleep(20 * time.Second)
		if err = rebalance.healPartition(rebalance.targetNode); err != nil {
			return getTime(), err
		}
		return getTime(), nil
	}
	if rebalance.crashLater {
		var targetNode string
		if rebalance.process == "beam.smp" {
			targetNode, err = rebalance.db.findOrchestrator(rebalance.apiNode)
			if err != nil {
				return getTime(), err
			}
			targetNode = strings.TrimPrefix(targetNode, "ns_1@")
		} else {
			if rebalance.mode != "rebalance-in" && rebalance.mode != "rebalance-out" && rebalance.mode != "swap" {
				return getTime(), errors.New("Rebalance mode unsupported")
			}
			targetNode = rebalance.targetNode
		}
		if err := rebalance.killProcess(targetNode); err != nil {
			return getTime(), err
		}
		return getTime(), nil // if kill is successful, rebalance stops and rebalance-polling can be skipped
	}
	return getTime(), rebalance.db.waitForRebalance(rebalance.apiNode)
}

func (rebalance *rebalanceGenerator) Name() string {
	switch rebalance.mode {
	case "swap":
		return "SwapRebalance"
	case "rebalance-in":
		return "RebalanceIn"
	case "rebalance-out":
		return "RebalanceOut"
	default:
		return "unknown-" + rebalance.mode
	}
}

func (rebalance *rebalanceGenerator) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (rebalance *rebalanceGenerator) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

func (rebalance *rebalanceGenerator) TearDown() error {
	return nil
}
