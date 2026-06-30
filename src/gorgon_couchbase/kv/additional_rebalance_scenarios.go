package kv

import (
	"errors"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

type additionalRebalanceGenerator struct {
	db          *database
	addNodes    []string
	removeNodes []string
	mode        string // sequential / bulk
	nodes       []string
	invokeTime  time.Time
	apiNode     string
	done        bool // Dual purpose - also an indicator of a bad rebalance config
}

type SequentialRebalanceInstruction struct {
	RebalanceInNodes  []string
	RebalanceOutNodes []string
}

func (instr *SequentialRebalanceInstruction) String() string {
	return "SequentialRebalanceInstruction"
}

func (instr *SequentialRebalanceInstruction) ForSelf() bool {
	return true
}

type BulkRebalanceInstruction struct {
	RebalanceInNodes  []string
	RebalanceOutNodes []string
}

func (instr *BulkRebalanceInstruction) String() string {
	return "BulkRebalanceInstruction"
}

func (instr *BulkRebalanceInstruction) ForSelf() bool {
	return true
}

func NewAdditionalRebalanceGenerator(db *database, addNodes, removeNodes []string, mode string) gorgon.Generator {
	// return stub generator to signify bad configuration
	if mode != "sequential" && mode != "bulk" {
		log.Warning("provided mode not supported - sequential / bulk; workload will fail with bad config error")
		return &additionalRebalanceGenerator{done: true}
	}
	if len(addNodes) != 2 || len(removeNodes) != 2 {
		log.Warning("require 2 nodes in these rebalance scenarios; workload will fail with bad config error")
		return &additionalRebalanceGenerator{done: true}
	}
	return &additionalRebalanceGenerator{
		db:          db,
		addNodes:    addNodes,
		removeNodes: removeNodes,
		mode:        mode,
	}
}

func (rebalance *additionalRebalanceGenerator) SetUp(opt *gorgon.Options) error {
	if rebalance.done { // early error return in case of bad config
		return errors.New("bad config given to sequential / bulk rebalance")
	}
	rebalance.nodes = make([]string, len(rebalance.db.options.Nodes))
	copy(rebalance.nodes, rebalance.db.options.Nodes) // copy as options's Nodes field is shared across workloads
	rebalance.invokeTime = time.Now().Add(5 * time.Second)
	// set api node to send api requests to
	for _, node := range rebalance.db.options.Nodes {
		if node != rebalance.addNodes[0] && node != rebalance.addNodes[1] {
			rebalance.apiNode = node
			break
		}
	}
	return nil
}

func (rebalance *additionalRebalanceGenerator) Next(client int) (gorgon.Instruction, error) {
	if client >= 0 {
		return nil, nil
	}
	if rebalance.done || time.Until(rebalance.invokeTime) > 0 {
		return nil, nil
	}
	rebalance.done = true
	switch rebalance.mode {
	case "sequential":
		return &SequentialRebalanceInstruction{
			RebalanceInNodes:  rebalance.addNodes,
			RebalanceOutNodes: rebalance.removeNodes,
		}, nil
	case "bulk":
		return &BulkRebalanceInstruction{
			RebalanceInNodes:  rebalance.addNodes,
			RebalanceOutNodes: rebalance.removeNodes,
		}, nil
	default:
		return nil, nil
	}
}

// function to resize the known nodes array that is a parameter to the rest api for rebalance-out
func (rebalance *additionalRebalanceGenerator) knownNodesResize(removeNode string) {
	lastNode := rebalance.nodes[len(rebalance.nodes)-1] // last node in the cluster to swap with removeNode
	for i, node := range rebalance.nodes {
		if node == removeNode {
			rebalance.nodes[i] = lastNode
			rebalance.nodes = rebalance.nodes[:len(rebalance.nodes)-1] // resize by removing the last element
			break
		}
	}
}

func (rebalance *additionalRebalanceGenerator) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	switch instr := instruction.(type) {
	case *SequentialRebalanceInstruction:
		// sequential out followed by sequential in
		for _, node := range instr.RebalanceOutNodes {
			ejected := []string{node}
			err := rebalance.db.rebalance(rebalance.apiNode, rebalance.nodes, ejected)
			if err != nil {
				return getTime(), err
			}
			err = rebalance.db.waitForRebalance(rebalance.apiNode)
			if err != nil {
				return getTime(), err
			}
			rebalance.knownNodesResize(node)
		}
		for _, node := range instr.RebalanceInNodes {
			err := rebalance.db.requestAddNode(rebalance.apiNode, node)
			if err != nil {
				return getTime(), err
			}
			rebalance.nodes = append(rebalance.nodes, node)
			err = rebalance.db.rebalance(rebalance.apiNode, rebalance.nodes, nil)
			if err != nil {
				return getTime(), err
			}
			err = rebalance.db.waitForRebalance(rebalance.apiNode)
			if err != nil {
				return getTime(), err
			}
		}
		return getTime(), nil
	case *BulkRebalanceInstruction:
		// bulk rebalance out followed by bulk rebalance in
		ejected := instr.RebalanceOutNodes
		err := rebalance.db.rebalance(rebalance.apiNode, rebalance.nodes, ejected)
		if err != nil {
			return getTime(), err
		}
		err = rebalance.db.waitForRebalance(rebalance.apiNode)
		if err != nil {
			return getTime(), err
		}
		time.Sleep(5 * time.Second)
		// bulk rebalance in
		for _, node := range instr.RebalanceInNodes {
			err := rebalance.db.requestAddNode(rebalance.apiNode, node)
			if err != nil {
				return getTime(), err
			}
		}
		err = rebalance.db.rebalance(rebalance.apiNode, rebalance.nodes, nil)
		if err != nil {
			return getTime(), err
		}
		err = rebalance.db.waitForRebalance(rebalance.apiNode)
		if err != nil {
			return getTime(), err
		}
		return getTime(), nil
	default:
		return -1, gorgon.ErrUnsupportedInstruction
	}
}

func (rebalance *additionalRebalanceGenerator) Name() string {
	switch rebalance.mode {
	case "sequential":
		return "SequentialRebalance"
	case "bulk":
		return "BulkRebalance"
	}
	return ""
}

func (rebalance *additionalRebalanceGenerator) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (rebalance *additionalRebalanceGenerator) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

func (rebalance *additionalRebalanceGenerator) TearDown() error {
	return nil
}
