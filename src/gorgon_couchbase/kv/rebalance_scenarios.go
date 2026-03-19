package kv

import (
	"errors"
	"fmt"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
)

type rebalanceConfiguration struct {
	db         *database
	addNode    string
	removeNode string
	mode       string
	targetNode string   // Node to send the REST api requests
	nodes      []string // Set of nodes in the cluster in current rebalance scenario
	invokeTime time.Time
	done       bool
}

type RebalanceInInstuction struct {
	addNode string
}

func (instr *RebalanceInInstuction) String() string {
	return "RebalanceIn(" + instr.addNode + ")"
}

func (instr *RebalanceInInstuction) ForSelf() bool {
	return true
}

type RebalanceOutInstuction struct {
	removeNode string
}

func (instr *RebalanceOutInstuction) String() string {
	return "RebalanceOut(" + instr.removeNode + ")"
}

func (instr *RebalanceOutInstuction) ForSelf() bool {
	return true
}

type SwapRebalanceInstuction struct {
	addNode    string
	removeNode string
}

func (instr *SwapRebalanceInstuction) String() string {
	return "SwapRebalance(" + instr.addNode + ", " + instr.removeNode + ")"
}

func (instr *SwapRebalanceInstuction) ForSelf() bool {
	return true
}

func NewRebalanceConfiguration(db *database, addNode, removeNode string) gorgon.Generator {
	if addNode != "" && removeNode != "" { //swap rebalance
		return &rebalanceConfiguration{
			db:         db,
			addNode:    addNode,
			removeNode: removeNode,
			mode:       "swap",
		}
	} else if addNode != "" { // rebalance-in
		return &rebalanceConfiguration{
			db:      db,
			addNode: addNode,
			mode:    "rebalance-in",
		}
	}
	return &rebalanceConfiguration{ // rebalance-out
		db:         db,
		removeNode: removeNode,
		mode:       "rebalance-out",
	}
}

func (rebalance *rebalanceConfiguration) SetUp(opt *gorgon.Options) error {
	// If rebalance-in configuration
	rebalance.invokeTime = time.Now().Add(30 * time.Second)
	rebalance.nodes = rebalance.db.options.Nodes
	if rebalance.mode == "rebalance-in" {
		rebalance.targetNode = rebalance.db.options.Nodes[0]
		return nil
	}
	// if rebalance-out or swap rebalance
	for _, node := range rebalance.db.options.Nodes {
		if node != rebalance.removeNode {
			rebalance.targetNode = node
			break
		}
	}
	return nil
}

func (rebalance *rebalanceConfiguration) Next(client int) (gorgon.Instruction, error) {
	if client >= 0 {
		return nil, nil
	}
	if time.Until(rebalance.invokeTime) <= 0 && !rebalance.done {
		rebalance.done = true
		switch rebalance.mode {
		case "swap":
			return &SwapRebalanceInstuction{
				addNode:    rebalance.addNode,
				removeNode: rebalance.removeNode,
			}, nil
		case "rebalance-in":
			return &RebalanceInInstuction{
				addNode: rebalance.addNode,
			}, nil
		case "rebalance-out":
			return &RebalanceOutInstuction{
				removeNode: rebalance.removeNode,
			}, nil
		default:
			return nil, errors.New("Rebalance operation not of the supported type")
		}
	}
	return nil, nil
}

func (rebalance *rebalanceConfiguration) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	switch instr := instruction.(type) {
	case *RebalanceInInstuction:
		err := rebalance.requestAddNode(instr.addNode)
		if err != nil {
			return getTime(), err
		}
		rebalance.nodes = append(rebalance.nodes, instr.addNode)
		return getTime(), rebalance.requestRebalance(rebalance.nodes, nil)
	case *RebalanceOutInstuction:
		ejected := []string{instr.removeNode}
		return getTime(), rebalance.requestRebalance(rebalance.nodes, ejected)
	case *SwapRebalanceInstuction:
		err := rebalance.requestAddNode(instr.addNode)
		if err != nil {
			return getTime(), err
		}
		rebalance.nodes = append(rebalance.nodes, instr.addNode)
		ejected := []string{instr.removeNode}
		return getTime(), rebalance.requestRebalance(rebalance.nodes, ejected)
	default:
		return -1, gorgon.ErrUnsupportedInstruction
	}
}

func (rebalance *rebalanceConfiguration) Name() string {
	return fmt.Sprintf("rebalance scenario - (%s)", rebalance.mode)
}

func (rebalance *rebalanceConfiguration) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (rebalance *rebalanceConfiguration) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

func (rebalance *rebalanceConfiguration) TearDown() error {
	return nil
}

func (rebalance *rebalanceConfiguration) requestRebalance(known, ejected []string) error {
	return rebalance.db.httpPost(rebalance.targetNode, "controller/rebalance", map[string]string{
		"knownNodes":   formatOtpNodes(known),
		"ejectedNodes": formatOtpNodes(ejected)})
}

func (rebalance *rebalanceConfiguration) requestAddNode(addNode string) error {
	return rebalance.db.httpPost(rebalance.targetNode, "controller/addNode", map[string]string{
		"hostname": addNode,
		"user":     *rebalance.db.config.User,
		"password": *rebalance.db.config.Pass,
		"services": "kv",
	})
}
