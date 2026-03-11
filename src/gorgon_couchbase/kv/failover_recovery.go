package kv

import (
	"errors"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/splitmix"
)

type FailoverInstruction struct {
	FailoverType string
}

type RecoveryInstruction struct {
	RecoveryType string
}

func (instruction *FailoverInstruction) String() string {
	return instruction.FailoverType + "Failover"
}

func (instruction *FailoverInstruction) ForSelf() bool {
	return true
}

func (instruction *RecoveryInstruction) String() string {
	return instruction.RecoveryType + "Recovery"
}

func (instruction *RecoveryInstruction) ForSelf() bool {
	return true
}

func NewFailoverAndRecoveryNemesis(db *database, failoverType, recoveryType string) gorgon.Generator {
	return &failoverAndRecovery{db: db, failoverType: failoverType, recoveryType: recoveryType}
}

type failoverAndRecovery struct {
	db           *database
	failoverType string
	recoveryType string
	node         string
	nodeIdx      int
	failedOver   bool
	recovered    bool
	failoverTime time.Time
	recoveryTime time.Time
}

func (nemesis *failoverAndRecovery) Name() string {
	return nemesis.failoverType + "FailoverAnd" + nemesis.recoveryType + "Recovery"
}

func (nemesis *failoverAndRecovery) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (nemesis *failoverAndRecovery) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

func (nemesis *failoverAndRecovery) Next(client int) (gorgon.Instruction, error) {
	// Nemesis instructions are issued only to the nemesis worker (client < 0)
	if client >= 0 {
		return nil, nil
	}
	if !nemesis.failedOver {
		if time.Until(nemesis.failoverTime) > 0 {
			return nil, nil
		}
		nemesis.failedOver = true
		return &FailoverInstruction{nemesis.failoverType}, nil
	}
	if !nemesis.recovered {
		if time.Until(nemesis.recoveryTime) > 0 {
			return nil, nil
		}
		nemesis.recovered = true
		return &RecoveryInstruction{nemesis.recoveryType}, nil
	}
	return nil, nil
}

func (nemesis *failoverAndRecovery) SetUp(opt *gorgon.Options) error {
	now := time.Now()
	nemesis.nodeIdx = splitmix.Rand.Intn(len(opt.Nodes))
	nemesis.node = opt.Nodes[nemesis.nodeIdx]
	nemesis.failoverTime = now.Add(opt.WorkloadDuration / 4)
	nemesis.recoveryTime = now.Add(opt.WorkloadDuration * 3 / 4)
	return nil
}

func (nemesis *failoverAndRecovery) TearDown() error {
	return nil
}

func (nemesis *failoverAndRecovery) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	switch instr := instruction.(type) {
	case *FailoverInstruction:
		return nemesis.invokeFailover(instr, getTime)
	case *RecoveryInstruction:
		return nemesis.invokeRecovery(instr, getTime)
	default:
		return -1, gorgon.ErrUnsupportedInstruction
	}
}

func (nemesis *failoverAndRecovery) invokeFailover(instr *FailoverInstruction, getTime func() int64) (int64, gorgon.Output) {
	switch instr.FailoverType {
	case "Hard":
		err := nemesis.db.httpPost(nemesis.node, "controller/failOver", map[string]string{
			"otpNode":     "ns_1@" + nemesis.node,
			"allowUnsafe": "false"})
		if err != nil {
			return getTime(), err
		}
	case "Graceful":
		err := nemesis.db.httpPost(nemesis.node, "controller/startGracefulFailover", map[string]string{
			"otpNode": "ns_1@" + nemesis.node})
		if err != nil {
			return getTime(), err
		}
	default:
		return getTime(), errors.New("invalid failover type: " + instr.FailoverType)
	}
	return getTime(), nil
}

func (nemesis *failoverAndRecovery) invokeRecovery(instr *RecoveryInstruction, getTime func() int64) (int64, gorgon.Output) {
	if instr.RecoveryType != "full" && instr.RecoveryType != "delta" {
		return getTime(), errors.New("invalid recovery type: " + instr.RecoveryType)
	}
	err := nemesis.recoveryRequest(instr.RecoveryType)
	if err != nil {
		return getTime(), err 
	}
	return getTime(), nil
}

func (nemesis *failoverAndRecovery) recoveryRequest(recovery_type string) error {
		err := nemesis.db.httpPost(nemesis.node, "/controller/setRecoveryType", map[string]string{
			"otpNode": "ns_1@" + nemesis.node,
			"recoveryType": recovery_type,
		})
		return err 
}