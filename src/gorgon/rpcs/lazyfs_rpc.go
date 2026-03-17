package rpcs

import (
	"fmt"
	"os/exec"

	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

type LazyFsInstruction struct {
	Fault string
	Node  string
}

func (instruction *LazyFsInstruction) String() string {
	return fmt.Sprintf("Fault called on the LazyFs is (%s)", instruction.Fault)
}

func (instruction *LazyFsInstruction) ForSelf() bool {
	return true
}

type LazyFsRpc struct{}

func (lazyfs_rpc *LazyFsRpc) InvokeLazyFsRpc(arg *LazyFsInstruction, reply *string) error {
	//Apply the function here on the worker node
	err := exec.Command("sh", "-c", arg.Fault).Run()
	log.Info("LazyFs fault injection %s returned %v", arg.Fault, err)
	if err != nil {
		return err
	}
	return nil
}
