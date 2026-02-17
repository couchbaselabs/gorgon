package rpcs

import (
	"os/exec"

	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

// this struct is the struct object that is not particularly useful but is necessary as the rpc.Register() needs an object
// whose method to register that can be called on the server side.
type IpTablesRpc struct{}

// This is a method on the RPC object on the worker that implements the iptables command
// This method is called by the RPC client on the worker side after the generator issues the iptables command on the client side
func (*IpTablesRpc) IpTables(arg *[]string, reply *string) error {
	err := exec.Command("iptables", (*arg)...).Run()
	log.Info("IpTables(%v) returned %v", *arg, err)
	if err != nil {
		return err
	}
	*reply = "ok"
	return nil
}
