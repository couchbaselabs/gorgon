package main

import (
	"flag"
	"log"
	"net/rpc"
	"os"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon/cmd"
	"github.com/couchbaselabs/gorgon/src/gorgon/generators"
	"github.com/couchbaselabs/gorgon/src/gorgon/rpcs"
	"github.com/couchbaselabs/gorgon/src/gorgon_couchbase/kv"
)

// main is the entry point for the Couchbase Gorgon binary.
// it sets up the Couchbase database by creating a new instance of kv.Database
// it registers the necessary RPCs and instructions for both the control node and the worker nodes, and then calls cmd.Main to handle command-line arguments and execute the appropriate commands.
func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	db := kv.NewDatabase(kv.DatabaseConfig{
		User:          flag.String("user", "Administrator", "Couchbase username"),
		Pass:          flag.String("pass", "password", "Couchbase password"),
		Port:          flag.Int("port", 11210, "Couchbase port"),
		Replicas:      flag.Int("replicas", 1, "Number of Couchbase replicas (0-3)"),
		Durability:    flag.String("durability", "none", "Couchbase durability level"),
		Timeout:       flag.Duration("timeout", 5*time.Second, "Couchbase operation timeout"),
		ClientOverRpc: flag.Bool("client-over-rpc", false, "Use RPC for client operations"),
	})

	// this is run on the worker nodes to register methods of reciever objects for RPC calls.
	// it registers ClientRpc, IpTablesRpc, and KillRpc structs to handle various remote procedure calls.
	// rpc.Register registers these objects to the rpc server running on the process
	rpc.Register(rpcs.NewClientRpc(db))
	rpc.Register(&rpcs.IpTablesRpc{})
	rpc.Register(&rpcs.KillRpc{})
	rpc.Register(&rpcs.LazyFsRpc{})

	//registers the GetInstruction and SetInstruction types into the registry map which is a mapping like map[string]reflect.Type{}
	rpcs.RegisterInstruction(&generators.GetInstruction{})
	rpcs.RegisterInstruction(&generators.SetInstruction{})

	// Call the main command handler with the Couchbase database instance.
	code := cmd.Main(db)
	if code != 0 {
		os.Exit(code)
	}
}
