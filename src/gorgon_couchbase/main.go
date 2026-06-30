package main

import (
	"flag"
	"log"
	"net/rpc"
	"os"

	"github.com/couchbaselabs/gorgon/src/gorgon/cmd"
	"github.com/couchbaselabs/gorgon/src/gorgon/generators"
	"github.com/couchbaselabs/gorgon/src/gorgon/rpcs"
	"github.com/couchbaselabs/gorgon/src/gorgon_couchbase/kv"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	db := kv.NewDatabase(kv.DatabaseConfig{
		User:           flag.String("user", "Administrator", "Couchbase username"),
		Pass:           flag.String("pass", "password", "Couchbase password"),
		Port:           flag.Int("port", 11210, "Couchbase port"),
		Replicas:       flag.Int("replicas", 1, "Number of Couchbase replicas (0-3)"),
		Durability:     flag.String("durability", "none", "Couchbase durability level"),
		ClientOverRpc:  flag.Bool("client-over-rpc", false, "Use RPC for client operations"),
		StorageEngine:  flag.String("storage-engine", "couchstore", "Couchbase storage engine (couchstore/magma)"),
		Vbuckets:       flag.Int("vbuckets", 1024, "Number of vbuckets for the bucket"),
		EvictionPolicy: flag.String("eviction-policy", "fullEviction", "Bucket eviction policy (fullEviction/valueOnly)"),
	})

	// Worker nodes must register RPC handlers before any calls can be served
	rpc.Register(&rpcs.CloseRpcServerRpc{})
	rpc.Register(rpcs.NewClientRpc(db))
	rpc.Register(&rpcs.IpTablesRpc{})
	rpc.Register(&rpcs.KillRpc{})

	// Register instruction types so they can be transmitted over RPC
	rpcs.RegisterInstruction(&generators.GetInstruction{})
	rpcs.RegisterInstruction(&generators.SetInstruction{})

	code := cmd.Main(db)
	if code != 0 {
		os.Exit(code)
	}
}
