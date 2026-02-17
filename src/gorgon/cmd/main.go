package cmd

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/jrpc"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

const exitUsage = 2

// Main is the main command handler for Gorgon.
//Parses cli options, sets database options, and executes the specified command (run or rpc).
func Main(db gorgon.Database) int {
	var filter Filter
	opt := &gorgon.Options{
		WorkloadDuration: time.Minute,
		Concurrency:      6,
		RpcPort:          9090,
	}
	ret := parseOptions(opt, &filter)
	if ret != 0 {
		return ret
	}
	if err := db.SetOptions(opt); err != nil {
		log.Error("Error in Database.SetOptions: %v", err)
		return 1
	}
	switch flag.Arg(0) {
	case "run":
		return cmdRun(db, opt, &filter)
	case "rpc":
		return cmdRpc(opt)
	}
	return usage()
}

func usage() int {
	fmt.Println("Usage:", os.Args[0], "[options] run|rpc [args...]")
	return exitUsage
}

//this function runs when the flag provided is run, it runs in the control node and sets up runners for each workload
func cmdRun(db gorgon.Database, opt *gorgon.Options, filter *Filter) int {
	workloads := db.Workloads()
	for _, workload := range workloads {
		runner := NewRunner(db, workload, opt)
		if !filter.Match(runner.Name()) {
			continue
		}
		if err := runner.SetUp(); err != nil {
			log.Error("Error in Runner.SetUp: %v", err)
			return 1
		}

		//history is a slice of Operations returned by the runner object's run method
		history, err := runner.Run()
		if err != nil {
			return 1
		}
		if err := runner.TearDown(); err != nil {
			log.Error("Error in Runner.TearDown: %v", err)
		}
		if err := runner.Check(history, ""); err != nil {
			log.Error("Error in Runner.Check: %v", err)
			return 1
		}
	}
	return 0
}

//this function runs when the flag provided is rpc, it sets up the rpc server for communication between control and worker nodes on the specified port on the worker nodes 
//it calls the Listen function from the jrpc package to start listening for incoming rpc requests
func cmdRpc(opt *gorgon.Options) int {
	err := jrpc.Listen(fmt.Sprintf(":%v", opt.RpcPort), []byte(opt.RpcPassword))
	if err != nil {
		log.Error("rpc: %v", err)
		return 1
	}
	return 0
}

func parseOptions(opt *gorgon.Options, filter *Filter) int {
	matchPattern := "*"
	excludePattern := ""
	nodes := "localhost"

	flag.StringVar(&matchPattern, "gorgon-match", matchPattern, "Wildcard pattern for scenarios to run")
	flag.StringVar(&excludePattern, "gorgon-exclude", excludePattern, "Wildcard pattern for scenarios to exclude")
	flag.StringVar(&nodes, "gorgon-nodes", nodes, "Comma-separated list of nodes")
	flag.DurationVar(&opt.WorkloadDuration, "gorgon-workload-duration", opt.WorkloadDuration, "Intended workload/nemesis duration")
	flag.IntVar(&opt.Concurrency, "gorgon-concurrency", opt.Concurrency, "Number of clients to use")
	flag.BoolVar(&opt.ContinueAmbiguousClient, "gorgon-continue-ambiguous-client", false,
		"Don't stop a worker when its client returns an error that is not unambiguous")
	flag.IntVar(&opt.RpcPort, "gorgon-rpc-port", opt.RpcPort, "RPC port to connect")
	flag.StringVar(&opt.RpcPassword, "gorgon-rpc-password", opt.RpcPassword, "RPC password")

	flag.Parse()
	if flag.NArg() == 0 {
		return usage()
	}

	opt.Args = flag.Args()[1:]

	*filter = MakeFilter(matchPattern, excludePattern)
	if opt.Concurrency < 1 {
		fmt.Println("Invalid concurrency", opt.Concurrency)
		return exitUsage
	}
	if opt.RpcPort <= 0 || opt.RpcPort >= (1<<16) {
		fmt.Println("Invalid port", opt.RpcPort)
		return exitUsage
	}
	if opt.WorkloadDuration < 10*time.Second {
		fmt.Println("Minimum workload duration 10s")
		return exitUsage
	}

	for _, node := range strings.Split(nodes, ",") {
		node = strings.TrimSpace(node)
		if len(node) == 0 {
			continue
		}
		opt.Nodes = append(opt.Nodes, node)
	}
	if len(opt.Nodes) == 0 {
		fmt.Println("Minimum one node")
		return exitUsage
	}

	return 0
}
