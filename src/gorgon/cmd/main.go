package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/jrpc"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

const exitUsage = 2

// FileTime is the timestamp format used for filenames throughout the codebase
const FileTime = "2006-01-02-150405-0700"

// Entry point for Gorgon; delegates to either control node (run) or worker node (rpc) based on command
func Main(db gorgon.Database) int {
	var filter Filter
	opt := &gorgon.Options{
		WorkloadDuration: time.Minute,
		Concurrency:      6,
		RpcPort:          9090,
	}
	if ret := parseOptions(opt, &filter); ret != 0 {
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
	case "closerpc":
		return cmdCloseRpc(opt)
	}
	return usage()
}

func usage() int {
	fmt.Println("Usage:", os.Args[0], "[options] run|rpc|closerpc [args...]")
	return exitUsage
}

// Execute all matching workloads in sequence, stopping on first failure
func cmdRun(db gorgon.Database, opt *gorgon.Options, filter *Filter) int {
	configmap := &GorgonConfig{opt}
	err := SaveMap(configmap, "", opt.StoreSubdir)
	testMap := &TestRunResult{}
	if err != nil {
		return 1
	}
	workloads := db.Workloads()
	// Run each workload independently to isolate failures and verify consistency guarantees
	for _, workload := range workloads {
		runner := NewRunner(db, workload, opt)
		// Skip workloads that don't match user-specified filter pattern
		if !filter.Match(runner.Name()) {
			continue
		}
		if err := runner.SetUp(); err != nil {
			log.Error("Error in Runner.SetUp: %v", err)
			return 1
		}
		history, err := runner.Run()
		if err != nil {
			return 1
		}
		if err := runner.TearDown(); err != nil {
			log.Error("Error in Runner.TearDown: %v", err)
		}
		// Verify linearizability/ sequential consistency of observed operations
		if err := runner.Check(history, testMap, ""); err != nil {
			log.Error("Error in Runner.Check: %v", err)
			return 1
		}
		if err := SaveMap(testMap, runner.name, opt.StoreSubdir); err != nil {
			log.Error("Error in Runner.SaveMap: %v", err)
			return 1
		}
		testMap = &TestRunResult{}
	}
	return 0
}

// Worker nodes run this RPC server to receive instruction invocations from control node
func cmdRpc(opt *gorgon.Options) int {
	err := jrpc.Listen(fmt.Sprintf(":%v", opt.RpcPort), []byte(opt.RpcPassword))
	// If the listener closed due to an error
	if err != nil {
		log.Error("rpc: %v", err)
		return 1
	}
	// If the listener was closed gracefully
	log.Info("RPC server shutting down gracefully")
	return 0
}

func cmdCloseRpc(opt *gorgon.Options) int {
	for _, node := range opt.Nodes {
		client, err := jrpc.Dial(fmt.Sprintf("%s:%d", node, opt.RpcPort), []byte(opt.RpcPassword))
		if err != nil {
			log.Error("Failed to connect to %s for shutdown: %v", node, err)
			return 1
		}
		var emptyString string
		var reply string
		err = client.Call("CloseRpcServerRpc.Shutdown", &emptyString, &reply)
		if err != nil {
			log.Error("Failed to shutdown RPC server on %s: %v", node, err)
			return 1
		}
		if err = client.Close(); err != nil {
			log.Error("Failed to close RPC client for %s: %v", node, err)
			return 1
		}
	}
	return 0
}

func SaveMap(m any, name string, storeSubdir string) error {
	timestamp := time.Now().Format(FileTime)
	var prefix string
	var filename string
	switch m.(type) {
	case *GorgonConfig:
		prefix = "configmap"
		filename = "configmap-" + timestamp + ".json"
	case *TestRunResult:
		prefix = "testmap"
		filename = name + "-" + prefix + "-" + timestamp + ".json"
	default:
		return fmt.Errorf("unsupported map type: %T", m)
	}

	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to marshal %s: %w", prefix, err)
	}

	dir := path.Join("/root/store", storeSubdir)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	filePath := path.Join(dir, filename)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file %s: %w", filePath, err)
	}

	return nil
}

// Parse and validate command-line options; early validation prevents runtime errors
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
	flag.StringVar(&opt.StoreSubdir, "gorgon-store-subdir", "gorgon_maps", "Subdirectory under /root/store/ to save maps")

	flag.Parse()
	if flag.NArg() == 0 {
		return usage()
	}

	// Skip command name (args[0]) to keep only workload-specific arguments
	opt.Args = flag.Args()[1:]

	*filter = MakeFilter(matchPattern, excludePattern)
	if opt.Concurrency < 1 {
		fmt.Println("Invalid concurrency", opt.Concurrency)
		return exitUsage
	}
	// Valid TCP port range is 1-65535
	if opt.RpcPort <= 0 || opt.RpcPort >= (1<<16) {
		fmt.Println("Invalid port", opt.RpcPort)
		return exitUsage
	}
	// Minimum duration ensures sufficient interleaving for meaningful linearizability tests
	if opt.WorkloadDuration < 10*time.Second {
		fmt.Println("Minimum workload duration 10s")
		return exitUsage
	}

	for _, node := range strings.Split(nodes, ",") {
		node = strings.TrimSpace(node)
		// Skip empty entries caused by trailing commas or extra spaces
		if len(node) == 0 {
			continue
		}
		opt.Nodes = append(opt.Nodes, node)
	}
	// At least one node is required for any meaningful distributed system test
	if len(opt.Nodes) == 0 {
		fmt.Println("Minimum one node")
		return exitUsage
	}

	return 0
}
