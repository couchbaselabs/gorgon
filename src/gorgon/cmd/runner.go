package cmd

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/checkers"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

type Runner struct {
	name     string
	db       gorgon.Database
	workload gorgon.Workload
	options  *gorgon.Options
	clients  []gorgon.Client
}

type GorgonConfig struct {
	Options *gorgon.Options
}

type TestRunResult struct {
	History         []gorgon.Operation
	ConsistencyType string
}

// NewRunner creates a new Runner instance with a unique descriptive name
func NewRunner(db gorgon.Database, workload gorgon.Workload, opts *gorgon.Options) *Runner {
	var sb strings.Builder
	// Start with database name as the primary identifier for this test run
	sb.WriteString(db.Name())
	// Append all generator names to the identifier; useful for distinguishing test variants
	for _, gen := range workload.Generators {
		sb.WriteByte('~')
		sb.WriteString(gen.Name())
	}
	// Construct runner with computed name
	return &Runner{sb.String(), db, workload, opts, nil}
}

func (runner *Runner) Name() string {
	return runner.name
}

// This function prepares the Runner by setting up the database,
// creating clients, and setting up the workload generators.
func (runner *Runner) SetUp() error {
	log.Info("[%s] Database SetUp", runner.name)
	// Initialize the database before any client connections can be established
	if err := runner.db.SetUp(); err != nil {
		return err
	}
	log.Info("[%s] Creating clients", runner.name)
	concurrency := runner.options.Concurrency

	// Pre-allocate slice for expected number of clients to avoid reallocations
	clients := make([]gorgon.Client, concurrency)
	defer func() {
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
	}()
	// Teardown the db if in case setup happens only partially
	defer func() {
		if clients != nil {
			if err := runner.db.TearDown(); err != nil {
				log.Error("[%s] Error in Database.TearDown: %v", runner.name, err)
			}
		}
	}()

	// Retrieve connection config once to reuse for all clients
	config := runner.db.ClientConfig()
	for i := 0; i < concurrency; i++ {
		// Database decides whether to create direct client or RPC proxy based on test configuration
		client, err := runner.db.NewClient(i)
		if err != nil {
			log.Error("[%s] Error creating new client: %v", runner.name, err)
			return err
		}
		// Establish actual connection to database; this may fail if server is unavailable
		err = client.Open(config)
		if err != nil {
			log.Error("[%s] Error opening client: %v", runner.name, err)
			return err
		}
		clients[i] = client
	}
	log.Info("[%s] Workload SetUp", runner.name)
	for _, gen := range runner.workload.Generators {
		if err := gen.SetUp(runner.options); err != nil {
			log.Error("[%s] Error in Generator.SetUp: %v", runner.name, err)
			return err
		}
	}
	// Transfer ownership to runner struct; clearing local variable prevents defer from closing valid clients
	runner.clients = clients
	clients = nil
	return nil
}

func (runner *Runner) Run() ([]gorgon.Operation, error) {
	// Shared atomic flag allows graceful shutdown of all workers on any error
	stopFlag := &atomic.Bool{}
	// Ensure workers stop even if Run panics or returns early
	defer stopFlag.Store(true)
	// Synchronize worker goroutines to ensure complete operation collection
	wg := &sync.WaitGroup{}
	// Protect shared generators from concurrent access across workers
	genMutex := &sync.Mutex{}
	// Thread-safe collection for recording all operations during the test
	operationList := gorgon.NewOperationList()
	concurrency := runner.options.Concurrency
	log.Info("[%s] Starting workers", runner.name)
	deadline := time.Now().Add(runner.options.WorkloadDuration)

	// Create one worker per client for database operations, plus one extra for nemesis (system fault injection)
	for i := -1; i < concurrency; i++ {
		var client gorgon.Client
		if i >= 0 {
			// Nemesis has no client
			client = runner.clients[i]
		}
		w := &worker{
			stopFlag:      stopFlag,
			wg:            wg,
			genMutex:      genMutex,
			generators:    runner.workload.Generators,
			client:        client,
			operations:    operationList,
			deadline:      deadline,
			stopAmbiguous: !runner.options.ContinueAmbiguousClient,
			name:          runner.name,
		}
		// Start workers concurrently to maximize parallel execution and test throughput
		wg.Add(1)
		go w.run()
	}

	// Block until all workers finish
	wg.Wait()
	log.Info("[%s] Workers finished", runner.name)
	history := operationList.Extract()
	return history, nil
}

func (runner *Runner) TearDown() (retErr error) {
	for _, gen := range runner.workload.Generators {
		if err := gen.TearDown(); err != nil {
			log.Error("[%s] Error in Generator.TearDown: %v", runner.name, err)
			if retErr == nil {
				retErr = err
			}
		}
	}
	for i, client := range runner.clients {
		if client != nil {
			err := client.Close()
			if err != nil {
				log.Error("[%s] Client %d error: %v", runner.name, i, err)
				if retErr == nil {
					retErr = err
				}
			}
		}
	}
	return
}

func (runner *Runner) Check(history []gorgon.Operation, testMap *TestRunResult, dir string) (err error) {
	testMap.History = history
	model := runner.workload.Model

	// Wrap workload model to match porcupine's expected interface
	ndmodel := porcupine.NondeterministicModel{
		Init: model.Init,
		Step: func(state, input, output interface{}) []interface{} {
			return model.Step(state, input.(gorgon.Instruction), output)
		},
		Equal: model.Equal,
		DescribeOperation: func(input, output interface{}) string {
			return model.DescribeOperation(input.(gorgon.Instruction), output)
		},
		DescribeState: model.DescribeState,
	}

	// Porcupine requires deterministic models for linearizability verification
	dmodel := ndmodel.ToModel()

	// Partition operations to reduce state explosion in model checking
	partitions := model.Partition(history)
	now := time.Now()
	linearizable := true
	for i, part := range partitions {
		// Convert to porcupine's operation format for linearizability checking
		hist := make([]porcupine.Operation, len(part))
		for j, op := range part {
			hist[j] = porcupine.Operation{
				ClientId: op.ClientId,
				Input:    op.Input,
				Call:     op.Call,
				Output:   op.Output,
				Return:   op.Return,
			}
		}

		// Verbose mode to produce visualization info
		result, info := porcupine.CheckOperationsVerbose(dmodel, hist, 40*time.Second)
		level := log.INFO
		// Save visualization for failed checks to help developers debug the violation
		if result != porcupine.Ok {
			linearizable = false
			level = log.WARNING
			filePath := path.Join(dir, EscapeFileName(fmt.Sprintf(
				"%s.%s.%d.html", now.Format(FileTime), runner.name, i)))
			visErr := porcupine.VisualizePath(dmodel, info, filePath)
			if visErr != nil && err == nil {
				err = visErr
			}
		}
		log.Log(level, "[%s] Checked partition %d - %s", runner.name, i, result)
	}

	testMap.ConsistencyType = "linearizable"

	// Sequential consistency is a weaker guarantee; check when linearizability fails
	if !linearizable {
		testMap.ConsistencyType = "sequential"

		var hist [][]gorgon.Operation
		for _, op := range history {
			for len(hist) <= op.ClientId {
				hist = append(hist, nil)
			}
			hist[op.ClientId] = append(hist[op.ClientId], op)
		}

		// Run the check for sequential consistency
		result, info := CheckSeqnuentialConsistency(model, hist, time.Minute)
		level := log.INFO
		if result != checkers.Ok {
			testMap.ConsistencyType = "none"
			level = log.WARNING
		}
		filePath := path.Join(dir, EscapeFileName(fmt.Sprintf(
			"%s.%s.html", now.Format(FileTime), runner.name)))
		visErr := checkers.VisualizeSequentialPath(model, hist, info, filePath)
		if visErr != nil && err == nil {
			err = visErr
		}

		log.Log(level, "[%s] Checked sequential consistency - %s", runner.name, result)
	}
	return
}

type worker struct {
	stopFlag      *atomic.Bool
	wg            *sync.WaitGroup
	genMutex      *sync.Mutex
	generators    []gorgon.Generator
	client        gorgon.Client
	operations    *gorgon.OperationList
	deadline      time.Time
	stopAmbiguous bool
	name          string
}

func (w *worker) run() {
	// Signal WaitGroup that this worker goroutine is complete when it exits
	defer w.wg.Done()
	id := -1
	if w.client != nil {
		id = w.client.Id()
	}

	// Keep processing instructions until timeout or explicit stop signal
	for time.Until(w.deadline) >= 0 && !w.stopFlag.Load() {
		instr, gen, err := w.getNext(id)
		if err != nil {
			return
		}
		// Generators may not always have ready work; avoid busy waiting
		if instr == nil {
			time.Sleep(time.Millisecond)
			continue
		}
		// Nemesis operations (e.g., network partition) don't need a client
		if instr.ForSelf() {
			// Notify generators
			if err := w.onCall(id, instr); err != nil {
				return
			}
			// Execute the instruction directly without a client
			_, output := gen.Invoke(instr, w.operations.GetTime)
			// Notify generators
			if err := w.onReturn(id, instr, output); err != nil {
				return
			}
			continue
		}

		// This should never happen; indicates a setup bug if we reach here
		if w.client == nil {
			panic(errors.New("worker has no client assigned"))
		}

		// Notify generators before client invocation (for traceability/state updates)
		if err := w.onCall(id, instr); err != nil {
			return
		}

		op := gorgon.Operation{ClientId: id, Input: instr, Call: w.operations.GetTime()}

		// Execute the instruction on the actual database client
		retTime, output := w.client.Invoke(instr, w.operations.GetTime)

		// Notify generators after client invocation (to complete any pending state changes)
		if err := w.onReturn(id, instr, output); err != nil {
			return
		}

		op.Return = retTime
		op.Output = output

		// An operation that failed ambiguously may complete at an unknown time.
		// Issuing further operation on the same client would make them concurrent,
		// breaking the assumption of client = thread.
		if err, ok := output.(error); ok && !gorgon.IsUnambiguousError(err) {
			op.Return = -1
			if w.stopAmbiguous {
				log.Warning("[%s] Client %d returned ambiguous error: %T %v", w.name, id, err, err)
				w.operations.Append(op)
				return
			}
		}
		w.operations.Append(op)
	}
}

// Wrapper over generator's Next function to fetch the next non nil instruction
func (w *worker) getNext(id int) (gorgon.Instruction, gorgon.Generator, error) {
	w.genMutex.Lock()
	defer w.genMutex.Unlock()
	// Iterate backwards over the mutex protected list of generators to prioritize nemesis and post-nemesis
	for i := len(w.generators) - 1; i >= 0; i-- {
		gen := w.generators[i]
		instr, err := gen.Next(id)
		if err != nil {
			log.Error("[%s] Generator %q failed: %v", w.name, gen.Name(), err)
			return nil, nil, err
		}
		if instr != nil {
			return instr, gen, nil
		}
	}
	// No generator had a ready instruction for this worker
	return nil, nil, nil
}

func (w *worker) onCall(client int, instruction gorgon.Instruction) error {
	w.genMutex.Lock()
	defer w.genMutex.Unlock()
	for _, gen := range w.generators {
		if err := gen.OnCall(client, instruction); err != nil {
			log.Error("[%s] Generator %q OnCall failed: %v", w.name, gen.Name(), err)
			return err
		}
	}
	return nil
}

func (w *worker) onReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	w.genMutex.Lock()
	defer w.genMutex.Unlock()
	for _, gen := range w.generators {
		if err := gen.OnReturn(client, instruction, output); err != nil {
			log.Error("[%s] Generator %q OnReturn failed: %v", w.name, gen.Name(), err)
			return err
		}
	}
	return nil
}
