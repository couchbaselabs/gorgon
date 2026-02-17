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

// NewRunner is the factory function/constructor that creates a new Runner for the given database and workload.
func NewRunner(db gorgon.Database, workload gorgon.Workload, opts *gorgon.Options) *Runner {
	var sb strings.Builder
	sb.WriteString(db.Name())
	for _, gen := range workload.Generators {
		sb.WriteByte('~')
		sb.WriteString(gen.Name())
	}
	return &Runner{sb.String(), db, workload, opts, nil}
}

func (runner *Runner) Name() string {
	return runner.name
}

// this function prepares the Runner by setting up the database, creating clients, and setting up the workload generators.
func (runner *Runner) SetUp() error {
	log.Info("[%s] Database SetUp", runner.name)
	if err := runner.db.SetUp(); err != nil {
		return err
	}
	defer func() {
		if err := runner.db.TearDown(); err != nil {
			log.Error("[%s] Error in Database.TearDown: %v", runner.name, err)
		}
	}()
	log.Info("[%s] Creating clients", runner.name)
	concurrency := runner.options.Concurrency

	//creates a slice of clients with length equal to concurrency variable
	clients := make([]gorgon.Client, concurrency)
	defer func() {
		for _, client := range clients {
			if client != nil {
				client.Close()
			}
		}
	}()

	//takes the client configuration from the database and uses it to create and open clients
	config := runner.db.ClientConfig()
	for i := 0; i < concurrency; i++ {
		//create a new client using the database's NewClient method
		//this client can either be the direct cb client on the worker node or an instance of the proxy, which is the ClientOverRpc object
		client, err := runner.db.NewClient(i)
		if err != nil {
			log.Error("[%s] Error creating new client: %v", runner.name, err)
			return err
		}
		//open the client(direct client or the proxy) with the provided configuration
		err = client.Open(config)
		if err != nil {
			log.Error("[%s] Error opening client: %v", runner.name, err)
			return err
		}
		//store the opened client in the clients slice
		clients[i] = client
	}
	log.Info("[%s] Workload SetUp", runner.name)

	//initializes each generator in the workload by calling its SetUp method with the provided options
	for _, gen := range runner.workload.Generators {
		if err := gen.SetUp(runner.options); err != nil {
			log.Error("[%s] Error in Generator.SetUp: %v", runner.name, err)
			return err
		}
	}
	runner.clients = clients
	clients = nil
	return nil
}

func (runner *Runner) Run() ([]gorgon.Operation, error) {
	//creates a boolean flag and returns a pointer to it
	stopFlag := &atomic.Bool{}

	//defers the setting of the flag to True until the function exits
	defer stopFlag.Store(true)

	//creates a waitgroup to synchronize the completion of multiple goroutines
	wg := &sync.WaitGroup{}

	//creates a mutex to synchronize access to the instruction generators
	genMutex := &sync.Mutex{}

	//creates a linkedlist which is an object of the type OperationList to store operations, List has objects of the type Operation
	operationList := gorgon.NewOperationList()
	concurrency := runner.options.Concurrency
	log.Info("[%s] Starting workers", runner.name)
	deadline := time.Now().Add(runner.options.WorkloadDuration)

	//starts a number of worker goroutines equal to the concurrency level, one control worker and other workers for each client.
	for i := -1; i < concurrency; i++ {
		var client gorgon.Client
		if i >= 0 {
			client = runner.clients[i] //only clients that run normal instructions are added to the runner's client list.
		}

		//create a new worker object with the necessary fields initialized
		//a worker is responsible for executing instructions using a specific client
		//worker is created for each normal instruction as well as for nemesis instruction.
		//this worker has access to all the generators in the workload.
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
		//add the worker to the waitgroup and start its run method in a new goroutine
		wg.Add(1)
		go w.run() //there exists a worker for a nemesis instruction as well and they all run in parallel.
	}

	//wait for all workers to finish
	wg.Wait()
	log.Info("[%s] Workers finished", runner.name)
	return operationList.Extract(), nil
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

func (runner *Runner) Check(history []gorgon.Operation, dir string) (err error) {
	const fileTime = "2006-01-02-150405-0700"
	model := runner.workload.Model

	//create a non-deterministic model for the kv-store
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

	//convert it into a deterministic model
	dmodel := ndmodel.ToModel()

	//partition the history to speed up checking
	partitions := model.Partition(history)
	now := time.Now()
	linearizable := true
	for i, part := range partitions {

		//creeate a history for an individual partition to run the check operations on it.
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

		//Run verbose Check Op on it
		result, info := porcupine.CheckOperationsVerbose(dmodel, hist, 40*time.Second)
		level := log.INFO

		//If not linearizable, set the linearizable flag to false and set the level to WARNING
		//Also visualize the path and save to a location in the path formulated below
		if result != porcupine.Ok {
			linearizable = false
			level = log.WARNING
			filePath := path.Join(dir, EscapeFileName(fmt.Sprintf(
				"%s.%s.%d.html", now.Format(fileTime), runner.name, i)))
			visErr := porcupine.VisualizePath(dmodel, info, filePath)
			if visErr != nil && err == nil {
				err = visErr
			}
		}

		//Log Ok status of the partition if the partition is linearizable
		log.Log(level, "[%s] Checked partition %d - %s", runner.name, i, result)
	}

	//check sequential consistency if the partition is not linearizable
	if !linearizable {
		var hist [][]gorgon.Operation
		for _, op := range history {
			for len(hist) <= op.ClientId {
				hist = append(hist, nil)
			}
			hist[op.ClientId] = append(hist[op.ClientId], op)
		}

		//Run the check for sequential consistency
		result, info := CheckSeqnuentialConsistency(model, hist, time.Minute)
		level := log.INFO
		if result != checkers.Ok {
			level = log.WARNING
		}

		//File path to save the visualization of the sequential consistency check
		filePath := path.Join(dir, EscapeFileName(fmt.Sprintf(
			"%s.%s.html", now.Format(fileTime), runner.name)))

		//Visualize the path and save to a location in the path formulated above
		visErr := checkers.VisualizeSequentialPath(model, hist, info, filePath)
		if visErr != nil && err == nil {
			err = visErr
		}

		//Log the result of the sequential consistency check
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
	//signal that the worker is done when the function exits
	defer w.wg.Done()
	id := -1
	if w.client != nil {
		id = w.client.Id()
	}

	//main loop that continues until the deadline is reached or the stop flag is set
	for time.Until(w.deadline) >= 0 && !w.stopFlag.Load() {

		//loads the next instruction into the instr variable from the generators from the slice of generators that belong to this worker
		//The worker's getNext function internally uses the .Next() function of the generator to get the next instruction.
		instr, gen, err := w.getNext(id)
		if err != nil {
			return
		}

		//if no instruction is available, the worker sleeps for a short duration before checking again
		if instr == nil {
			time.Sleep(time.Millisecond)
			continue
		}

		//if the instruction is meant to be executed by the generator, itself (ForSelf), it invokes the instruction directly on the generator
		// these are meant for the nemesis operations that do not involve the client
		if instr.ForSelf() {
			if err := w.onCall(id, instr); err != nil {
				return
			}
			_, output := gen.Invoke(instr, w.operations.GetTime)
			if err := w.onReturn(id, instr, output); err != nil {
				return
			}
			continue
		}

		//if no client is assigned to the worker, it panics
		if w.client == nil {
			panic(errors.New("worker has no client assigned"))
		}

		//
		if err := w.onCall(id, instr); err != nil {
			return
		}

		//creates an Operation object to record the details of the instruction execution
		op := gorgon.Operation{ClientId: id, Input: instr, Call: w.operations.GetTime()}

		//invokes the instruction on the client and records the return time and output
		retTime, output := w.client.Invoke(instr, w.operations.GetTime)

		//notifies the generators about the completion of the instruction execution
		if err := w.onReturn(id, instr, output); err != nil {
			return
		}

		op.Return = retTime
		op.Output = output
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

// This function is used as a wrapper for the .Next() function of the generator.
// This wrapper function returns the generator object along with the instruction object(generator's Next)
// This function loops over all the generators in the workload and fetches the one whose's next function does not return a nil.
func (w *worker) getNext(id int) (gorgon.Instruction, gorgon.Generator, error) {
	w.genMutex.Lock()
	defer w.genMutex.Unlock()
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
