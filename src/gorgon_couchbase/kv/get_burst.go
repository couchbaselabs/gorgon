package kv

import (
	"sync"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/generators"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
	"github.com/couchbaselabs/gorgon/src/gorgon/nemeses"
)

// A key plus mutex type for controlling access to the key field
type safeKey struct {
	key   int
	mutex sync.Mutex
}

// This implements the generator interface
type GetBurst struct {
	keys []string
	key  safeKey
}

// Contructor function to init the generator
func NewGetBurst() *GetBurst {
	return &GetBurst{
		keys: []string{"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7"},
	}
}

func (gen *GetBurst) Name() string {
	return "GetBurst"
}


func (gen *GetBurst) SetUp(opt *gorgon.Options) error {
	gen.key.mutex.Lock()
	gen.key.key = -1
	gen.key.mutex.Unlock()
	return nil
}

// This function will return an instruction for all clients, but use a mutex.
func (gen *GetBurst) Next(client int) (gorgon.Instruction, error) {
	if client < 0 {
		return nil, nil
	}
	// Lock the variable for reading the field key
	gen.key.mutex.Lock()
	defer gen.key.mutex.Unlock()
	// Return when the field working is false (the case before nemesis has been applied)
	if gen.key.key < 0 {
		return nil, nil
	}
	// If a read has been generated for all the keys
	if gen.key.key >= len(gen.keys) {
		gen.key.key = -1
		return nil, nil
	}
	key := gen.keys[gen.key.key]
	gen.key.key++
	log.Info("client (%v) read key (%s) as part of the burst", client, key)
	return &generators.GetInstruction{Key: key}, nil
}

// This function is a no-op and is not supposed to run, as these are Set operations
// These are not supposed to be run by the generator itself but by the client
func (gen *GetBurst) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	return -1, gorgon.ErrUnsupportedInstruction
}

// The nemesis generator runs this Oncall
// After the nemesis is applied, the key field for the generator is set to 0, which begins burst.
func (gen *GetBurst) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

// On return to take care of ambiguous error
func (gen *GetBurst) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	// No op when the a non nemesis instruction runs this function
	if _, ok := instruction.(*nemeses.KillAndClearInstruction); ok {
		gen.key.mutex.Lock()
		gen.key.key = 0
		gen.key.mutex.Unlock()
	}
	return nil
}

func (gen *GetBurst) TearDown() error {
	return nil
}
