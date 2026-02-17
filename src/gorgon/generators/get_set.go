package generators

import (
	"fmt"
	"math/rand"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/splitmix"
)

type GetInstruction struct {
	Key string
}

type SetInstruction struct {
	Key   string
	Value int
}

func (op *GetInstruction) GetKey() string {
	return op.Key
}

func (op *SetInstruction) GetKey() string {
	return op.Key
}

func (op *GetInstruction) String() string {
	return fmt.Sprintf("Get(%q)", op.Key)
}

func (op *SetInstruction) String() string {
	return fmt.Sprintf("Set(%q, %d)", op.Key, op.Value)
}

func (op *GetInstruction) ForSelf() bool {
	return false
}

func (op *SetInstruction) ForSelf() bool {
	return false
}

func NewGetSetGenerator(keys []string) gorgon.Generator {
	return &getSetGenerator{keys: keys, rand: splitmix.NewRand()}
}

type getSetGenerator struct {
	keys []string
	rand *rand.Rand
	val  int
}

func (gen *getSetGenerator) Next(client int) (gorgon.Instruction, error) {
	//if the client is a nemesis client, return nil
	if client < 0 {
		return nil, nil
	}
	//if the client is a normal client, generate a random key and return a Get or Set instruction based on a random probability
	key := gen.keys[gen.rand.Intn(len(gen.keys))]
	if client&1 != 0 || gen.rand.Int63()&1 != 0 {
		return &GetInstruction{Key: key}, nil
	}
	gen.val++
	return &SetInstruction{Key: key, Value: gen.val}, nil
}

func (gen *getSetGenerator) Name() string {
	return "GetSet"
}

func (gen *getSetGenerator) SetUp(opt *gorgon.Options) error {
	return nil
}

func (gen *getSetGenerator) TearDown() error {
	return nil
}

func (gen *getSetGenerator) OnCall(client int, instruction gorgon.Instruction) error {
	return nil
}

func (gen *getSetGenerator) OnReturn(client int, instruction gorgon.Instruction, output gorgon.Output) error {
	return nil
}

func (gen *getSetGenerator) Invoke(instruction gorgon.Instruction, getTime func() int64) (int64, gorgon.Output) {
	return getTime(), gorgon.ErrUnsupportedInstruction
}
