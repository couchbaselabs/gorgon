package rpcs

//this package encapsulates the logic for setting up Client Proxy if specified in the flag to relay requests to clients on the worker nodes,
//and Client objects on the worker nodes
//it also provides the logic for running the nemesis instructions on the worker nodes via RPC

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/rpc"
	"reflect"
	"strconv"
	"sync"

	"github.com/couchbaselabs/gorgon/src/gorgon"
	"github.com/couchbaselabs/gorgon/src/gorgon/jrpc"
	"github.com/couchbaselabs/gorgon/src/gorgon/log"
)

// registers the instruction type into the registry map
func RegisterInstruction(instrunction gorgon.Instruction) {
	rtype := reflect.TypeOf(instrunction)
	for rtype.Kind() == reflect.Pointer {
		rtype = rtype.Elem()
	}
	instructions[rtype.PkgPath()+"."+rtype.Name()] = rtype
}

func NewClientOverRpc(id int, node string, opt *gorgon.Options) gorgon.Client {
	return &clientOverRpc{id: id, node: node, opt: opt}
}

func NewClientRpc(db gorgon.Database) *ClientRpc {
	return &ClientRpc{db: db, clients: make(map[int]*lockableClient)}
}

// objects of this struct live on the server side (worker node) and handle rpc calls from the client side proxy(client in this case)
type ClientRpc struct {
	db      gorgon.Database
	clients map[int]*lockableClient //this map has a list of clients created on the worker nodes owned by the ClientRpc object
	mutex   sync.Mutex
}

// a wrapper on top of client to provide locking for concurrent access
type lockableClient struct {
	client gorgon.Client
	mutex  sync.Mutex
}

// this function creates a new client instance and adds it to the clients map that exists on the ClientRpc object on the server side
// this client that is created is the same client that is created on the control node if non-rpc mode of communication is used
func (rpc *ClientRpc) OpenClient(arg *RpcOpenClient, reply *string) error {

	//acquire the mutex on the map
	rpc.mutex.Lock()
	defer rpc.mutex.Unlock()

	//check for existing client
	if _, ok := rpc.clients[arg.Id]; ok {
		return errors.New("ClientRpc: client already exists")
	}
	//create a new client
	client, err := rpc.db.NewClient(arg.Id)
	if err != nil {
		return err
	}

	//open the client with the given config
	if err := client.Open(arg.Config); err != nil {
		return err
	}

	//add it to the clients map
	rpc.clients[arg.Id] = &lockableClient{client: client}
	*reply = "ok"
	log.Info("ClientRpc opened client %d with config %s", arg.Id, arg.Config)
	return nil
}

// deleted the specified client from the clients map and closes its connection
func (rpc *ClientRpc) CloseClient(id *int, reply *string) error {
	rpc.mutex.Lock()
	defer rpc.mutex.Unlock()
	if client, ok := rpc.clients[*id]; ok {
		client.mutex.Lock()
		defer client.mutex.Unlock()
		if err := client.client.Close(); err != nil {
			return err
		}
		delete(rpc.clients, *id)
		*reply = "ok"
		return nil
	}
	return errors.New("ClientRpc: client not found")
}

// this method handles the invocation of instructions on the client instance created on the server side
func (rpc *ClientRpc) Invoke(arg *RpcInvoke, reply *RpcInvokeReply) error {
	var instruction gorgon.Instruction
	if rtype, ok := instructions[arg.Instructon]; ok {
		instr := reflect.New(rtype).Interface()
		if err := json.Unmarshal([]byte(arg.Value), instr); err != nil {
			return fmt.Errorf("ClientRpc.Invoke: error unmarshalling instruction %s: %v", arg.Instructon, err)
		}
		instruction = instr.(gorgon.Instruction)
	} else {
		return fmt.Errorf("ClientRpc.Invoke: unknown instruction type %s", arg.Instructon)
	}

	//delegate the invocation to the invoke method which runs the instruction on the client instance
	output := rpc.invoke(arg.Id, instruction)

	//parse the output and populate the reply struct accordingly
	if output == nil {
		reply.Type = "nil"
		reply.Value = "null"
		return nil
	}
	switch v := output.(type) {
	case int:
		reply.Type = "int"
		reply.Value = strconv.Itoa(v)
	case string:
		reply.Type = "string"
		reply.Value = v
	case error:
		if gorgon.IsUnambiguousError(v) {
			reply.Type = "unambiguous_error"
		} else {
			reply.Type = "error"
		}
		reply.Value = v.Error()
	default:
		return fmt.Errorf("ClientRpc.Invoke: unexpected output type %T", output)
	}
	return nil
}

// this method looks up the client instance from the clients map and invokes the instruction on it
func (rpc *ClientRpc) invoke(id int, instruction gorgon.Instruction) gorgon.Output {
	var client *lockableClient
	rpc.mutex.Lock()
	if c, ok := rpc.clients[id]; ok {
		rpc.mutex.Unlock()
		client = c
	} else {
		rpc.mutex.Unlock()
		return errors.New("ClientRpc: client not found")
	}
	client.mutex.Lock()
	_, output := client.client.Invoke(instruction, func() int64 { return 0 })
	client.mutex.Unlock()
	return output
}

// this is the struct that implements the gorgon.Client interface over RPC. this is the proxy
// it implements a rpc client that has a rpc client from the rpc package
type clientOverRpc struct {
	id     int
	node   string //string representing the cluster
	opt    *gorgon.Options
	client *rpc.Client //the rpc client which is the connection to the remote server
}

func (c *clientOverRpc) Id() int {
	return c.id
}

// This method, with jrpc.Dial(), accepts the created client object(wiz a connection).
// it then calls the remote method "ClientRpc.OpenClient" on the server side, passing the client ID and config string as arguments.
// this ClientRpc.OpenClient method runs on the remote server(which is the worker node) and instructs it to create a new client instance with the given config on the server.
func (c *clientOverRpc) Open(config string) error {
	client, err := jrpc.Dial(fmt.Sprintf("%s:%d", c.node, c.opt.RpcPort), []byte(c.opt.RpcPassword))
	if err != nil {
		return err
	}
	//client object created on the server side to accept service requests sent by the rpc client on the rpc proxy
	err = client.Call("ClientRpc.OpenClient", &RpcOpenClient{Id: c.id, Config: config}, new(string))
	if err != nil {
		client.Close()
		return err
	}
	//store this client object in the clientOverRpc struct for remote procedure calls
	c.client = client
	return nil
}

// Proxy has a rpc client provided by the rpc package, which connects to the Rpc Client on the server
// that gets closed with the Close method
func (c *clientOverRpc) Close() error {
	err := c.client.Call("ClientRpc.CloseClient", &c.id, new(string))
	if err != nil {
		return err
	}
	return c.client.Close()
}

func (c *clientOverRpc) Invoke(instruction gorgon.Instruction, getTime func() int64) (retTime int64, output gorgon.Output) {
	//serialize the instruction into json to send it over rpc
	instructionJson, err := json.Marshal(instruction)
	if err != nil {
		log.Error("ClientOverRpc.Invoke: failed to marshal instruction %T", instruction)
		return getTime(), errors.New("ClientOverRpc: failed to marshal instruction")
	}

	//create an instance of the RPCReply struct, the response from the rpc server is in this struct
	var reply RpcInvokeReply

	//pointer unwrapping to get the actual type of the instruction
	rtype := reflect.TypeOf(instruction)
	for rtype.Kind() == reflect.Pointer {
		rtype = rtype.Elem()
	}

	//create an instance of the struct that holds the format for the invocation that has to be sent
	arg := RpcInvoke{Id: c.id, Instructon: rtype.PkgPath() + "." + rtype.Name(), Value: string(instructionJson)}

	//make the rpc call to the remote method "ClientRpc.Invoke" on the server side, this runs the invocation on the clientRpc object on the server
	err = c.client.Call("ClientRpc.Invoke", &arg, &reply)
	retTime = getTime()
	if err != nil {
		return retTime, err
	}

	//response type handling based on the type field in the reply struct
	switch reply.Type {
	case "nil":
		output = nil
	case "int":
		i, err := strconv.Atoi(reply.Value)
		if err != nil {
			output = fmt.Errorf("ClientOverRpc.Invoke: expected int, got %s", reply.Value)
		} else {
			output = i
		}
	case "string":
		output = reply.Value
	case "unambiguous_error":
		output = gorgon.WrapUnambiguousError(errors.New(reply.Value))
	case "error":
		output = errors.New(reply.Value)
	default:
		output = fmt.Errorf("ClientOverRpc.Invoke: unexpected reply type %s", reply.Type)
	}
	return
}

// this is the struct of the object that we pass to the function OpenClient on the RpcClient Object that lives on the client side
type RpcOpenClient struct {
	Id     int
	Config string
}

type RpcInvoke struct {
	Id         int
	Instructon string
	Value      string
}

type RpcInvokeReply struct {
	Type  string
	Value string
}

// this is the registry map which is a mapping instruction_name -> type of instruction
var instructions = make(map[string]reflect.Type)
