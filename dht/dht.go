package dht 

import (
	"time"
	"net"
	"fmt"
	"net/rpc"
	"math/big"
	"errors"
)

// Node exported
type Node struct {
	IP, successor, predecessor string 
	data map[string]string
	id *big.Int 
}

// PutArgs exported 
type PutArgs struct {
	Key, Val string 
}

// NewNode exported
func NewNode(port string) *Node {
	addr := getLocalAddress()
	return &Node {
		IP: addr + ":" + port,
		data: make(map[string]string),
		id: HashString(addr + ":" + port),
	}
}

func (n *Node) stabilize() {
	x, err := rpcGetPredecessor(n.successor)
	if err == nil {
		if between(n.id, HashString(x), HashString(n.successor), false) {
			n.successor = x
		}
	} else {
		Cyan.Println(TimeClock(), err, "from", n.successor)
	}
	err = rpcNotify(n.successor, n.IP)
	if err != nil {
		Cyan.Println(TimeClock(), err)
	}
}

func (n *Node) checkPredecessor() {
	client := Dial(n.predecessor)
	if client == nil {
		n.predecessor = ""
	} else {
		var reply bool
		err := client.Call("Node.Ping", true, &reply)
		if err != nil {
			n.predecessor = ""
		}
	}
}

func (n *Node) stabilizePeriodically() {
	period := time.Tick(333 * time.Millisecond)
	for {
		<-period 
		n.stabilize()
	}
}

func (n *Node) checkPredecessorPeriodically() {
	period := time.Tick(333 * time.Millisecond)
	for {
		<-period 
		n.checkPredecessor()
	}
}

func (n *Node) create() {
	n.predecessor = ""
	n.successor = n.IP 
	go n.stabilizePeriodically()
	go n.checkPredecessorPeriodically()
}

func (n *Node) join(addr string) error {
	n.predecessor = ""
	successor, err := rpcFindSuccessor(addr, HashString(n.IP))
	if err != nil {
		return err
	}
	n.successor = successor
	return nil
}

// GetPredecessor exported
func (n *Node) GetPredecessor(none bool, addr *string) error {
	*addr = n.predecessor
	return nil
}

// Notify exported
func (n *Node) Notify(addr string, reply *bool) error {
	if n.predecessor == "" || between(HashString(n.predecessor), HashString(addr), n.id, false) {
		n.predecessor = addr
	}
	return nil
}

// FindSuccessor exported
func (n *Node) FindSuccessor(id *big.Int, reply *string) error {
	if n.id == id {
		*reply = n.IP
		return nil
	}
	if between(n.id, id, HashString(n.successor), true) {
		*reply = n.successor
		return nil
	}
	var err error 
	*reply, err = rpcFindSuccessor(n.successor, id) 
	return err
}

// Put exported
func (n *Node) Put(args PutArgs, reply *bool) error {
	n.data[args.Key] = args.Val 
	*reply = true 
	return nil
}

// Get exported
func (n *Node) Get(key string, reply *string) error {
	*reply = n.data[key]
	return nil
}

// Delete exported
func (n *Node) Delete(key string, reply *bool) error {
	if n.data[key] != "" {
		*reply = true
		delete(n.data, key)
	}
	return nil
}

// Ping exported
func (n *Node) Ping(none bool, reply *bool) error {
	return nil
}

func rpcGetPredecessor(addr string) (string, error) {
	if addr == "" {
		return "", errors.New("Get predecessor: lack valid address") 
	}
	client := Dial(addr)
	if client == nil {
		return "", errors.New("Get predecessor: client offline")
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.GetPredecessor", true, &reply) 
	if err != nil {
		return "", err 
	}
	if reply == "" {
		return "", errors.New("Get predecessor: predecessor not found") 
	}
	return reply, nil
}

func rpcNotify(addr, predecessor string) error {
	if addr == "" {
		return errors.New("Notify: lack valid address")
	}
	client := Dial(addr)
	if client == nil {
		return errors.New("Notify: client offline")
	}
	defer client.Close()
	var reply bool 
	return client.Call("Node.Notify", predecessor, &reply)
}

func rpcFindSuccessor(addr string, id *big.Int) (string, error) {
	if addr == "" {
		return "", errors.New("Find successor: lack valid address")
	}
	client := Dial(addr)
	if client == nil {
		return "", errors.New("Find successor: client offline")
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.FindSuccessor", id, &reply)
	return reply, err
}

// RPCServer exported
type RPCServer struct {
	node *Node 
	server *rpc.Server
	listener net.Listener
	Listening bool
}

// NewRPCServer exported
func NewRPCServer(n *Node) *RPCServer {
	return &RPCServer {
		node : n,
	}
}

// Listen exported
func (s *RPCServer) Listen() error {
	s.server = rpc.NewServer()
	s.server.Register(s.node)
	l, e := net.Listen("tcp", s.node.IP)
	if e != nil {
		return e
	}
	s.node.create()
	s.listener = l
	s.Listening = true
	go s.server.Accept(l)
	return nil
}

// Quit exported
func (s *RPCServer) Quit() {
	s.Listening = false
	s.listener.Close()
}

// Join exported
func (s *RPCServer) Join(addr string) error {
	err := s.Listen()
	if err != nil {
		return err
	}
	err = s.node.join(addr)
	if err != nil {
		return err
	}
	return nil
}

// Dump exported 
func (s *RPCServer) Dump() {
	fmt.Println(TimeClock(), "Address:", s.node.IP)
	fmt.Println(TimeClock(), "ID:", s.node.id)
	fmt.Println(TimeClock(), "Successor:", s.node.successor)
	fmt.Println(TimeClock(), "Predecessor:", s.node.predecessor)
	fmt.Println(TimeClock(), "Data:", s.node.data)
}
