package dht 

import (
	"fmt"
	"time"
	"net"
	"net/http"
	"net/rpc"
	"math/big"
	"errors"
)

// Node exported
type Node struct {
	Address, Port, successor, predecessor string 
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
		Address: addr,
		Port: port, 
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
		err = rpcNotify(n.successor, n.Address + ":" + n.Port)
		if err != nil {
			fmt.Println(err)
		}
	} else {
		fmt.Println(err)
	}
}

func (n *Node) checkPredecessor() {
	client := Dial(n.predecessor)
	if client == nil {
		n.predecessor = ""
	}
}

func (n *Node) stabilizePeriodically() {
	period := time.Tick(1 * time.Second)
	for {
		<-period 
		n.stabilize()
	}
}

func (n *Node) checkPredecessorPeriodically() {
	period := time.Tick(1 * time.Second)
	for {
		<-period 
		n.checkPredecessor()
	}
}

func (n *Node) create() {
	n.predecessor = ""
	n.successor = n.Address + ":" + n.Port 
	go n.stabilizePeriodically()
	go n.checkPredecessorPeriodically()
}

func (n *Node) join(addr string) {
	n.predecessor = ""
	successor, err := rpcFindSuccessor(addr, HashString(n.Address + ":" + n.Port))
	if err != nil {
		fmt.Println("Join failed:", err)
		return
	}
	n.successor = successor
}

// GetPredecessor exported
func (n *Node) GetPredecessor(_, addr *string) error {
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

func rpcGetPredecessor(addr string) (string, error) {
	if addr == "" {
		return "", errors.New("Find predecessor: lack valid address") 
	}
	client := Dial(addr)
	if client == nil {
		return "", errors.New("Find predecessor: client offline")
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.GetPredecessor", true, &reply) 
	if err != nil {
		return "", err 
	}
	if reply == "" {
		return "", errors.New("Find predecessor: Predecessor not found") 
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

// Server exported
type Server struct {
	node *Node 
	listener net.Listener
}

// NewServer exported
func NewServer(n *Node) *Server {
	return &Server {
		node : n,
	}
}

// Listen exported
func (s *Server) Listen() {
	rpc.Register(s.node)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":" + s.node.Port)
	if e != nil {
		panic(e)
	}
	s.node.create()
	s.listener = l
	go http.Serve(l, nil)
}

// Quit exported
func (s *Server) Quit() {
	s.listener.Close()
}

// Join exported
func (s *Server) Join(addr string) {
	s.Listen()
	s.node.join(addr)
}

// Dump exported 
func (s *Server) Dump() {
	fmt.Println("Address:", s.node.Address + ":" + s.node.Port)
	fmt.Println("ID:", s.node.id)
	fmt.Println("Successor:", s.node.successor)
	fmt.Println("Predecessor:", s.node.predecessor)
	fmt.Println("Data:", s.node.data)
}