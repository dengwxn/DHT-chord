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

// NewNode exported
func NewNode(port string) *Node {
	addr := getLocalAddress()
	return &Node {
		Address: addr,
		Port: port, 
		data: make(map[string]string),
		id: hashString(addr + ":" + port),
	}
}

func (n *Node) stabilize() {
	x, err := rpcGetPredecessor(n.successor)
	if err == nil {
		if between(n.id, hashString(x), hashString(n.successor), false) {
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

// GetPredecessor exported
func (n *Node) GetPredecessor(_, addr *string) error {
	*addr = n.predecessor
	return nil
}

func dial(addr string) *rpc.Client {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return nil
	}
	return client
}

func rpcGetPredecessor(addr string) (string, error) {
	if addr == "" {
		return "", errors.New("Find predecessor: lack valid address") 
	}
	client := dial(addr)
	if client == nil {
		return "", errors.New("Client is offline")
	}
	defer client.Close() 
	
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