package dht

import (
	"errors"
	"math/big"
	"net"
	"net/rpc"
	"time"
)

// Node exported
type Node struct {
	successor [3]string
	IP, predecessor string
	data map[string]string
	id *big.Int
	listening bool
}

// PutArgs exported
type PutArgs struct {
	Key, Val string
}

// NewNode exported
func NewNode(port string) *Node {
	addr := getLocalAddress()
	return &Node{
		IP:   addr + ":" + port,
		data: make(map[string]string),
		id:   HashString(addr + ":" + port),
	}
}

func ping(addr string) (bool) {
	client := Dial(addr)
	if client == nil {
		return false
	} 
	defer client.Close()
	var reply bool
	err := client.Call("Node.Ping", true, &reply)
	if err != nil {
		return false
	}
	return true
}

func (n *Node) stabilize() {
	for _, suc := range n.successor {
		status := ping(suc)
		if !status {
			continue
		}
		n.successor[0] = suc
		x, err := rpcGetPredecessor(suc)
		if err == nil {
			if between(n.id, HashString(x), HashString(suc), false) {
				n.successor[0] = x
			}
		} else {
			Cyan.Println(TimeClock(), "stabilize:", err, "from", suc, "at", n.IP)
		}
		ok := true
		client := Dial(suc)
		defer client.Close()
		for i := 1; i < 3; i++ {
			err = client.Call("Node.PassSuccessor", i - 1, &n.successor[i])
			if err != nil {
				Cyan.Println(TimeClock(), "stabilize: pass successor", err, "from", suc)
				ok = false
			}
		}
		if !ok {
			continue
		}
		err = rpcNotify(n.successor[0], n.IP)
		if err != nil {
			Cyan.Println(TimeClock(), "stabilize:", err, "when notifying", n.successor[0], "at", n.IP)
		}
		break
	}
}

func (n *Node) checkPredecessor() {
	status := ping(n.predecessor)
	if !status {
		n.predecessor = ""
	}
}

func (n *Node) stabilizePeriodically() {
	period := time.Tick(333 * time.Millisecond)
	for {
		if !n.listening {
			break
		}
		<-period
		n.stabilize()
	}
}

func (n *Node) checkPredecessorPeriodically() {
	period := time.Tick(333 * time.Millisecond)
	for {
		if !n.listening {
			break
		}
		<-period
		n.checkPredecessor()
	}
}

func (n *Node) create() {
	n.predecessor = ""
	for i := 0; i < 3; i++ {
		n.successor[i] = n.IP
	}
	go n.stabilizePeriodically()
	go n.checkPredecessorPeriodically()
}

func (n *Node) join(addr string) error {
	n.predecessor = ""
	successor, err := rpcFindSuccessor(addr, HashString(n.IP))
	if err != nil {
		return err
	}
	n.successor[0] = successor
	err = rpcMigrateWhenJoining(successor, n.IP)
	return err
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
	for _, suc := range n.successor {
		status := ping(suc)
		if !status {
			continue
		}
		if between(n.id, id, HashString(suc), true) {
			*reply = suc
			return nil
		}
		var err error
		*reply, err = rpcFindSuccessor(suc, id)
		return err
	}
	return errors.New("find successor: successor not found")
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

// MigrateWhenJoining exported
func (n *Node) MigrateWhenJoining(addr string, reply *bool) error {
	client := Dial(addr)
	if client == nil {
		Green.Println(addr)
		return errors.New("Migrate when joining: client offline")
	}
	defer client.Close()
	for k, v := range n.data {
		if between(HashString(k), HashString(addr), HashString(n.IP), true) {
			n.Delete(k, reply)
			putArgs := PutArgs {
				Key: k,
				Val: v,
			}
			err := client.Call("Node.Put", putArgs, reply)
			if err != nil {
				return err
			}
			Magenta.Printf("%v Migrate (%v, %v) to %v\n", TimeClock(), k, v, addr)
		}
	}
	return nil
}

func (n *Node) migrateWhenQuiting(addr string) error {
	client := Dial(addr)
	if client == nil {
		return errors.New("Migrate when quiting: client offline")
	}
	defer client.Close()
	var reply bool
	for k, v := range n.data {
		n.Delete(k, &reply)
		putArgs := PutArgs {
			Key: k, 
			Val: v,
		}
		err := client.Call("Node.Put", putArgs, &reply)
		if err != nil {
			return err
		}
		Magenta.Printf("%v Migrate (%v, %v) to %v\n", TimeClock(), k, v, addr)
	}
	return nil
}

// PassSuccessor exported
func (n *Node) PassSuccessor(nth int, successor *string) error {
	*successor = n.successor[nth]
	return nil
}

func rpcGetPredecessor(addr string) (string, error) {
	if addr == "" {
		return "", errors.New("get predecessor: lack valid address")
	}
	client := Dial(addr)
	if client == nil {
		return "", errors.New("get predecessor: client offline")
	}
	defer client.Close()
	var reply string
	err := client.Call("Node.GetPredecessor", true, &reply)
	if err != nil {
		return "", err
	}
	if reply == "" {
		return "", errors.New("get predecessor: predecessor not found")
	}
	return reply, nil
}

func rpcNotify(addr, predecessor string) error {
	if addr == "" {
		return errors.New("notify: lack valid address")
	}
	client := Dial(addr)
	if client == nil {
		return errors.New("notify: client offline")
	}
	defer client.Close()
	var reply bool
	return client.Call("Node.Notify", predecessor, &reply)
}

func rpcFindSuccessor(addr string, id *big.Int) (string, error) {
	if addr == "" {
		return "", errors.New("find successor: lack valid address")
	}
	client := Dial(addr)
	if client == nil {
		return "", errors.New("find successor: client offline")
	}
	defer client.Close()
	var reply string
	err := client.Call("Node.FindSuccessor", id, &reply)
	return reply, err
}

func rpcMigrateWhenJoining(addr, predecessor string) error {
	if addr == "" {
		return errors.New("Migrate when joining: lack valid address")
	}
	client := Dial(addr)
	if client == nil {
		return errors.New("Migrate when joining: client offline")
	}
	defer client.Close()
	var reply bool
	err := client.Call("Node.MigrateWhenJoining", predecessor, &reply)
	return err
}

func (n *Node) find(key string) string {
	client := Dial(n.IP)
	if client == nil {
		panic(errors.New("Dial localhost failed"))
	}
	defer client.Close()
	var reply string
	err := client.Call("Node.FindSuccessor", HashString(key), &reply)
	if err != nil {
		Yellow.Println(TimeClock(), err)
	}
	return reply
}

// RPCServer exported
type RPCServer struct {
	node      *Node
	server    *rpc.Server
	listener  net.Listener
}

// NewRPCServer exported
func NewRPCServer(n *Node) *RPCServer {
	return &RPCServer{
		node: n,
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
	s.node.listening = true
	go s.server.Accept(l)
	return nil
}

// Quit exported
func (s *RPCServer) Quit() {
	s.node.listening = false
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
	Red.Println(TimeClock(), "Address:", s.node.IP)
	Red.Println(TimeClock(), "ID:", s.node.id)
	Red.Println(TimeClock(), "Successor:", s.node.successor)
	Red.Println(TimeClock(), "Predecessor:", s.node.predecessor)
	Red.Println(TimeClock(), "Data:", s.node.data)
}
