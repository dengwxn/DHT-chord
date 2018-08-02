package dht

import "C"

import (
	"os"
	"fmt"
	"errors"
)

// Chord exported
type Chord struct {
	node *Node 
	server *Server 
	port string
}

// HelpCmd exported
func (c *Chord) HelpCmd(args ...string) error {
	fmt.Println(TimeClock(), "Available commands: help, quit, port, create, join, dump, put, get, delete")
	return nil 
} 

// CreateCmd exported
func (c *Chord) CreateCmd(args ...string) error {
	if c.node != nil {
		return errors.New("Create failed: have created or joined")
	}
	c.dispatch()
	err := c.server.Listen()
	if err != nil {
		panic(err)
	}
	fmt.Println(TimeClock(), "Creating new ring")
	fmt.Printf("%v Listening at %v\n", TimeClock(), c.node.IP)
	return nil
}

// QuitCmd exported
func (c *Chord) QuitCmd(args ...string) error {
	// migrate data
	if c.server != nil {
		c.server.Quit();
	}
	os.Exit(0)
	return nil
}

// JoinCmd exported
func (c *Chord) JoinCmd(args ...string) error {
	if c.node != nil {
		return errors.New("Join failed: have created or joined")
	}
	if len(args) < 1 {
		return errors.New("Join failed: lack valid address")
	}
	c.dispatch()
	err := c.server.Join(args[0])
	if err != nil {
		panic(err)
	}
	fmt.Printf("%v Join at %v\n", TimeClock(), args[0])
	return nil
}

// PortCmd exported
func (c *Chord) PortCmd(args ...string) error {
	if len(args) < 1 {
		fmt.Printf("%v Current port is %v\n", TimeClock(), c.port)
	} else {
		if c.node != nil {
			return errors.New("Can't change port now")
		}
		c.port = args[0]
		fmt.Printf("%v Port set to %v\n", TimeClock(), c.port)
	}
	return nil
}

// PutCmd exported
func (c *Chord) PutCmd(args ...string) error {
	addr := c.find(args[0])
	client := Dial(addr)
	if client == nil {
		return errors.New("Put failed. Client offline")
	}
	defer client.Close()
	putArgs := PutArgs { 
		Key: args[0],
		Val: args[1],
	}
	var reply bool
	err := client.Call("Node.Put", putArgs, &reply) 
	if err != nil {
		return err 
	}
	fmt.Printf("%v Put (%v, %v) at %v\n", TimeClock(), args[0], args[1], addr)
	return nil
}

// GetCmd exported
func (c *Chord) GetCmd(args ...string) error {
	addr := c.find(args[0])
	client := Dial(addr)
	if client == nil {
		return errors.New("Get failed. Client offline")
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.Get", args[0], &reply)
	if err != nil {
		return err 
	}
	if reply != "" {
		fmt.Printf("%v Get %v with %v at %v\n", TimeClock(), args[0], reply, addr)
	} else {
		fmt.Printf("%v Fail to get %v at %v\n", TimeClock(), args[0], addr)
	}
	return nil
}

// DeleteCmd exported
func (c *Chord) DeleteCmd(args ...string) error {
	addr := c.find(args[0])
	client := Dial(addr)
	if client == nil {
		return errors.New("Delete failed. Client offline")
	}
	defer client.Close()
	var reply bool
	err := client.Call("Node.Delete", args[0], &reply)
	if err != nil {
		return err
	}
	if reply == true {
		fmt.Printf("%v Deleted key %v at %v\n", TimeClock(), args[0], addr)
	} else {
		fmt.Printf("%v Fail to delete key %v at %v\n", TimeClock(), args[0], addr)
	}
	return nil
}

func (c *Chord) dumpCmd(args ...string) error {
	c.server.Dump()
	return nil
}

func (c *Chord) find(key string) string {
	client := Dial(c.node.IP)
	if client == nil {
		panic(errors.New("Dial localhost failed"))
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.FindSuccessor", HashString(key), &reply)
	if err != nil {
		fmt.Println(TimeClock(), err)
	}
	fmt.Println(key, HashString(key), reply)
	return reply
}

func (c *Chord) dispatch() {
	c.node = NewNode(c.port)
	c.server = NewServer(c.node)
}