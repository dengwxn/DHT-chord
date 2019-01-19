package dht

import (
	"time"
	"errors"
)

// Chord exported
type Chord struct {
	Node *Node 
	server *rpcServer 
	port string
}

// HelpCmd exported
func (c *Chord) HelpCmd(args ...string) error {
	Magenta.Println(TimeClock(), "Available commands: help, quit, port, create, join, dump, put, get, delete")
	return nil 
} 

// CreateCmd exported
func (c *Chord) CreateCmd(args ...string) error {
	if c.Node != nil {
		return errors.New("Create: have created or joined")
	}
	c.dispatch()
	err := c.server.listen()
	if err != nil {
		panic(err)
	}
	Magenta.Println(TimeClock(), "Creating new ring")
	Magenta.Printf("%v Listening at %v\n", TimeClock(), c.Node.IP)
	return nil
}

// QuitCmd exported
func (c *Chord) QuitCmd(args ...string) error {
	defer Magenta.Printf("%v Quit normally from %v\n", TimeClock(), c.Node.IP)
	if c.server != nil {
		defer c.server.quit()
		for _, suc := range c.Node.successor {
			status := ping(suc)
			if !status {
				continue
			}
			err := c.Node.migrateWhenQuiting(suc)
			if err != nil {
				continue
			}
			break
		}
	}
	c.Node = nil
	c.server = nil
	return nil
}

// ForceQuitCmd exported
func (c *Chord) ForceQuitCmd(args ...string) error {
	// debug backup function
	// in real circumstance, these won't be executed
	c.Node.bufferWriter.Flush()
	defer Yellow.Printf("%v Force Quit from %v\n", TimeClock(), c.Node.IP)
	if c.server != nil {
		defer c.server.forceQuit()
	}
	c.Node = nil
	c.server = nil
	return nil
}

func (c *Chord) recover() {
	time.Sleep(1 * time.Second)
	for key, value := range c.Node.backup {
		err := c.PutCmd(key, value)
		if err != nil {
			Magenta.Printf("%v Fail to recover data (%v, %v)\n", TimeClock(), key, value)
		} else {
			Green.Printf("%v Recover (%v, %v) from %v\n", TimeClock(), key, value, c.Node.IP)
		}
		delete(c.Node.backup, key)
	}
}

// JoinCmd exported
func (c *Chord) JoinCmd(args ...string) error {
	if c.Node != nil {
		return errors.New("Join: have created or joined")
	}
	if len(args) < 1 {
		return errors.New("Join: lack valid address")
	}
	c.dispatch()
	err := c.server.join(args[0])
	if err != nil {
		panic(err)
	}
	go c.recover()
	Magenta.Printf("%v Join at %v\n", TimeClock(), args[0])
	return nil
}

// PortCmd exported
func (c *Chord) PortCmd(args ...string) error {
	if len(args) < 1 {
		Magenta.Printf("%v Current port is %v\n", TimeClock(), c.port)
	} else {
		if c.Node != nil {
			return errors.New("Can't change port now")
		}
		c.port = args[0]
		Magenta.Printf("%v Port set to %v\n", TimeClock(), c.port)
	}
	return nil
}

// PutCmd exported
func (c *Chord) PutCmd(args ...string) error {
	addr := c.Node.find(args[0])
	client := dial(addr)
	if client == nil {
		return errors.New("Put: client offline")
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
	Magenta.Printf("%v Put (%v, %v) at %v\n", TimeClock(), args[0], args[1], addr)
	return nil
}

// GetCmd exported
func (c *Chord) GetCmd(args ...string) error {
	addr := c.Node.find(args[0])
	client := dial(addr)
	if client == nil {
		return errors.New("Get: client offline")
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.Get", args[0], &reply)
	if err != nil {
		return err 
	}
	if reply != "" {
		Magenta.Printf("%v Get (%v, %v) at %v\n", TimeClock(), args[0], reply, addr)
	} else {
		Yellow.Printf("%v Fail to get %v at %v\n", TimeClock(), args[0], addr)
		return errors.New("Get: match not found")
	}
	return nil
}

// DeleteCmd exported
func (c *Chord) DeleteCmd(args ...string) error {
	addr := c.Node.find(args[0])
	client := dial(addr)
	if client == nil {
		return errors.New("Delete: client offline")
	}
	defer client.Close()
	var reply bool
	err := client.Call("Node.Delete", args[0], &reply)
	if err != nil {
		return err
	}
	if reply == true {
		Magenta.Printf("%v Deleted key %v at %v\n", TimeClock(), args[0], addr)
	} else {
		Yellow.Printf("%v Fail to delete key %v at %v\n", TimeClock(), args[0], addr)
		return errors.New("Delete: match not found")
	}
	return nil
}

// DumpCmd exported
func (c *Chord) DumpCmd(args ...string) error {
	c.server.dump()
	return nil
}

func (c *Chord) dispatch() error {
	c.Node = newNode(c.port)
	c.Node.getBackup()
	c.Node.startBackup()
	c.server = newrpcServer(c.Node)
	return nil
}