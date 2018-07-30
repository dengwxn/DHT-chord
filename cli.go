package main 

import (
	"os"
	"bufio"
	"fmt"
	"DHT-chord/dht"
	"strings"
	"errors"
)

var command = map[string]func(args ...string) error {
	"help": helpCmd,
	"create": createCmd,
	"quit": quitCmd, 
	"join": joinCmd,
	"port": portCmd,
	"put": putCmd,
	"get": getCmd,
	"delete": deleteCmd,
	"dump": dumpCmd,
}

var (
	node *dht.Node 
	server *dht.Server 
	port = "3410"
)

func helpCmd(args ...string) error {
	fmt.Println("Available commands: help, quit, port, create, join, dump, put, get, delete.")
	return nil 
} 

func createCmd(args ...string) error {
	dispatch()
	server.Listen()
	fmt.Println("Creating new ring.")
	fmt.Printf("Listening at %v:%v\n", node.Address, node.Port)
	return nil
}

func quitCmd(args ...string) error {
	server.Quit();
	os.Exit(0)
	return nil
}

func joinCmd(args ...string) error {
	dispatch()
	server.Join(args[0])
	fmt.Printf("Join at %v\n", args[0])
	return nil
}

func portCmd(args ...string) error {
	if len(args) < 1 {
		fmt.Println("Current port is", node.Port)
	} else {
		if node != nil {
			return errors.New("Can't change port now")
		}
		port = args[0]
		fmt.Println("Port set to", port)
	}
	return nil
}

func putCmd(args ...string) error {
	addr := find(args[0])
	client := dht.Dial(addr)
	if client == nil {
		return errors.New("Put failed. Client offline")
	}
	defer client.Close()
	putArgs := dht.PutArgs{args[0], args[1]}
	var reply bool
	err := client.Call("Node.Put", putArgs, &reply) 
	if err != nil {
		return err 
	}
	fmt.Printf("(%v, %v) stored at %v\n", args[0], args[1], addr)
	return nil
}

func getCmd(args ...string) error {
	addr := find(args[0])
	client := dht.Dial(addr)
	if client == nil {
		return errors.New("Get failed. Client offline")
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.Get", args[0], &reply)
	if err != nil {
		return err 
	}
	fmt.Printf("At %v matched %v with %v\n", addr, args[0], args[1])
	return nil
}

func deleteCmd(args ...string) error {
	addr := find(args[0])
	client := dht.Dial(addr)
	if client == nil {
		return errors.New("Delete failed. Client offline")
	}
	defer client.Close()
	var reply string 
	err := client.Call("Node.Delete", args[0], &reply)
	if err != nil {
		return err
	}
	fmt.Printf("At %v deleted key %v\n", addr, args[0])
	return nil
}

func dumpCmd(args ...string) error {
	server.Dump()
	return nil
}

func find(key string) string {
	client := dht.Dial(node.Address + ":" + node.Port)
	if client == nil {
		panic(errors.New("Dial itself failed"))
	}
	defer client.Close() 
	var reply string 
	err := client.Call("Node.FindSuccessor", dht.HashString(key), &reply)
	if err != nil {
		fmt.Println(err)
	}
	return reply
}

func main() {
	prompt := "dht: "
	fmt.Println("Welcome to the dht machine! Type \"help\" to learn about available commands.")
	for {
		fmt.Printf(prompt)
		input, err := getInput()
		if err != nil {
			fmt.Println("Command Error!")
			continue 
		}
		if _, ok := command[input[0]]; !ok {
			fmt.Println("Command not found!")
			continue 
		}
		err = command[input[0]](input[1:]...)
		if err != nil {
			fmt.Println(err)
		}
	}
}

func dispatch() {
	node = dht.NewNode(port)
	server = dht.NewServer(node)
}

func getInput() ([]string, error) {
	input := bufio.NewReader(os.Stdin)
	line, err := input.ReadString('\n')
	if err != nil {
		return []string{}, err
	}
	return strings.Split(strings.TrimSpace(line), " "), nil
}