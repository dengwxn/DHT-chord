package main 

import (
	"os"
	"bufio"
	"fmt"
	"DHT-chord/dht"
	"strings"
)

var command = map[string]func(args ...string) error {
	"help": helpCmd,
	"create": createCmd,
	"quit": quitCmd, 
	/*
	"port": portCmd,
	"join": joinCmd,
	"put": putCmd,
	"get": getCmd,
	"delete": deleteCmd,
	"dump": dumpCmd,
	*/
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