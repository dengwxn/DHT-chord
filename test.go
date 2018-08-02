package main 

import "C"

import (
	"os"
	"bufio"
	"fmt"
	"DHT-chord/dht"
	"strings"
	"errors"
)

func main() {
	fmt.Println(dht.TimeDate(), "Welcome to the dht machine")
	fmt.Println(dht.TimeDate(), "by Rivers Deng, Summer 2018")
	fmt.Println(dht.TimeClock())
	fmt.Println(dht.TimeClock(), "Type \"help\" to learn about available commands")
	for {
		input, err := getInput()
		if err != nil {
			fmt.Println(dht.TimeClock(), "Command Error")
			continue 
		}
		if _, ok := command[input[0]]; !ok {
			fmt.Println(dht.TimeClock(), "Command not found")
			continue 
		}
		err = command[input[0]](input[1:]...)
		if err != nil {
			fmt.Println(dht.TimeClock(), err)
		}
	}
}
