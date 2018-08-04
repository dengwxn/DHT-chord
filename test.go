package main 

import "C"

import (
	"os"
	"time"
	"strconv"
	"DHT-chord/dht"
)

var (
	opCount [2]int
	c [60]dht.Chord
	data int
)

func testNaive() {
	for i := 0; i < 5; i++ {
		c[i].PortCmd(strconv.Itoa(8000 + i))
		if i == 0 {
			c[i].CreateCmd()
		} else {
			c[i].JoinCmd(c[i - 1].Node.IP)
		}
		time.Sleep(time.Second)
	}
	for i := 0; i < 5; i++ {
		for k := 0; k < 5; k++ {
			opCount[1]++
			data++
			err := c[i].PutCmd(strconv.Itoa(data), strconv.Itoa(data))
			if err != nil {
				opCount[0]++	
			}
		}
	}
	for i := 0; i < 5; i++ {
		for j := 1; j <= data; j++ {
			opCount[1]++
			err := c[i].GetCmd(strconv.Itoa(j))
			if err != nil{
				opCount[0]++
			}
		}
	}
}

func testAlpha() {
	for i := 5; i < 10; i++ {
		c[i].PortCmd(strconv.Itoa(8000 + i))
		c[i].JoinCmd(c[i - 1].Node.IP)
		time.Sleep(time.Second)
	}
}

func main() {
	dht.Blue.Println(dht.TimeDate(), "Welcome to the dht machine testing")
	dht.Blue.Println(dht.TimeDate(), "by Rivers Deng, Summer 2018")
	dht.Magenta.Println(dht.TimeClock())
	dht.Magenta.Println(dht.TimeClock(), "Test starts")

	testNaive()
	testAlpha()

	dht.Green.Printf("Test finished: %.2f%% Correct\n", float64(opCount[1] - opCount[0]) / float64(opCount[1]) * 100)
	os.Exit(0)
}
