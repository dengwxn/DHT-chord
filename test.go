package main 

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

func putCmd(i int) {
	opCount[1]++
	data++
	err := c[i].PutCmd(strconv.Itoa(data), strconv.Itoa(data))
	if err != nil {
		opCount[0]++	
	}
}

func getCmd(i, j int) {
	opCount[1]++
	err := c[i].GetCmd(strconv.Itoa(j))
	if err != nil {
		opCount[0]++
	}
}

func testNaive() {
	dht.Magenta.Println(dht.TimeClock())
	dht.Magenta.Println(dht.TimeClock(), "Test Naive starts")
	opCount[0] = 0
	opCount[1] = 0
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
			putCmd(i)
		}
	}
	for i := 0; i < 5; i++ {
		for j := 1; j <= data; j++ {
			getCmd(i, j)
		}
	}
	dht.Green.Printf("Test Naive Complete: %.2f%% Correct\n", float64(opCount[1] - opCount[0]) / float64(opCount[1]) * 100)
}

func testAlpha() {
	dht.Magenta.Println(dht.TimeClock())
	dht.Magenta.Println(dht.TimeClock(), "Test Alpha starts")
	opCount[0] = 0
	opCount[1] = 0
	for i := 5; i < 10; i++ {
		c[i].PortCmd(strconv.Itoa(8000 + i))
		c[i].JoinCmd(c[i - 1].Node.IP)
		time.Sleep(time.Second)
	}
	for i := 0; i < 10; i++ {
		for j := 0; j < 5; j++ {
			putCmd(i)
		}
		for j := 0; j < 10; j++ {
			for k := 1; k <= data; k++ {
				getCmd(j, k)
			}
		}
	}
	dht.Green.Printf("Test Alpha Complete: %.2f%% Correct\n", float64(opCount[1] - opCount[0]) / float64(opCount[1]) * 100)
}

func testBeta() {
	dht.Magenta.Println(dht.TimeClock())
	dht.Magenta.Println(dht.TimeClock(), "Test Beta starts")
	opCount[0] = 0
	opCount[1] = 0
	for i := 0; i < 9; i++ {
		c[i].QuitCmd()
		time.Sleep(1 * time.Second)
		for j := i + 1; j < 10; j++ {
			for k := 1; k <= data; k++ {
				getCmd(j, k)
			}
		}
	}
	dht.Green.Printf("Test Beta Complete: %.2f%% Correct\n", float64(opCount[1] - opCount[0]) / float64(opCount[1]) * 100)
}

func testGamma() {
	dht.Magenta.Println(dht.TimeClock())
	dht.Magenta.Println(dht.TimeClock(), "Test Gamma starts")
	opCount[0] = 0
	opCount[1] = 0
	for i := 10; i < 20; i++ {
		c[i].PortCmd(strconv.Itoa(8000 + i))
		c[i].JoinCmd(c[i - 1].Node.IP)
		time.Sleep(1 * time.Second)
		for j := 0; j < 5; j++ {
			putCmd(i)
		}
		for j := 9; j <= i; j++ {
			for k := 1; k <= data; k++ {
				getCmd(j, k)
			}
		}
	}
	dht.Green.Printf("Test Gamma Complete: %.2f%% Correct\n", float64(opCount[1] - opCount[0]) / float64(opCount[1]) * 100)
}

func main() {
	dht.Blue.Println(dht.TimeDate(), "Welcome to the dht machine testing")
	dht.Blue.Println(dht.TimeDate(), "by Rivers Deng, Summer 2018")

	testNaive()
	testAlpha()
	testBeta()
	testGamma()

	os.Exit(0)
}
