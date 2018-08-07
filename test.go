package main 

import (
	"os"
	"time"
	"strconv"
	"DHT-chord/dht"
	"math/rand"
)

var (
	opCount [2]int
	nodeCount [2]int
	node [210]int
	dataCount [2]int
	data [3010]int
	c [210]dht.Chord
)
/*
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
	for i := 8; i >= 0; i-- {
		c[i].PortCmd(strconv.Itoa(8000 + i))
		c[i].JoinCmd(c[i + 1].Node.IP)
		time.Sleep(1 * time.Second)
		for j := i; j < 10; j++ {
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
		for j := 0; j < 50; j++ {
			putCmd(i)
		}
		for j := 0; j <= i; j++ {
			for k := 1; k <= data; k++ {
				getCmd(j, k)
			}
		}
	}
	dht.Green.Printf("Test Gamma Complete: %.2f%% Correct\n", float64(opCount[1] - opCount[0]) / float64(opCount[1]) * 100)
}
*/
func randomShuffleNode() {
	rand.Seed(time.Now().Unix())
	for i := nodeCount[0] + 1; i < nodeCount[1]; i++ {
		j := rand.Intn(i - nodeCount[0]) + 1
		node[i], node[i - j] = node[i - j], node[i]
	}
}

func putCmd(i int) {
	opCount[1]++
	err := c[i].PutCmd(strconv.Itoa(dataCount[1]), strconv.Itoa(dataCount[1]))
	data[dataCount[1]] = dataCount[1]
	dataCount[1]++
	if err != nil {
		opCount[0]++	
	}
}	

func randomShuffleData() {
	rand.Seed(time.Now().Unix())
	for i := dataCount[0] + 1; i < dataCount[1]; i++ {
		j := rand.Intn(i - dataCount[0]) + 1
		data[i], data[i - j] = data[i - j], data[i]
	}
}

func getCmd(i, j int) {
	opCount[1]++
	err := c[i].GetCmd(strconv.Itoa(j))
	if err != nil {
		opCount[0]++
	}
}

func deleteCmd(i, j int) {
	opCount[1]++
	err := c[i].DeleteCmd(strconv.Itoa(j))
	if err != nil {
		opCount[0]++
	}
}

func testMachine() {
	dht.Magenta.Println(dht.TimeClock())
	dht.Magenta.Println(dht.TimeClock(), "Test Machine starts")
	opCount[0], opCount[1] = 0, 0
	nodeCount[0], nodeCount[1] = 0, 1
	c[0].PortCmd(strconv.Itoa(8000))
	c[0].CreateCmd()
	time.Sleep(1 * time.Second)
	for rnd := 0; rnd < 10; rnd++ {
		randomShuffleNode()
		if (rnd & 1) == 0 {
			for i := 0; i < 15; i++ {
				j := nodeCount[1]
				c[j].PortCmd(strconv.Itoa(8000 + j))
				c[j].JoinCmd(c[j - 1].Node.IP)
				node[j] = j
				nodeCount[1]++
				time.Sleep(1 * time.Second)
			}
		} else {
			for i := 0; i < 5; i++ {
				j := nodeCount[0]
				c[node[j]].QuitCmd()
				nodeCount[0]++
				time.Sleep(1 * time.Second)
			}
		}
		opAverage := 300 / (nodeCount[1] - nodeCount[0]) + 1
		randomShuffleNode()
		for i, cnt := nodeCount[0], 0; i < nodeCount[1] && cnt < 300; i++ {
			for j := 0; j < opAverage && cnt < 300; j++ {
				putCmd(node[i])
				cnt++
			}
		}
		opAverage = 200 / (nodeCount[1] - nodeCount[0]) + 1
		randomShuffleNode()
		randomShuffleData()
		for i, k, cnt := nodeCount[0], dataCount[0], 0; i < nodeCount[1] && cnt < 200; i++ {
			for j := 0; j < opAverage && cnt < 200; j++ {
				getCmd(node[i], data[k])
				k++
				cnt++
			}
		}
		opAverage = 150 / (nodeCount[1] - nodeCount[0]) + 1
		randomShuffleNode()
		randomShuffleData()
		for i, k, cnt := nodeCount[0], dataCount[0], 0; i < nodeCount[1] && cnt < 150; i++ {
			for j := 0; j < opAverage && cnt < 150; j++ {
				deleteCmd(node[i], data[k])
				k++
				cnt++
				dataCount[0]++
			}
		}
	}
	dht.Green.Printf("Test Machine Complete: %.2f%% Correct\n", float64(opCount[1] - opCount[0]) / float64(opCount[1]) * 100)
}

func main() {
	dht.Blue.Println(dht.TimeDate(), "Welcome to the dht machine testing")
	dht.Blue.Println(dht.TimeDate(), "by Rivers Deng, Summer 2018")

	/*
	testNaive()
	testAlpha()
	testBeta()
	testGamma()
	*/
	testMachine()

	os.Exit(0)
}
