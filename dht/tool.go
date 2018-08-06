package dht 

import (
    "time"
    "net"
    "net/rpc"
	"math/big"
	"crypto/sha1"
	"github.com/fatih/color"
)

var (
    // Blue exported
	Blue = color.New(color.FgBlue)
    // Magenta exported
    Magenta = color.New(color.FgMagenta)
    // Cyan exported
    Cyan = color.New(color.FgCyan)
    // Yellow exported
    Yellow = color.New(color.FgYellow)
    // Green exported
    Green = color.New(color.FgGreen)
    // Red exported
    Red = color.New(color.FgRed)
)

// HashString exported
func HashString(elt string) *big.Int {
    hasher := sha1.New()
    hasher.Write([]byte(elt))
    return new(big.Int).SetBytes(hasher.Sum(nil))
}

func getLocalAddress() string {
    var localaddress string
    ifaces, err := net.Interfaces()
    if err != nil {
        panic("init: failed to find network interfaces")
    }
    // find the first non-loopback interface with an IP address
    for _, elt := range ifaces {
        if elt.Flags & net.FlagLoopback == 0 && elt.Flags & net.FlagUp != 0 {
            addrs, err := elt.Addrs()
            if err != nil {
                panic("init: failed to get addresses for network interface")
            }
			for _, addr := range addrs {
                if ipnet, ok := addr.(*net.IPNet); ok {
                    if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
                        localaddress = ip4.String()
                        break
                    }
                }
            }
        }
    }
    if localaddress == "" {
        panic("init: failed to find non-loopback interface with valid address on this node")
    }
    return localaddress
}

func between(start, elt, end *big.Int, inclusive bool) bool {
    if end.Cmp(start) > 0 {
        return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
    }
    return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
}

// Dial exported
func Dial(addr string) *rpc.Client {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil
	}
	return client
}

// TimeDate exported
func TimeDate() string {
    return time.Now().Format("2006-01-02 15:04:05")
}

// TimeClock exported
func TimeClock() string {
    return time.Now().Format("15:04:05.000000")
}