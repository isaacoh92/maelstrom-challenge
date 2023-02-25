package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	numNodes    = 3
	numSeconds  = 30
	rps         = 1000 // requests per second
	includeSign = false
)

var epoch int64 = time.Now().UnixMilli()

// var clock int64 = time.Now().UnixMilli()
var lastSeen int64 = time.Now().UnixMilli()
var sequence = 0
var mux *sync.RWMutex = &sync.RWMutex{}

// func getIDSize() int {
// 	// a-bbbb-cccc-dddd
// 	// where a = sign, b = timestamp since epoch, c = nodeId, d = sequence number
// 	return getTimestampSize() + getNodeSize()
// }

// func getTimestampSize() int {
// 	s := int(math.Ceil(math.Log2(float64(numSeconds * 1000))))
// 	fmt.Println(s)
// 	return s
// }

// func getNodeSize() int {
// 	return int(math.Ceil(math.Log2(float64(numNodes))))
// }

// func getSequenceSize() int {
// 	// it is possible that one node handles all incoming requests
// 	return int(math.Ceil(math.Log2(float64(rps * numSeconds))))
// }

// func genSequence() string {
// 	mux.Lock()
// 	s := fmt.Sprintf("%b", sequence)
// 	sequence++
// 	mux.Unlock()
// 	return s
// }

// func genTimestamp() string {
// 	// return fmt.Sprintf("%b", time.Now().UnixMilli()-epoch)
// 	return fmt.Sprintf("%b", clock)
// }

//	func genNode(id int) string {
//		return fmt.Sprintf("%b", id)
//	}
func genID(id int) string {
	// return fmt.Sprintf("%s-%s-%s", genTimestamp(), genNode(id), genSequence())
	mux.Lock()
	ms := time.Now().UnixMilli() - epoch
	ret := fmt.Sprintf("%s-%s-%s", fmt.Sprintf("%b", ms), fmt.Sprintf("%b", id), fmt.Sprintf("%b", sequence))
	lastSeen = ms
	sequence++
	mux.Unlock()
	return ret
}

func main() {
	go resetSequence()
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		id, err := strconv.Atoi(strings.TrimPrefix(n.ID(), "n"))
		if err != nil {
			return err
		}
		body["type"] = "generate_ok"
		body["id"] = genID(id)
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func resetSequence() {
	for range time.Tick(time.Millisecond * 1) {
		mux.Lock()
		if time.Now().UnixMilli()-epoch != lastSeen {
			sequence = 0
		}
		mux.Unlock()
	}
}
