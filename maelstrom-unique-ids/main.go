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

// A simpler implementation of Twitter's Snowflake ID generator
// ID generated is simply a binary representation (as a string)
// Could have been done using just a sequence number and node name
// (such as the one implemented in maelstrom-counter/id_gen.go),
// but Snowflake is beautifully crafted :)

var epoch = time.Now().UnixMilli()
var lastSeen = time.Now().UnixMilli()
var sequence = 0
var mux = &sync.RWMutex{}

func genID(id int) string {
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
