package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	topology map[string][]string
	counter  *Counter
	once     sync.Once
)

func main() {

	n := maelstrom.NewNode()

	go broadcastCounter(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		setup(n)
		type request struct {
			Type  string `json:"type"`
			Delta int    `json:"delta"`
		}

		var body request
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		counter.Add(body.Delta)

		return n.Reply(msg, map[string]any{
			"type": "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		setup(n)
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": counter.Count(),
		})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		setup(n)

		type request struct {
			Type     string         `json:"type"`
			Counters map[string]int `json:"counters"`
		}

		var body request
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		counter.Merge(body.Counters)
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func setup(node *maelstrom.Node) {
	once.Do(func() {
		counter = InitCounter(node.ID())
		topology = GenerateTopology(node.NodeIDs(), len(node.NodeIDs())-1)
	})
}

func broadcastCounter(node *maelstrom.Node) {
	for range time.Tick(time.Millisecond * 1000) {
		if counter == nil || topology == nil {
			continue
		}
		counter.Mux.Lock()
		if counter.Changed {
			for _, n := range topology[node.ID()] {
				node.Send(n, map[string]any{
					"type":     "broadcast",
					"counters": counter.Counters,
				})
			}
			counter.Changed = false
		}
		counter.Mux.Unlock()
	}
}
