package main

import (
	"encoding/json"
	"log"
	logger "log"
	"sync"
	"time"

	"github.com/isaacoh92/maelstrom-challenge/maelstrom-utils/topology"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	kv   *KV
	top  map[string][]string
	once sync.Once
	wg   sync.WaitGroup
	mux  sync.RWMutex
	raft *Raft
)

const dbKey = "database"

func init() {
	wg = sync.WaitGroup{}
	mux = sync.RWMutex{}
}

func main() {
	n := maelstrom.NewNode()
	go broadcastDatabase(n)
	kv = InitKV()

	n.Handle("txn", func(msg maelstrom.Message) error {
		setup(n)
		type request struct {
			Type string  `json:"type"`
			Txn  [][]any `json:"txn"`
		}
		var body request

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		result, err := kv.Transact(body.Txn)
		if err != nil {
			return err
		}
		n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  result,
		})
		return nil
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		setup(n)
		type request struct {
			Type       string        `json:"type"`
			Database   map[int]int   `json:"database"`
			Timestamps map[int]int64 `json:"timestamps"`
		}

		var body request

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		kv.Merge(body.Database, body.Timestamps)
		return nil
	})

	n.Handle("append_entries", func(msg maelstrom.Message) error {
		return nil
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
}

func setup(node *maelstrom.Node) {
	once.Do(func() {
		var err error
		top, err = topology.GenerateTopology(node.NodeIDs(), len(node.NodeIDs())-1)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func broadcastDatabase(node *maelstrom.Node) {
	for range time.Tick(time.Millisecond * 1000) {
		if top == nil {
			continue
		}
		kv.mux.Lock()
		if kv.changed {
			for _, n := range top[node.ID()] {
				node.Send(n, map[string]any{
					"type":       "sync",
					"database":   kv.database,
					"timestamps": kv.timestamp,
				})
			}
			kv.changed = false
		}
		kv.mux.Unlock()
	}
}
