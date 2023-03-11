package main

import (
	"encoding/json"
	"fmt"
	rft "github.com/isaacoh92/maelstrom-challenge/maelstrom-utils/raft"
	"github.com/isaacoh92/maelstrom-challenge/maelstrom-utils/topology"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

// Implemented a totally available Key value store with a Read-committed consistency model
// Also implemented a store with strict-serializable consistency (which cannot achieve total availability)
// Consistency model can be changed in main()

var (
	kv   *KV
	top  map[string][]string
	once sync.Once
	raft *rft.Raft
)

func ReadCommitted() {
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
		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  result,
		})
	})

	n.Handle("sync", func(msg maelstrom.Message) error {
		setup(n)
		type request struct {
			Timestamps map[int]int64 `json:"timestamps"`
			Database   map[int]int   `json:"database"`
			Type       string        `json:"type"`
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
		log.Fatal(err)
	}
}

func StrictSerializable() {
	n := maelstrom.NewNode()

	n.Handle("txn", func(msg maelstrom.Message) error {
		setupRaft(n)
		return raft.HandleClientRequest(msg)
	})

	n.Handle("request_vote", func(msg maelstrom.Message) error {
		setupRaft(n)
		return raft.SubmitVote(msg)
	})

	n.Handle("append_entries", func(msg maelstrom.Message) error {
		setupRaft(n)
		return raft.ReceiveAppendEntries(msg)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	ReadCommitted()
	//StrictSerializable()
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

func setupRaft(node *maelstrom.Node) {
	once.Do(func() {
		raft = rft.InitRaft(node)
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
				if err := node.Send(n, map[string]any{
					"type":       "sync",
					"database":   kv.database,
					"timestamps": kv.timestamp,
				}); err != nil {
					fmt.Printf("broadcast to peer %s failed with: %v", n, err)
				}
			}
			kv.changed = false
		}
		kv.mux.Unlock()
	}
}
