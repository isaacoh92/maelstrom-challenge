package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/isaacoh92/maelstrom-challenge/maelstrom-utils/topology"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	kv   *KV
	top  map[string][]string
	once sync.Once
	raft *Raft
)

// func main() {
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
		log.Fatal(err)
	}
}

// var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
// var memprofile = flag.String("memprofile", "", "write memory profile to `file`")
var (
	cpu string = "cpu.prof"
	mem string = "mem.prof"
)

func main() {
	// flag.Parse()
	go broadcastMessage()
	// cpuprofile := &cpu
	// memprofile := &mem
	// if *cpuprofile != "" {
	// 	f, err := os.Create(*cpuprofile)
	// 	if err != nil {
	// 		log.Fatal("could not create CPU profile: ", err)
	// 	}
	// 	defer f.Close() // error handling omitted for example
	// 	if err := pprof.StartCPUProfile(f); err != nil {
	// 		log.Fatal("could not start CPU profile: ", err)
	// 	}
	// 	defer pprof.StopCPUProfile()
	// }

	// // ... rest of the program ...

	// if *memprofile != "" {
	// 	f, err := os.Create(*memprofile)
	// 	if err != nil {
	// 		log.Fatal("could not create memory profile: ", err)
	// 	}
	// 	defer f.Close() // error handling omitted for example
	// 	runtime.GC()    // get up-to-date statistics
	// 	if err := pprof.WriteHeapProfile(f); err != nil {
	// 		log.Fatal("could not write memory profile: ", err)
	// 	}
	// }
	n := maelstrom.NewNode()
	// kv := maelstrom.NewLinKV(n)
	// raft = InitRaft(n)

	n.Handle("txn", func(msg maelstrom.Message) error {
		setup(n)
		return raft.HandleClientRequest(msg)
	})

	n.Handle("request_vote", func(msg maelstrom.Message) error {
		setup(n)
		return raft.SubmitVote(msg)
	})

	// n.Handle("request_vote_response", func(msg maelstrom.Message) error {
	// 	setup(n)
	// 	return nil
	// })

	// n.Handle("append_entries_response", func(msg maelstrom.Message) error {
	// 	setup(n)
	// 	return nil
	// })

	n.Handle("append_entries", func(msg maelstrom.Message) error {
		setup(n)
		return raft.ReceiveAppendEntries(msg)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	// wg.Wait()
}

func setup(node *maelstrom.Node) {
	once.Do(func() {
		var err error
		top, err = topology.GenerateTopology(node.NodeIDs(), len(node.NodeIDs())-1)
		if err != nil {
			log.Fatal(err)
		}
		raft = InitRaft(node)
	})
}
func broadcastMessage() {
	for range time.Tick(time.Millisecond * 1000) {
		log.Println("hello world")
	}
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
