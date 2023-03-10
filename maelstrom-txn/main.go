package main

import (
	"context"
	"encoding/json"
	"errors"
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
	n := maelstrom.NewNode()
	// kv := maelstrom.NewLinKV(n)
	// raft = InitRaft(n)

	n.Handle("txn", func(msg maelstrom.Message) error {
		setup(n)
		return HandleClientRequest(msg, raft)
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

/*
Request:
Received {c4 n0 {"txn":[["r",9,null],["r",8,null],["w",9,1]],"type":"txn","msg_id":1}}

Response:
Sent {"src":"n0","dest":"c7","body":{"in_reply_to":1,"txn":[["w",9,3]],"type":"txn_ok"}}
*/
func HandleClientRequest(msg maelstrom.Message, r *Raft) error {
	var req map[string]any
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}
	//r.Lock(0)
	//defer r.Unlock(0)

	switch {
	case r.IsLeader(true):
		r.Lock(0)
		defer r.Unlock(0)
		r.logs.Append(&Log{
			Term:      r.term,
			Operation: req["txn"].([]any),
		})

		txs := []any{}
		for _, txn := range req["txn"].([]any) {
			res, err := r.stateMachine.Apply(txn.([]any))
			if err != nil {
				return err
			}
			txs = append(txs, res)
		}

		return r.node.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  txs,
		})
	case r.Leader(true) != "":
		responseChannel := make(chan maelstrom.Message)
		// Async RPC request
		if err := r.node.RPC(r.Leader(true), req, func(m maelstrom.Message) error {
			responseChannel <- m
			return nil
		}); err != nil {
			r.Logf("client request RPC error")
			return err
		}
		ctx, close := context.WithTimeout(context.Background(), time.Millisecond*1000)
		defer close()
		select {
		case res := <-responseChannel:
			var resp map[string]any
			if err := json.Unmarshal(res.Body, &resp); err != nil {
				return err
			}
			return r.node.Reply(msg, resp)
		case <-ctx.Done():
			return errors.New("client request unsuccessful, timed out")
		}
	default:
		r.Logf("client request temp unavail")
		return errors.New("temporarily unavailable")
	}
	//if !r.IsLeader() {
	//	return errors.New("temporarily unavailable")
	//}

	//r.logs.Append(&Log{
	//	Term:      r.term,
	//	Operation: req["txn"].([]any),
	//})
	//
	//txs := []any{}
	//for _, txn := range req["txn"].([]any) {
	//	res, err := r.state.Apply(txn.([]any))
	//	if err != nil {
	//		return err
	//	}
	//	txs = append(txs, res)
	//}
	//
	//return r.node.Reply(msg, map[string]any{
	//	"type": "txn_ok",
	//	"txn":  txs,
	//})
}
