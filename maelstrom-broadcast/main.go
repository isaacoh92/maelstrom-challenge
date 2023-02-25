package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type message struct {
	changed          bool
	messages         []int
	messageSet       map[int]bool
	lastReceivedFrom string
	mux              *sync.RWMutex
}

var (
	topology      map[string][]string
	globalMessage message
	wg            sync.WaitGroup
)

func init() {
	globalMessage = message{
		messages:   []int{},
		messageSet: map[int]bool{},
		mux:        &sync.RWMutex{},
	}
	wg = sync.WaitGroup{}
}

func main() {

	n := maelstrom.NewNode()

	go broadcast_all(n)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message, err := getMessage(body["message"])
		if err != nil {
			return err
		}

		globalMessage.mux.RLock()
		_, seen := globalMessage.messageSet[message]
		globalMessage.mux.RUnlock()

		if !seen {
			globalMessage.mux.Lock()
			globalMessage.messages = append(globalMessage.messages, message)
			globalMessage.changed = true
			globalMessage.mux.Unlock()
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("broadcast_all", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		messages := body["messages"].([]any)
		unseen := []int{}

		// merge incoming message set with node's current message set
		for _, msgs := range messages {
			message, err := getMessage(msgs)
			if err != nil {
				return err
			}

			globalMessage.mux.RLock()
			_, seen := globalMessage.messageSet[message]
			globalMessage.mux.RUnlock()

			if !seen {
				unseen = append(unseen, message)
			}
		}

		if len(unseen) != 0 {
			globalMessage.mux.Lock()
			globalMessage.changed = true
			globalMessage.lastReceivedFrom = body["src"].(string)
			for _, u := range unseen {
				globalMessage.messageSet[u] = true
			}
			globalMessage.messages = append(globalMessage.messages, unseen...)
			globalMessage.mux.Unlock()
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		globalMessage.mux.RLock()
		body["messages"] = globalMessage.messages
		globalMessage.mux.RUnlock()
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topMap, ok := body["topology"].(map[string]any)
		if !ok {
			return fmt.Errorf("incorrect type %T", topMap)
		}
		for node, neighbors := range topMap {
			neigh := neighbors.([]any)
			topology[node] = make([]string, len(neigh))

			for i, neighbor := range neigh {
				topology[node][i] = neighbor.(string)
			}
		}
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	wg.Wait()
}

func broadcast(node *maelstrom.Node, neighbor string, body any) {
	defer wg.Done()

	respCh := make(chan maelstrom.Message)
	if err := node.RPC(neighbor, body, func(m maelstrom.Message) error {
		respCh <- m
		return nil
	}); err != nil {
		wg.Add(1)
		go broadcast(node, neighbor, body)
		return
	}

	m := <-respCh
	if err := m.RPCError(); err != nil {
		wg.Add(1)
		go broadcast(node, neighbor, body)
	}
}

func broadcast_all(node *maelstrom.Node) {
	for range time.Tick(time.Millisecond * 5) {
		globalMessage.mux.RLock()
		msgs := globalMessage.messages
		changed := globalMessage.changed
		lastReceived := globalMessage.lastReceivedFrom
		globalMessage.mux.RUnlock()
		if changed {
			for _, neighbor := range topology[node.ID()] {
				if neighbor == lastReceived {
					continue
				}
				wg.Add(1)
				go broadcast(node, neighbor, map[string]any{"type": "broadcast_all", "messages": msgs, "src": node.ID()})
			}
		}

		globalMessage.mux.Lock()
		globalMessage.changed = false
		globalMessage.mux.Unlock()
	}
}

func getMessage(m any) (int, error) {
	var message int
	switch v := m.(type) {
	case int:
		message = v
	case float64:
		message = int(v)
	default:
		return message, fmt.Errorf("unsupported type %T", v)
	}
	return message, nil
}
