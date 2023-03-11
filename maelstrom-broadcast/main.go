package main

import (
	"context"
	"encoding/json"
	"github.com/isaacoh92/maelstrom-challenge/maelstrom-utils/topology"
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
	once sync.Once
	top  map[string][]string
	m    message
	wg   sync.WaitGroup
)

func init() {
	m = message{
		messages:   []int{},
		messageSet: map[int]bool{},
		mux:        &sync.RWMutex{},
	}
	wg = sync.WaitGroup{}
}

func main() {

	n := maelstrom.NewNode()

	go broadcastAll(n)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		type request struct {
			Message int
		}
		var body request
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		m.mux.Lock()
		defer m.mux.Unlock()

		if !m.messageSet[body.Message] {
			m.messages = append(m.messages, body.Message)
			m.changed = true
		}

		return n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	})

	n.Handle("broadcast_all", func(msg maelstrom.Message) error {
		var body BroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		unseen := []int{}

		// merge incoming message set with node's current message set
		for _, message := range body.Messages {
			m.mux.RLock()
			_, seen := m.messageSet[message]
			m.mux.RUnlock()

			if !seen {
				unseen = append(unseen, message)
			}
		}

		if len(unseen) != 0 {
			m.mux.Lock()
			m.changed = true
			m.lastReceivedFrom = body.Src
			for _, u := range unseen {
				m.messageSet[u] = true
			}
			m.messages = append(m.messages, unseen...)
			m.mux.Unlock()
		}

		return nil
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		m.mux.RLock()
		body["messages"] = m.messages
		m.mux.RUnlock()
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// increasing "level" of internode connectivity = lower stable-latency, but higher messages per operation
		// decreasing level will have the opposite effect
		setup(n, 2)
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
	node.SyncRPC(context.Background(), neighbor, body)
}

func broadcastAll(node *maelstrom.Node) {
	for range time.Tick(time.Millisecond * 5) {
		m.mux.RLock()
		msgs := m.messages
		changed := m.changed
		lastReceived := m.lastReceivedFrom
		m.mux.RUnlock()
		if changed {
			for _, neighbor := range top[node.ID()] {
				if neighbor == lastReceived {
					continue
				}
				wg.Add(1)
				go broadcast(node, neighbor, BroadcastMessage{
					Type:     "broadcast_all",
					Messages: msgs,
					Src:      node.ID(),
				})
			}
		}

		m.mux.Lock()
		m.changed = false
		m.mux.Unlock()
	}
}

type BroadcastMessage struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
	Src      string `json:"src"`
}

func setup(node *maelstrom.Node, level int) {
	once.Do(func() {
		var err error
		top, err = topology.GenerateTopology(node.NodeIDs(), level)
		if err != nil {
			log.Fatal(err)
		}
	})
}
