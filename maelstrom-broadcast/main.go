package main

import (
	"encoding/json"
	"github.com/isaacoh92/maelstrom-challenge/maelstrom-utils/topology"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"sync"
	"time"
)

type message struct {
	mux              *sync.RWMutex
	lastReceivedFrom string
	messageSet       map[int]bool
	messages         []int
	changed          bool
}

type broadcastMessage struct {
	Type     string `json:"type"`
	Src      string `json:"src"`
	Messages []int  `json:"messages"`
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
		var body broadcastMessage
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
		//     for example, when executing the test with `--node-count 25 --time-limit 20 --rate 100 --latency 100`
		//     a level of 2 yields latencies {0: 0, 0.5: 478, 0.95: 619, 0.99: 696, 1: 737} with 11.261326 msgs-per-op
		//     a level of 3 yields latencies {0: 0, 0.5: 313, 0.95: 407, 0.99: 417, 1: 488} with 21.394722 msgs-per-op
		setup(n, 2)
		return n.Reply(msg, map[string]any{
			"type": "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	wg.Wait()
}

func broadcast(node *maelstrom.Node, neighbor string, body any) {
	defer wg.Done()
	//node.SyncRPC(context.Background(), neighbor, body)
	if err := node.Send(neighbor, body); err != nil {
		log.Println("error broadcasting to node", node)
	}
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
				go broadcast(node, neighbor, broadcastMessage{
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

func setup(node *maelstrom.Node, level int) {
	once.Do(func() {
		var err error
		top, err = topology.GenerateTopology(node.NodeIDs(), level)
		if err != nil {
			log.Fatal(err)
		}
	})
}
