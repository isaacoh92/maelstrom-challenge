package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/slices"
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
	topology = map[string][]string{}
	wg = sync.WaitGroup{}
}

// copies topology provided for us
func handleTopologyV1(msg maelstrom.Message) error {
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
	return nil
}

func handleTopologyV2(nodes []string, level int) {
	// use indexed priority queue because IPQs are cool :)
	size := len(nodes)
	if level >= size {
		log.Fatal("level must be less than number of nodes")
	}
	// our mapping (e.g. 0: "n0", 1: "n1")
	indexToNodeID := map[int]string{}

	// values are priorities of each node
	values := make([]int, size)

	// pm is position mapping of node i to position in PQ
	// e.g. pm[0] = 5 means that node 0's position in PQ is 5
	pm := make([]int, size)

	// im is inverse of pm
	im := make([]int, size)

	top := map[string][]string{}

	for i, nodeId := range nodes {
		indexToNodeID[i] = nodeId
		top[nodeId] = []string{}
	}

	for i := range values {
		values[i] = level
	}

	for i := range pm {
		pm[i] = i
		im[i] = i
	}

	// swap node index a with node index b in our heap
	// dont forget to swap pm
	swap := func(a int, b int) {
		nodeA := im[a]
		nodeB := im[b]

		im[a], im[b] = im[b], im[a]
		pm[nodeA], pm[nodeB] = pm[nodeB], pm[nodeA]
	}

	// we just need sink because priorities are exclusively decreasing
	sink := func(index int) {
		for {
			leftChild := index*2 + 1
			rightChild := index*2 + 2
			largest := leftChild
			if rightChild < size && values[im[rightChild]] >= values[im[largest]] {
				largest = rightChild
			}

			if largest >= size || values[im[index]] > values[im[largest]] {
				break
			}

			swap(index, largest)
			index = largest
		}
	}
	i := -1
	for {
		i++
		if i == size {
			i = 0
		}
		if values[im[0]] == 0 {
			break
		}

		currNodeName := indexToNodeID[i]
		heapNodeName := indexToNodeID[im[0]]

		neighbors := top[currNodeName]

		// skip conditions include:
		// 1. if current node is same as root in our PQ (prevent routing to itself)
		// 2. if node already has enough specified neighbors (promote better balanced internode communication)
		// 3. if node already contains the node as a neighbor
		if i == im[0] || len(neighbors) >= level || slices.Contains(neighbors, heapNodeName) {
			continue
		}

		top[currNodeName] = append(top[currNodeName], heapNodeName)
		values[im[0]]--
		sink(0)
	}
	topology = top
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

	/*
		level=2 results
			:stable-latencies {
								0 0,
								0.5 486,
								0.95 631,
								0.99 707,
								1 764
							}
			:msgs-per-op 13.998848

		level=3 results
			:stable-latencies {
								0 0,
								0.5 312,
								0.95 420,
								0.99 587,
								1 766
							}
			:msgs-per-op 23.101835

		level=4 results
			:stable-latencies {
								0 0,
								0.5 260,
								0.95 375,
								0.99 497,
								1 704
							}
			:msgs-per-op 38.13087
	*/
	// use V1 to use default topology provided by maelstrom
	// use V2 to configure level of internode connectivity (i.e. each node will contain <level> neighbors)
	// higher internode connectivity = lower stable-latency, but higher msgs per op and vice versa
	n.Handle("topology", func(msg maelstrom.Message) error {
		// if err := handleTopologyV1(msg); err != nil {
		// 	return err
		// }

		level := 2
		log.Println("number of nodes: ", len(n.NodeIDs()))
		log.Println("configured degree of connectivity: ", level)
		handleTopologyV2(n.NodeIDs(), level)

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
