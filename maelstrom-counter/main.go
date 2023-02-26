package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"golang.org/x/exp/slices"
)

const counterKey = "counter"

var (
	topology       map[string][]string
	topMux         sync.RWMutex // only used for topology
	wg             sync.WaitGroup
	mux            sync.RWMutex
	counter        int
	counterChanged bool
)

func init() {
	wg = sync.WaitGroup{}
	mux = sync.RWMutex{}
	topMux = sync.RWMutex{}
	counter = 0
}

func getCounter() int {
	mux.RLock()
	c := counter
	mux.RUnlock()
	return c
}

func setCounter(val int) {
	mux.Lock()
	counter = val
	counterChanged = true
	mux.Unlock()
}

func handleTopology(nodes []string, level int) {
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

func write(n *maelstrom.Node, kv *maelstrom.KV, delta int) {
	defer wg.Done()
	v := getCounter()
	plusDelta := v + delta
	if e := kv.CompareAndSwap(context.Background(), counterKey, v, v+delta, true); e != nil {
		v, _ := kv.ReadInt(context.Background(), counterKey)
		setCounter(v)
		wg.Add(1)
		go write(n, kv, delta)
		return
	}

	setCounter(plusDelta)
}

func broadcast(node *maelstrom.Node, neighbor string, body map[string]any) {
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

func main() {

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	go broadcastCounter(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta, err := getDelta(body["delta"])
		if err != nil {
			log.Println("heyhey 1")
			return err
		}

		// if err := write(kv, delta); err != nil {
		// 	log.Println("heyhey 2")
		// 	return err
		// }
		wg.Add(1)
		go write(n, kv, delta)
		var ret map[string]any = map[string]any{}
		ret["type"] = "add_ok"

		return n.Reply(msg, ret)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var ok bool
		topMux.RLock()
		ok = topology != nil
		topMux.RUnlock()
		if !ok {
			level := len(n.NodeIDs()) - 1 // requires a rather high degree of connectivity
			log.Println("number of nodes: ", len(n.NodeIDs()))
			log.Println("configured degree of connectivity: ", level)
			topMux.Lock()
			handleTopology(n.NodeIDs(), level)
			topMux.Unlock()
		}
		body["type"] = "read_ok"
		body["value"] = getCounter()
		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		received, err := getDelta(body["counter"])
		if err != nil {
			return err
		}
		curr := getCounter()
		if received > curr {
			setCounter(received)
		}
		return nil
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	wg.Wait()
}

func getDelta(m any) (int, error) {
	var delta int
	switch v := m.(type) {
	case int:
		delta = v
	case float64:
		delta = int(v)
	default:
		return delta, fmt.Errorf("unsupported type %T", v)
	}
	return delta, nil
}

func broadcastCounter(node *maelstrom.Node) {
	for range time.Tick(time.Millisecond * 1) {
		mux.RLock()
		send := counterChanged
		mux.RUnlock()

		if send {
			for _, neighbor := range topology[node.ID()] {
				wg.Add(1)
				go broadcast(node, neighbor, map[string]any{
					"type":    "broadcast",
					"counter": getCounter(),
					"src":     node.ID(),
				})
			}
		}

		mux.Lock()
		counterChanged = false
		mux.Unlock()
	}
}
