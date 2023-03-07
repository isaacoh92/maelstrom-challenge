package main

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"hash/crc32"
// 	logger "log"
// 	"sort"
// 	"sync"
// 	"time"

// 	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
// )

// var (
// 	setupOnce sync.Once
// 	topology  map[string][]string
// 	hashRing  HashRing
// 	topMux    sync.RWMutex
// 	wg        sync.WaitGroup
// 	mux       sync.RWMutex
// 	cache     Cache
// )

// func init() {
// 	wg = sync.WaitGroup{}
// 	mux = sync.RWMutex{}
// 	topMux = sync.RWMutex{}
// 	cache = Cache{
// 		mux:  sync.RWMutex{},
// 		logs: map[string]*Log{},
// 	}
// 	hashRing = HashRing{
// 		hashToNode: map[int]string{},
// 		hashes:     []int{},
// 	}
// }

// type HashRing struct {
// 	hashToNode map[int]string
// 	hashes     []int
// }

// func (h *HashRing) getNode(key string) string {
// 	index := 0
// 	checksum := int(crc32.ChecksumIEEE([]byte(key)))

// 	// if the checksum is larger than our largest checksum, then our first node is responsible for it
// 	if checksum < h.hashes[len(h.hashes)-1] {
// 		for i, hash := range h.hashes {
// 			index = i
// 			if checksum < hash {
// 				break
// 			}
// 		}
// 	}

// 	logger.Println("checksum", checksum, "node", h.hashToNode[h.hashes[index]])
// 	return h.hashToNode[h.hashes[index]]
// }

// type Log struct {
// 	Messages  [][]int `json:"messages"` // list of [offset, msg] pairs
// 	Key       string  `json:"key"`
// 	Offset    int     `json:"offset"`    // last offset
// 	Committed int     `json:"committed"` // last committed offset
// }

// // a very simple cache, used to store logs in memory for "frequently accessed logs"
// // used to reduce number of calls to our KV store
// type Cache struct {
// 	mux  sync.RWMutex
// 	logs map[string]*Log
// }

// func getLogsFromOffset(logs *Log, offset int) [][]int {
// 	result := [][]int{}

// 	var include bool
// 	for _, m := range logs.Messages {
// 		if m[0] == offset {
// 			include = true
// 		}

// 		if include {
// 			result = append(result, []int{m[0], m[1]})
// 		}

// 	}
// 	return result
// }

// func (c *Cache) getLogs(key string, offset int) [][]int {
// 	c.mux.RLock()
// 	logs, exists := c.logs[key]
// 	c.mux.RUnlock()

// 	if !exists {
// 		return nil
// 	}
// 	return getLogsFromOffset(logs, offset)
// }

// // func write(n *maelstrom.Node, kv *maelstrom.KV, delta int) {
// // 	defer wg.Done()
// // 	v := getCounter()
// // 	plusDelta := v + delta
// // if e := kv.CompareAndSwap(context.Background(), counterKey, v, v+delta, true); e != nil {
// // 	v, _ := kv.ReadInt(context.Background(), counterKey)
// // 	setCounter(v)
// // 	wg.Add(1)
// // 	go write(n, kv, delta)
// // 	return
// // }

// // 	setCounter(plusDelta)
// // }

// func (c *Cache) appendLog(key string, msg int) int {
// 	c.mux.Lock()

// 	if _, exists := c.logs[key]; !exists {
// 		c.logs[key] = &Log{
// 			Messages:  [][]int{},
// 			Committed: 0,
// 			Key:       key,
// 			Offset:    -1,
// 		}
// 	}

// 	c.logs[key].Offset++
// 	retOffset := c.logs[key].Offset
// 	c.logs[key].Messages = append(c.logs[key].Messages, []int{retOffset, msg})

// 	c.mux.Unlock()
// 	return retOffset
// }

// func (c *Cache) commitOffset(key string, offset int) {
// 	c.mux.Lock()
// 	if _, ok := c.logs[key]; !ok {
// 		c.logs[key] = &Log{
// 			Messages:  [][]int{},
// 			Key:       key,
// 			Offset:    offset,
// 			Committed: 0,
// 		}
// 		return
// 	}
// 	c.logs[key].Committed = offset
// 	c.mux.Unlock()
// }

// func (c *Cache) getCommittedOffset(key string) int {
// 	c.mux.RLock()
// 	var committed int
// 	if _, ok := c.logs[key]; !ok {
// 		committed = 0
// 	} else {
// 		committed = c.logs[key].Committed
// 	}
// 	c.mux.RUnlock()
// 	return committed
// }

// /*
// Requests in the form of a map (e.g. poll and commit_offsets requests)

// 	"offsets": {
// 	    "k1": 1000,
// 	    "k2": 2000,
// 		"k3": 3000
// 	}

// # For each key, find the node responsible for it and group them together

// 	e.g. {
// 		"node1": {
// 			"k1": 1000,
// 			"k3": 3000
// 		}

// 		"node2": {
// 			"k2": 2000,
// 		}
// 	}
// */
// func groupLogsToNodesMap(request map[string]any) map[string]map[string]int {
// 	requestMap := map[string]map[string]int{}
// 	for k, v := range request {
// 		// get the node responsible for key k
// 		node := hashRing.getNode(k)

// 		// add the key and offset to the node's map
// 		if _, ok := requestMap[node]; !ok {
// 			requestMap[node] = map[string]int{}
// 		}
// 		offset, _ := getMessage(v)
// 		requestMap[node][k] = offset
// 	}

// 	return requestMap
// }

// /*
// Requests in the form of a list (e.g. list_committed_offsets request)

// 	"keys": ["k1", "k2"]
// */
// func groupLogsToNodesList(request []any) map[string][]string {
// 	requestMap := map[string][]string{}
// 	for _, k := range request {
// 		key := k.(string)
// 		node := hashRing.getNode(key)
// 		if _, ok := requestMap[node]; !ok {
// 			requestMap[node] = []string{}
// 		}
// 		requestMap[node] = append(requestMap[node], key)
// 	}

// 	return requestMap
// }

// // forwards a request to the specified dest node and simply returns the response
// func forwardRequest(node *maelstrom.Node, dest string, body map[string]any) (map[string]any, error) {
// 	msg, err := node.SyncRPC(context.Background(), dest, body)
// 	if err != nil {
// 		return nil, err
// 	}
// 	var response map[string]any
// 	if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
// 		return nil, jsonErr
// 	}

// 	return response, nil
// }

// // forwards a request to the specified dest node and simply returns the response
// func forwardRequestAsync(node *maelstrom.Node, dest string, body map[string]any, result chan map[string]any) {
// 	logger.Println("forwarding request to node ", dest, body["msg_id"], body["type"])
// 	defer wg.Done()

// 	ctx, cancel := context.WithTimeout(context.Background(), 1000*time.Millisecond)
// 	defer cancel()

// respCh := make(chan maelstrom.Message)
// if err := node.RPC(dest, body, func(m maelstrom.Message) error {
// 	respCh <- m
// 	return nil
// }); err != nil {
// 	wg.Add(1)
// 	go forwardRequestAsync(node, dest, body, result)
// 	return
// }

// select {
// case msg := <-respCh:
// 	logger.Println("forwarding successful ", dest, body["msg_id"], body["type"])
// 	var response map[string]any
// 	if jsonErr := json.Unmarshal(msg.Body, &response); jsonErr != nil {
// 		logger.Fatal("noooo", jsonErr)
// 	}

// 	result <- response
// 	return
// case <-ctx.Done():
// 	logger.Println(body["msg_id"], body["type"], "took too long, trying again...")
// 	wg.Add(1)
// 	go forwardRequestAsync(node, dest, body, result)
// }

// 	// respCh := make(chan maelstrom.Message)
// 	// a := time.Now()
// 	// if err := node.RPC(dest, body, func(m maelstrom.Message) error {
// 	// 	respCh <- m
// 	// 	return nil
// 	// }); err != nil {
// 	// 	// wg.Add(1)
// 	// 	// go broadcast(node, neighbor, body)
// 	// 	return nil, err
// 	// }

// 	// msg := <-respCh
// 	// // if err := m.RPCError(); err != nil {
// 	// // 	wg.Add(1)
// 	// // 	go broadcast(node, neighbor, body)
// 	// // }
// 	// // if err != nil {
// 	// // 	return nil, err
// 	// // }

// }

// func handlePollAction(node *maelstrom.Node, offsetsRequest map[string]any) (map[string][][]int, error) {
// 	result := map[string][][]int{}

// 	/*
// 		Our nodesToLogsMapping would look something like this
// 		{
// 			"node1": {
// 				"k1": 1000,
// 				"k3": 3000
// 			}

// 			"node2": {
// 				"k2": 2000,
// 			}
// 		}
// 	*/
// 	nodesToLogsMapping := groupLogsToNodesMap(offsetsRequest)
// 	logger.Println("ISAAC HERE", nodesToLogsMapping)
// 	for dest, offsets := range nodesToLogsMapping {

// 		if dest == node.ID() { // handle response locally
// 			for key, v := range offsets {
// 				offset, err := getMessage(v)
// 				if err != nil {
// 					return nil, err
// 				}
// 				result[key] = cache.getLogs(key, offset)
// 			}
// 		} else { // otherwise, forward response to appropriate node that can handle it
// 			// response, fwdErr := forwardRequest(node, dest, map[string]any{
// 			// 	"type":    "poll",
// 			// 	"offsets": offsets,
// 			// })
// 			// if fwdErr != nil {
// 			// 	logger.Fatal("FAIL", fwdErr)
// 			// 	return nil, fwdErr
// 			// }
// 			resultChannel := make(chan map[string]any)
// 			wg.Add(1)
// 			go forwardRequestAsync(node, dest, map[string]any{
// 				"type":    "poll",
// 				"offsets": offsets,
// 			}, resultChannel)

// 			response := <-resultChannel
// 			msgs := response["msgs"].(map[string]any)
// 			for key, nodeMessages := range msgs {
// 				// let's just encode from interface{} to string to simplify converting to [][]int
// 				msgString, encodeErr := json.Marshal(nodeMessages)
// 				if encodeErr != nil {
// 					return nil, encodeErr
// 				}

// 				// decode
// 				var msgTyped [][]int
// 				if decodeErr := json.Unmarshal(msgString, &msgTyped); decodeErr != nil {
// 					return nil, decodeErr
// 				}
// 				result[key] = msgTyped
// 			}
// 		}
// 	}
// 	return result, nil
// }

// func handleSendAction(node *maelstrom.Node, request map[string]any) (int, error) {
// 	var offset int

// 	key := request["key"].(string)
// 	message, err := getMessage(request["msg"])

// 	if err != nil {
// 		return offset, err
// 	}

// 	dest := hashRing.getNode(key)
// 	if dest == node.ID() { // handle locally
// 		offset = cache.appendLog(key, message)
// 	} else { // forward the send request to the appropriate node
// 		resultChannel := make(chan map[string]any)
// 		wg.Add(1)
// 		go forwardRequestAsync(node, dest, request, resultChannel)

// 		response := <-resultChannel
// 		// response, fwdErr := forwardRequest(node, dest, request)

// 		// if fwdErr != nil {
// 		// 	logger.Fatal("FAIL", fwdErr)
// 		// 	return offset, fwdErr
// 		// }
// 		// Since the client needs the correct offset, we need to wait for the dest node's response
// 		offset, err = getMessage(response["offset"])
// 		if err != nil {
// 			return offset, err
// 		}
// 	}

// 	return offset, nil
// }

// func handleCommitAction() {

// }

// func handleListCommitAction() {

// }

// func main() {

// 	n := maelstrom.NewNode()
// 	// kv := maelstrom.NewSeqKV(n)

// 	n.Handle("send", func(msg maelstrom.Message) error {
// 		setupNodes(n.NodeIDs())
// 		var body map[string]any
// 		if err := json.Unmarshal(msg.Body, &body); err != nil {
// 			return err
// 		}
// 		// key := body["key"].(string)
// 		// message, err := getMessage(body["msg"])
// 		// if err != nil {
// 		// 	return err
// 		// }

// 		// offset := cache.appendLog(key, message)
// 		offset, err := handleSendAction(n, body)
// 		if err != nil {
// 			return err
// 		}
// 		n.Reply(msg, map[string]any{
// 			"type":   "send_ok",
// 			"offset": offset,
// 		})

// 		return nil
// 	})

// 	n.Handle("poll", func(msg maelstrom.Message) error {
// 		setupNodes(n.NodeIDs())
// 		var body map[string]any
// 		if err := json.Unmarshal(msg.Body, &body); err != nil {
// 			return err
// 		}
// 		result := map[string][][]int{}
// 		offsets := body["offsets"].(map[string]any)

// 		result, err := handlePollAction(n, offsets)
// 		if err != nil {
// 			return err
// 		}
// 		n.Reply(msg, map[string]any{
// 			"type": "poll_ok",
// 			"msgs": result,
// 		})
// 		return nil
// 	})

// 	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
// 		setupNodes(n.NodeIDs())
// 		var body map[string]any
// 		if err := json.Unmarshal(msg.Body, &body); err != nil {
// 			return err
// 		}

// 		offsets := body["offsets"].(map[string]any)
// 		for key, v := range offsets {
// 			offset, _ := getMessage(v)
// 			cache.commitOffset(key, offset)
// 		}
// 		n.Reply(msg, map[string]any{
// 			"type": "commit_offsets_ok",
// 		})
// 		return nil
// 	})

// 	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
// 		setupNodes(n.NodeIDs())
// 		var body map[string]any
// 		if err := json.Unmarshal(msg.Body, &body); err != nil {
// 			return err
// 		}
// 		result := map[string]int{}
// 		for _, key := range body["keys"].([]any) {
// 			result[key.(string)] = cache.getCommittedOffset(key.(string))
// 		}
// 		n.Reply(msg, map[string]any{
// 			"type":    "list_committed_offsets_ok",
// 			"offsets": result,
// 		})
// 		return nil
// 	})

// 	if err := n.Run(); err != nil {
// 		logger.Fatal(err)
// 	}
// 	wg.Wait()
// }

// func getMessage(m any) (int, error) {
// 	var message int
// 	switch v := m.(type) {
// 	case int:
// 		message = v
// 	case float64:
// 		message = int(v)
// 	default:
// 		return message, fmt.Errorf("unsupported type %T", v)
// 	}
// 	return message, nil
// }

// func setupNodes(ids []string) {
// 	setupOnce.Do(func() {
// 		// virtualNodes := 3
// 		for _, nodeID := range ids {
// 			checksum := int(crc32.ChecksumIEEE([]byte(nodeID)))
// 			hashRing.hashToNode[checksum] = nodeID
// 			hashRing.hashes = append(hashRing.hashes, checksum)
// 		}
// 		sort.Sort(sort.IntSlice(hashRing.hashes))
// 	})
// }
