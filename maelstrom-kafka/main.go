package main

import (
	"context"
	"encoding/json"
	"fmt"
	logger "log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	wg  sync.WaitGroup
	mux sync.RWMutex
)

func init() {
	wg = sync.WaitGroup{}
	mux = sync.RWMutex{}
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("send", func(msg maelstrom.Message) error {
		type request struct {
			Type    string `json:"type"`
			Key     string `json:"key"`
			Message int    `json:"msg"`
		}

		var body request
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		wg.Add(1)
		resultChannel := make(chan int)
		errorChannel := make(chan error)
		go writeLog(n, kv, body.Key, body.Message, resultChannel, errorChannel)

		var offset int
		select {
		case o := <-resultChannel:
			offset = o
		case err := <-errorChannel:
			return err
		}

		n.Reply(msg, map[string]any{
			"type":   "send_ok",
			"offset": offset,
		})

		return nil
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		type request struct {
			Type    string         `json:"type"`
			Offsets map[string]any `json:"offsets"`
		}

		var body request
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		result := map[string][][]int{}

		for key, value := range body.Offsets {
			offset, err := getInt(value)
			if err != nil {
				return err
			}
			messages, err := readLog(n, kv, key, offset)
			if err != nil {
				return err
			}
			result[key] = messages
		}
		n.Reply(msg, map[string]any{
			"type": "poll_ok",
			"msgs": result,
		})
		return nil
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		type request struct {
			Type    string         `json:"type"`
			Offsets map[string]any `json:"offsets"`
		}

		var body request
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		for key, value := range body.Offsets {
			offset, err := getInt(value)
			if err != nil {
				return err
			}
			wg.Add(1)
			go writeCommittedOffset(n, kv, key, offset)
		}
		n.Reply(msg, map[string]any{
			"type": "commit_offsets_ok",
		})
		return nil
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		type request struct {
			Type string   `json:"type"`
			Keys []string `json:"keys"`
		}

		var req request
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}
		result := map[string]int{}
		for _, key := range req.Keys {
			offset := readCommittedOffset(n, kv, key)
			result[key] = offset
		}

		n.Reply(msg, map[string]any{
			"type":    "list_committed_offsets_ok",
			"offsets": result,
		})
		return nil
	})
	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
	wg.Wait()
}

func writeCommittedOffset(n *maelstrom.Node, kv *maelstrom.KV, key string, offset int) {
	defer wg.Done()
	kvKey := fmt.Sprintf("committed_%s", key)
	committed, _ := kv.ReadInt(context.Background(), kvKey)
	if e := kv.CompareAndSwap(context.Background(), kvKey, committed, offset, true); e != nil {
		wg.Add(1)
		go writeCommittedOffset(n, kv, key, offset)
		return
	}
}

func readCommittedOffset(n *maelstrom.Node, kv *maelstrom.KV, key string) int {
	kvKey := fmt.Sprintf("committed_%s", key)
	committed, err := kv.ReadInt(context.Background(), kvKey)
	if err != nil {
		return 0
	}
	return committed
}

type Log struct {
	Offset   int     `json:"offset"`
	Messages [][]int `json:"messages"`
}

func writeLog(n *maelstrom.Node, kv *maelstrom.KV, key string, message int, resultChannel chan int, errorChannel chan error) {
	defer wg.Done()
	kvKey := key
	var log Log
	committed, err := kv.Read(context.Background(), kvKey)
	if err != nil {
		log = Log{
			Offset:   -1,
			Messages: [][]int{},
		}
	} else {
		if jsonErr := json.Unmarshal([]byte(committed.(string)), &log); jsonErr != nil {
			errorChannel <- jsonErr
			return
		}
	}

	log.Offset++
	log.Messages = append(log.Messages, []int{log.Offset, message})

	newEntry, err := json.Marshal(log)
	if err != nil {
		errorChannel <- err
		return
	}

	if e := kv.CompareAndSwap(context.Background(), kvKey, committed, string(newEntry), true); e != nil {
		logger.Println("rewriting due to contention...")
		wg.Add(1)
		go writeLog(n, kv, key, message, resultChannel, errorChannel)
		return
	}

	resultChannel <- log.Offset
}

func readLog(n *maelstrom.Node, kv *maelstrom.KV, key string, offset int) ([][]int, error) {
	var log Log
	kvKey := key
	committed, err := kv.Read(context.Background(), kvKey)
	if err != nil {
		log = Log{
			Offset:   0,
			Messages: [][]int{},
		}
	} else {
		if jsonErr := json.Unmarshal([]byte(committed.(string)), &log); jsonErr != nil {
			return nil, jsonErr
		}
	}
	return getLogsFromOffset(&log, offset), nil
}

func getLogsFromOffset(logs *Log, offset int) [][]int {
	result := [][]int{}

	var include bool
	for _, m := range logs.Messages {
		if m[0] == offset {
			include = true
		}

		if include {
			result = append(result, []int{m[0], m[1]})
		}
	}
	return result
}

func getInt(m any) (int, error) {
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
