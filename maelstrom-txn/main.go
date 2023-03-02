package main

import (
	"context"
	"encoding/json"
	"fmt"
	logger "log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	wg  sync.WaitGroup
	mux sync.RWMutex
)

const dbKey = "database"

func init() {
	wg = sync.WaitGroup{}
	mux = sync.RWMutex{}
}

type TransactionError struct {
	ErrorMessage string
	ErrorCode    int
}

func (t TransactionError) Error() string {
	return t.ErrorMessage
}

func (t TransactionError) Code() int {
	return t.ErrorCode
}

func NewTxError(code int, message string, params ...any) TransactionError {
	return TransactionError{
		ErrorMessage: fmt.Sprintf(message, params...),
		ErrorCode:    code,
	}
}

func handleTxnWithKV(kv *maelstrom.KV, ops []any, resultChannel chan [][]any, errorChannel chan error) {
	defer wg.Done()
	mux.Lock()
	defer mux.Unlock()
	result := [][]any{}

	db, err := kv.Read(context.Background(), dbKey)
	if err != nil {
		if _, ok := err.(*maelstrom.RPCError); ok {
			if writeErr := kv.Write(context.Background(), dbKey, "{}"); writeErr != nil {
				errorChannel <- writeErr
				return
			}
			db = "{}"
		} else {
			errorChannel <- NewTxError(13, "Something went unexpectedly wrong when Reading from KV store: %v", err)
			return
		}
	}

	var database map[int]int
	// var database map[int][]any
	if jsonErr := json.Unmarshal([]byte(db.(string)), &database); jsonErr != nil {
		errorChannel <- NewTxError(13, "Unexpected decoding of database object: %v", err)
		return
	}

	// handle each operation in our transaction
	for _, opt := range ops {
		op := opt.([]any)
		opKey, err := getInt(op[1])
		if err != nil {
			errorChannel <- NewTxError(12, "Requested value should be of type int: %v", err)
			return
		}
		dbVal, ok := database[opKey]
		if op[0].(string) == "w" { // write
			opValue, _ := getInt(op[2])
			database[opKey] = opValue
			// if ok {
			// 	database[opKey] = append(database[opKey], opValue)
			// } else {
			// 	database[opKey] = []any{opValue}
			// }
			result = append(result, []any{"w", opKey, opValue})

		} else { // read
			if ok {
				result = append(result, []any{"r", opKey, dbVal})
				// result = append(result, []any{"r", opKey, dbVal[len(dbVal)-1]})
			} else {
				result = append(result, []any{"r", opKey, nil})
			}
		}
	}
	newDbVal, _ := json.Marshal(database)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if string(newDbVal) != db {
		if err := kv.CompareAndSwap(ctx, dbKey, db, string(newDbVal), false); err != nil {
			wg.Add(1)
			go handleTxnWithKV(kv, ops, resultChannel, errorChannel)
			return
		}
	}
	resultChannel <- result
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)

	n.Handle("txn", func(msg maelstrom.Message) error {

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		resultChannel := make(chan [][]any)
		errorChannel := make(chan error)
		wg.Add(1)
		go handleTxnWithKV(kv, body["txn"].([]any), resultChannel, errorChannel)

		select {
		case res := <-resultChannel:
			n.Reply(msg, map[string]any{
				"type": "txn_ok",
				"txn":  res,
			})
		case txErr := <-errorChannel:
			n.Reply(msg, map[string]any{
				"type": "",
				"code": txErr.(TransactionError).Code(),
				"text": txErr.Error(),
			})
		}

		return nil
	})

	if err := n.Run(); err != nil {
		logger.Fatal(err)
	}
	wg.Wait()
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
