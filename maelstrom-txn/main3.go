package main

// import (
// 	"fmt"
// 	logger "log"
// 	"sync"

// 	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
// )

// var (
// 	wg   sync.WaitGroup
// 	mux  sync.RWMutex
// 	raft *Raft
// )

// const dbKey = "database"

// func init() {
// 	wg = sync.WaitGroup{}
// 	mux = sync.RWMutex{}
// }

// type TransactionError struct {
// 	ErrorMessage string
// 	ErrorCode    int
// }

// func (t TransactionError) Error() string {
// 	return t.ErrorMessage
// }

// func (t TransactionError) Code() int {
// 	return t.ErrorCode
// }

// func NewTxError(code int, message string, params ...any) TransactionError {
// 	return TransactionError{
// 		ErrorMessage: fmt.Sprintf(message, params...),
// 		ErrorCode:    code,
// 	}
// }

// // func main() {
// // 	rand.Seed(time.Now().UnixNano())
// // 	min := 10
// // 	max := 30
// // 	fmt.Println(rand.Intn(max-min+1) + min)

// //		c := map[string]bool{}
// //		fmt.Println(c["hello"])
// //	}
// func main() {
// 	n := maelstrom.NewNode()
// 	// kv := maelstrom.NewLinKV(n)
// 	raft = InitRaft(n)

// 	n.Handle("txn", func(msg maelstrom.Message) error {
// 		// raft.SetNode(n)
// 		return raft.HandleClientRequest(msg)
// 	})

// 	n.Handle("vote", func(msg maelstrom.Message) error {

// 		return raft.SubmitVote(msg)
// 	})

// 	// n.Handle("receive_vote", func(msg maelstrom.Message) error {
// 	// 	return nil
// 	// })

// 	n.Handle("append_entries", func(msg maelstrom.Message) error {
// 		return nil
// 	})

// 	if err := n.Run(); err != nil {
// 		logger.Fatal(err)
// 	}
// 	// wg.Wait()
// }

// func getInt(m any) (int, error) {
// 	var delta int
// 	switch v := m.(type) {
// 	case int:
// 		delta = v
// 	case float64:
// 		delta = int(v)
// 	default:
// 		return delta, fmt.Errorf("unsupported type %T", v)
// 	}
// 	return delta, nil
// }
