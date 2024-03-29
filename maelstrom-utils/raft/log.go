package raft

import (
	"encoding/json"
	"log"
)

type Logs struct {
	Entries []*Log `json:"entries"`
}

type Log struct {
	Operation []any `json:"op"`
	Term      int   `json:"term"`
}

func InitLogs() *Logs {
	return &Logs{
		Entries: []*Log{
			&Log{
				Operation: []any{},
				Term:      0,
			},
		},
	}
}

// Get Log is 1-indexed
func (logs *Logs) Get(index int) *Log {
	return logs.Entries[index-1]
}

func (logs *Logs) HasIndex(index int) bool {
	return index <= logs.Size()
}

func (logs *Logs) FromIndex(index int) *Logs {

	return &Logs{logs.Entries[index-1:]}
}

func (logs *Logs) Append(entries ...*Log) {
	logs.Entries = append(logs.Entries, entries...)
}

func (logs *Logs) Last() *Log {
	return logs.Get(logs.Size())
}

func (logs *Logs) LastLogTerm() int {
	return logs.Last().Term
}

func (logs *Logs) Size() int {
	return len(logs.Entries)
}

// Truncate the entries up to the specified index
func (logs *Logs) Truncate(index int) {
	logs.Entries = logs.Entries[:index] // up to :index because our entries are 1-indexed
}

func (logs *Logs) PrintLogs() string {
	r, err := json.Marshal(logs.Entries)
	if err != nil {
		log.Printf("error encoding logs: %v", err)
	}
	return string(r)
}
