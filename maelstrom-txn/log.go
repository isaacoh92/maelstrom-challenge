package main

type Logs struct {
	//node    *maelstrom.Node
	entries []*Log
}

type Log struct {
	operation string
	term      int
}

func InitLogs() *Logs {
	return &Logs{
		//node:    node,
		entries: []*Log{
			&Log{
				operation: "",
				term:      0,
			}},
	}
}

// Get Log is 1-indexed
func (logs *Logs) Get(index int) *Log {
	return logs.entries[index-1]
}

func (logs *Logs) Append(entries ...*Log) {
	logs.entries = append(logs.entries, entries...)
}

func (logs *Logs) Last() *Log {
	return logs.entries[logs.Size()-1]
}

func (logs *Logs) LastLogTerm() int {
	return logs.Last().term
}

func (logs *Logs) Size() int {
	return len(logs.entries)
}
