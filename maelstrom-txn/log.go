package main

type Logs struct {
	//node    *maelstrom.Node
	Entries []*Log `json:"entries"`
}

type Log struct {
	Operation []any `json:"ops"`
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

func (logs *Logs) FromIndex(index int) *Logs {
	return &Logs{logs.Entries[index-1:]}
}

func (logs *Logs) Append(entries ...*Log) {
	logs.Entries = append(logs.Entries, entries...)
}

func (logs *Logs) Last() *Log {
	//return logs.entries[logs.Size()-1]
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
