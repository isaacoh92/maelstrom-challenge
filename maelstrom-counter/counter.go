package main

import (
	"sync"
)

type Counter struct {
	IDGenerator *UniqueID
	Counters    map[string]int
	Mux         sync.RWMutex
	Changed     bool
}

func InitCounter(node string) *Counter {
	c := &Counter{
		IDGenerator: InitIDGenerator(node),
		Counters:    map[string]int{},
		Mux:         sync.RWMutex{},
		Changed:     false,
	}
	return c
}

func (c *Counter) Merge(peerCounters map[string]int) {
	c.Mux.Lock()
	defer c.Mux.Unlock()
	c.Changed = true
	c.Counters = mergeCounters(c.Counters, peerCounters)
}

func mergeCounters(a, b map[string]int) map[string]int {
	result := map[string]int{}
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func (c *Counter) Count() int {
	c.Mux.RLock()
	defer c.Mux.RUnlock()
	sum := 0
	for _, v := range c.Counters {
		sum += v
	}
	return sum
}

func (c *Counter) Add(value int) {
	c.Mux.Lock()
	defer c.Mux.Unlock()
	c.Counters[c.IDGenerator.GenerateID()] = value
	c.Changed = true
}
