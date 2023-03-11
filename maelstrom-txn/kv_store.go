package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type KV struct {
	mux       *sync.RWMutex
	database  map[int]int
	timestamp map[int]int64
	epoch     int64
	changed   bool
}

func InitKV() *KV {
	return &KV{
		mux:       &sync.RWMutex{},
		database:  map[int]int{},
		timestamp: map[int]int64{},
		epoch:     time.Now().UnixMilli(),
	}
}

func (kv *KV) Read(key int) (int, error) {
	// kv.mux.RLock()
	// defer kv.mux.RUnlock()

	if _, ok := kv.database[key]; !ok {
		return 0, errors.New("not found")
	}
	return kv.database[key], nil
}

func (kv *KV) Write(key int, value int) {
	kv.database[key] = value
	kv.timestamp[key] = time.Now().UnixMilli() - kv.epoch
	kv.changed = true
}

func (kv *KV) Merge(peerKV map[int]int, peerTimestamp map[int]int64) {
	kv.mux.Lock()
	defer kv.mux.Unlock()

	for k, v := range peerKV {
		if _, ok := kv.database[k]; !ok {
			kv.database[k] = v
			kv.timestamp[k] = peerTimestamp[k]
			continue
		}

		if peerTimestamp[k] > kv.timestamp[k] {
			kv.database[k] = v
		}
		kv.timestamp[k] = time.Now().UnixMilli() - kv.epoch
	}
	kv.changed = true
}

func (kv *KV) Transact(transactions [][]any) ([][]any, error) {
	result := [][]any{}
	kv.mux.Lock()
	defer kv.mux.Unlock()

	for _, transaction := range transactions {
		opType, ok := transaction[0].(string)
		if !ok {
			return nil, errors.New("expect first element in tx to be string")
		}
		opKey, err := Int(transaction[1])
		if err != nil {
			return nil, err
		}

		switch opType {
		case "r":
			value, err := kv.Read(opKey)
			if err != nil {
				result = append(result, []any{opType, opKey, nil})
				continue
			}
			result = append(result, []any{opType, opKey, value})

		case "w":
			opValue, err := Int(transaction[2])
			if err != nil {
				return nil, err
			}
			kv.Write(opKey, opValue)
			result = append(result, []any{opType, opKey, opValue})
		}

	}
	return result, nil
}

func Int(m any) (int, error) {
	var res int
	switch v := m.(type) {
	case int:
		res = v
	case float64:
		res = int(v)
	default:
		return res, fmt.Errorf("unsupported type %T", v)
	}
	return res, nil
}
