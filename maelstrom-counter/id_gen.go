package main

import (
	"fmt"
)

type UniqueID struct {
	Node     string
	Sequence int
}

func InitIDGenerator(node string) *UniqueID {
	return &UniqueID{
		Node:     node,
		Sequence: 0,
	}
}

func (u *UniqueID) GenerateID() string {
	u.Sequence++
	return fmt.Sprintf("%s-%d", u.Node, u.Sequence)
}
