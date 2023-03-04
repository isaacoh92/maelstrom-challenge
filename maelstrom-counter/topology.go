package main

import (
	"log"

	"golang.org/x/exp/slices"
)

/*
Indexed Priority Queue
Fields:

	KeyIndex: mapping of keys to node ID
		- e.g. {
			0: "n0",
			1: "n1",
			2: "n2",
		}

	Values: Priority values

	Pm: Position Map
		- Given an index i, where i is equal to the key index of a node n, pm[i] is n's position in our priority queue
		- Example
			- pm = [8, 15, 30]
			- index i = 0, 0 is the key index for node n0. The position of n0 in our PQ is 8

	Im: Inverse Map (Heap)
		- Inverse of Position Map
		- Given an index of the heap i, im[i] is the key index of the node in position i of our PQ
		- Example
			- im = [1, 2, 3]
			- im[0] = 1
			- n1 is at the top of our heap
*/
type IPQ struct {
	KeyIndex map[int]string
	Values   []int
	Pm       []int
	Im       []int
	Level    int // level of connectivity
}

func InitPQ(nodes []string, connectivity int) IPQ {
	pq := IPQ{
		KeyIndex: map[int]string{},
		Values:   []int{},
		Pm:       []int{},
		Im:       []int{},
		Level:    connectivity,
	}
	pq.IndexNodes(nodes)
	return pq
}

func (pq *IPQ) IndexNodes(nodes []string) {
	for i, node := range nodes {
		pq.KeyIndex[i] = node

		pq.Values = append(pq.Values, pq.Level)
		pq.Pm = append(pq.Pm, i)
		pq.Im = append(pq.Im, i)
	}
}

// Swap two nodes in our PQ
func (pq *IPQ) Swap(indexA, indexB int) {
	nodeA := pq.Im[indexA]
	nodeB := pq.Im[indexB]

	pq.Im[indexA], pq.Im[indexB] = pq.Im[indexB], pq.Im[indexA]
	pq.Pm[nodeA], pq.Pm[nodeB] = pq.Pm[nodeB], pq.Pm[nodeA]
}

func (pq *IPQ) Sink(index int) {
	size := len(pq.KeyIndex)
	for {
		leftChild := index*2 + 1
		rightChild := index*2 + 2
		largest := leftChild

		// If right child exists and its priority is >= left child, set largest to right child
		if rightChild < size && pq.Values[pq.Im[rightChild]] >= pq.Values[pq.Im[largest]] {
			largest = rightChild
		}

		if largest >= size || pq.Values[pq.Im[index]] > pq.Values[pq.Im[largest]] {
			break
		}

		pq.Swap(index, largest)
		index = largest
	}
}

func (pq *IPQ) SinkRoot() {
	pq.Sink(0)
}

func (pq *IPQ) Peek() int {
	return pq.Im[0]
}

func (pq *IPQ) PriorityOf(keyIndex int) int {
	return pq.Values[keyIndex]
}

func (pq *IPQ) NameOf(keyIndex int) string {
	return pq.KeyIndex[keyIndex]
}

func (pq *IPQ) DecrementPriorityOf(keyIndex int) {
	pq.Values[keyIndex]--
}

func GenerateTopology(nodes []string, level int) map[string][]string {
	// use indexed priority queue because IPQs are cool :)
	if level <= 0 {
		return nil
	}
	size := len(nodes)
	if level >= size {
		log.Fatal("level must be less than number of nodes")
	}

	top := map[string][]string{}

	for _, nodeId := range nodes {
		top[nodeId] = []string{}
	}

	pq := InitPQ(nodes, level)
	i := -1
	for {
		i++
		if i == size {
			i = 0
		}

		// If all nodes have a priority of 0, we cycled through all of them
		if pq.PriorityOf(pq.Peek()) == 0 {
			break
		}

		currNodeName := pq.NameOf(i)
		heapNodeName := pq.NameOf(pq.Peek())
		neighbors := top[currNodeName]

		// skip conditions include:
		// 1. if current node is same as root in our PQ (prevent routing to itself)
		// 2. if node already has enough specified neighbors (promote better balanced internode communication)
		// 3. if node already contains the node as a neighbor
		if i == pq.Peek() || len(neighbors) >= level || slices.Contains(neighbors, heapNodeName) {
			continue
		}

		top[currNodeName] = append(top[currNodeName], heapNodeName)
		pq.DecrementPriorityOf(pq.Peek())
		pq.SinkRoot()
	}
	return top
}
