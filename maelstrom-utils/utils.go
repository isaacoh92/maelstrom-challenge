package utils

import (
	"log"

	"golang.org/x/exp/slices"
)

func HandleTopology(nodes []string, level int) map[string][]string {
	// use indexed priority queue because IPQs are cool :)
	size := len(nodes)
	if level >= size {
		log.Fatal("level must be less than number of nodes")
	}
	// our mapping (e.g. 0: "n0", 1: "n1")
	indexToNodeID := map[int]string{}

	// values are priorities of each node
	values := make([]int, size)

	// pm is position mapping of node i to position in PQ
	// e.g. pm[0] = 5 means that node 0's position in PQ is 5
	pm := make([]int, size)

	// im is inverse of pm
	im := make([]int, size)

	top := map[string][]string{}

	for i, nodeId := range nodes {
		indexToNodeID[i] = nodeId
		top[nodeId] = []string{}
	}

	for i := range values {
		values[i] = level
	}

	for i := range pm {
		pm[i] = i
		im[i] = i
	}

	// swap node index a with node index b in our heap
	// dont forget to swap pm
	swap := func(a int, b int) {
		nodeA := im[a]
		nodeB := im[b]

		im[a], im[b] = im[b], im[a]
		pm[nodeA], pm[nodeB] = pm[nodeB], pm[nodeA]
	}

	// we just need sink because priorities are exclusively decreasing
	sink := func(index int) {
		for {
			leftChild := index*2 + 1
			rightChild := index*2 + 2
			largest := leftChild
			if rightChild < size && values[im[rightChild]] >= values[im[largest]] {
				largest = rightChild
			}

			if largest >= size || values[im[index]] > values[im[largest]] {
				break
			}

			swap(index, largest)
			index = largest
		}
	}
	i := -1
	for {
		i++
		if i == size {
			i = 0
		}
		if values[im[0]] == 0 {
			break
		}

		currNodeName := indexToNodeID[i]
		heapNodeName := indexToNodeID[im[0]]

		neighbors := top[currNodeName]

		// skip conditions include:
		// 1. if current node is same as root in our PQ (prevent routing to itself)
		// 2. if node already has enough specified neighbors (promote better balanced internode communication)
		// 3. if node already contains the node as a neighbor
		if i == im[0] || len(neighbors) >= level || slices.Contains(neighbors, heapNodeName) {
			continue
		}

		top[currNodeName] = append(top[currNodeName], heapNodeName)
		values[im[0]]--
		sink(0)
	}
	return top
}
