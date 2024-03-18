package main

import (
	// "strings"
	// "container/heap"
	//"container/heap"
	//"fmt"
	//"strconv"
)

// source: https://stackoverflow.com/questions/59078929/how-do-you-use-the-heap-package-in-go

type Message struct {
    msg string
    priority int
	deliverable bool
	sender int	// tiebreaker -> if sent by node3, then 3 is stored in sender
	init_time float64
}

 type Interface interface {
// 	//sort.Interface
// 	Push(x interface{}) // add x as element Len()
 	Top() interface{}   // remove and return element Len() - 1.
 }

type PriorityQueue []Message

func (h PriorityQueue) Len() int {
	return len(h)
}

func (h PriorityQueue) Less(i int, j int) bool {
	tie1 := float64(h[i].sender) * 0.1
	tie2 := float64(h[j].sender) * 0.1
	one := float64(h[i].priority) + tie1
	//fmt.Println(one)
	two := float64(h[j].priority) + tie2
	//fmt.Println(two)
	return one < two
}

func (h PriorityQueue) Swap(i int, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *PriorityQueue) Push(x interface{}) {
	*h = append(*h, x.(Message))
}

func (h *PriorityQueue) Pop() interface{} {
	q := *h
	x := q[len(q) - 1]
	*h = q[0 : len(q) - 1]
	// x := q[0]
	// *h = q[1 : len(q)]
	return x
}

func (h *PriorityQueue) Top() interface{} {
	q := *h
	if len(q) == 0 {
		return nil
	}

	x := q[len(q) - 1]
	//x := q[0]
	return x
}