package main

import (
	"fmt"
)

func main() {
	q, err := NewQueue("queue.db", []byte("queue_"))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("enqueue now...")
	err = q.Enqueue([]byte("hello world"))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("current size is",  q.Size())

	value, err := q.Dequeue()

	if err != nil {

		fmt.Println(err)
		return
	}
	fmt.Println("pop value", string(value))

	fmt.Println("current size is",  q.Size())
}

