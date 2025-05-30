package main

import (
	"sync"
	"errors"
	"fmt"
	"encoding/binary"
	"github.com/syndtr/goleveldb/leveldb"
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
}


// a wrapper for leveldb
type LevelDBQueue struct {
	db       *leveldb.DB
	path     string
	prefix   []byte
	mu       sync.Mutex
	enqueued uint64 //a counter for item in queue
	dequeued uint64 // item leave queue
}


func NewQueue(path string, prefix []byte) (*LevelDBQueue, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	return &LevelDBQueue{
		db: db,
		path: path,
		prefix: prefix,
	}, nil
}


func (q *LevelDBQueue) Close() error {
	return q.db.Close()
}


func (q *LevelDBQueue) generateKey() []byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueued++

	// uint64 is 8 byte
	key := make([]byte, len(q.prefix) + 8)
	copy(key, q.prefix)
	// append to prefix, if prefix is queue_, full key is queue_\0x...x8
	binary.BigEndian.PutUint64(key[len(q.prefix):], q.enqueued)
	return key
}

func (q *LevelDBQueue) Enqueue(value []byte) error {
	return q.db.Put(q.generateKey(), value, nil)
}


func (q *LevelDBQueue) Dequeue() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Lock()

	if q.dequeued >= q.enqueued {
		return nil, errors.New("queue is empty")
	}


	key := make([]byte, len(q.prefix) + 8)
	copy(key, q.prefix)
	binary.BigEndian.PutUint64(key[len(q.prefix):], q.dequeued+1)

	value, err := q.db.Get(key, nil)
	if err != nil {
		return nil, err
	}

	if err := q.db.Delete(key, nil); err != nil {
		return nil, err
	}

	q.dequeued++
	return value, nil
}

func (q *LevelDBQueue) Size() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enqueued - q.dequeued
}
