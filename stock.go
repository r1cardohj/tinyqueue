package main

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/dgraph-io/badger"
)

type Queue struct {
	db       *badger.DB
	path     string
	prefix   []byte
	mu       sync.Mutex
	enqueued uint64
	dequeued uint64
}

func NewQueue(path string, prefix []byte) (*Queue, error) {
	opts := badger.DefaultOptions(path)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	queue := &Queue{
		db:     db,
		path:   path,
		prefix: prefix,
	}

	err = queue.restoreCounters()
	if err != nil {
		return nil, err
	}

	return queue, nil
}

func (q *Queue) restoreCounters() error {
	return q.db.View(func(txn *badger.Txn) error {
		// recover enqueued counter
		enqKey := append(q.prefix, []byte("_meta_enqueued")...)
		item, err := txn.Get(enqKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err == nil && len(val) == 8 {
				q.enqueued = binary.BigEndian.Uint64(val)
			}
		}

		// recover dequeued counter
		deqKey := append(q.prefix, []byte("_meta_dequeued")...)
		item, err = txn.Get(deqKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err == nil && len(val) == 8 {
				q.dequeued = binary.BigEndian.Uint64(val)
			}
		}

		return nil
	})
}

func (q *Queue) Close() error {
	return q.db.Close()
}

func (q *Queue) generateKey() []byte {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.enqueued++

	_ = q.db.Update(func(txn *badger.Txn) error {
		key := append(q.prefix, []byte("_meta_enqueued")...)
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, q.enqueued)
		return txn.Set(key, val)
	})

	key := make([]byte, len(q.prefix)+8)
	copy(key, q.prefix)
	binary.BigEndian.PutUint64(key[len(q.prefix):], q.enqueued)
	return key
}

func (q *Queue) Enqueue(value []byte) error {
	return q.db.Update(func(txn *badger.Txn) error {
		return txn.Set(q.generateKey(), value)
	})
}

func (q *Queue) Dequeue() ([]byte, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.dequeued >= q.enqueued {
		return nil, errors.New("queue is empty")
	}

	nextID := q.dequeued + 1
	key := make([]byte, len(q.prefix)+8)
	copy(key, q.prefix)
	binary.BigEndian.PutUint64(key[len(q.prefix):], nextID)

	var value []byte
	err := q.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		if err := txn.Delete(key); err != nil {
			return err
		}

		q.dequeued = nextID
		metaKey := append(q.prefix, []byte("_meta_dequeued")...)
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, q.dequeued)
		return txn.Set(metaKey, val)
	})

	if err != nil {
		return nil, err
	}

	return value, nil
}

func (q *Queue) Size() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.enqueued - q.dequeued
}
