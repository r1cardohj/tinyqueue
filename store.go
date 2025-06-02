package main

import (
	"encoding/binary"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger"
)

type Queue struct {
	db       *badger.DB
	prefix   []byte
	enqueued uint64
	dequeued uint64
}

func NewQueue(path string, prefix []byte, safe bool) (*Queue, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = safe // async is not safe but terrible fast
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	q := &Queue{
		db:     db,
		prefix: prefix,
	}

	err = q.restoreCounters()
	if err != nil {
		db.Close()
		return nil, err
	}

	log.Println("queue initialized with prefix:", string(q.prefix))
	log.Println("enqueued items:", atomic.LoadUint64(&q.enqueued))
	log.Println("dequeued items:", atomic.LoadUint64(&q.dequeued))
	log.Println("current size is", q.Size())

	return q, nil
}

func (q *Queue) restoreCounters() error {
	return q.db.View(func(txn *badger.Txn) error {
		// restore enqueued and dequeued counters
		enqKey := append(q.prefix, []byte("_enq")...)
		item, err := txn.Get(enqKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err == nil && len(val) == 8 {
				atomic.StoreUint64(&q.enqueued, binary.BigEndian.Uint64(val))
			}
		}

		deqKey := append(q.prefix, []byte("_deq")...)
		item, err = txn.Get(deqKey)
		if err == nil {
			val, err := item.ValueCopy(nil)
			if err == nil && len(val) == 8 {
				atomic.StoreUint64(&q.dequeued, binary.BigEndian.Uint64(val))
			}
		}
		return nil
	})
}

func (q *Queue) Enqueue(value []byte) error {
	id := atomic.AddUint64(&q.enqueued, 1)
	key := make([]byte, len(q.prefix)+8)
	copy(key, q.prefix)
	binary.BigEndian.PutUint64(key[len(q.prefix):], id)

	return q.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (q *Queue) Dequeue() ([]byte, error) {
	for {
		id := atomic.LoadUint64(&q.dequeued) + 1
		if id > atomic.LoadUint64(&q.enqueued) {
			return nil, errors.New("queue is empty")
		}
		key := make([]byte, len(q.prefix)+8)
		copy(key, q.prefix)
		binary.BigEndian.PutUint64(key[len(q.prefix):], id)

		var value []byte
		err := q.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			value, err = item.ValueCopy(nil)
			return err
		})
		if err == nil {
			// when successfully dequeued, increment the dequeued counter
			atomic.AddUint64(&q.dequeued, 1)

			return value, nil

		}
		if err == badger.ErrKeyNotFound {
			// if the key is not found, it means the item was already dequeued
			// or deleted by GC, so we wait a bit and retry
			time.Sleep(10 * time.Microsecond)
			continue
		}

		return nil, err
	}
}

func (q *Queue) Size() uint64 {
	return atomic.LoadUint64(&q.enqueued) - atomic.LoadUint64(&q.dequeued)
}

func (q *Queue) Close() error {
	// save enqueued and dequeued counters
	err := q.db.Update(func(txn *badger.Txn) error {
		enqKey := append(q.prefix, []byte("_enq")...)
		enqVal := make([]byte, 8)
		binary.BigEndian.PutUint64(enqVal, atomic.LoadUint64(&q.enqueued))
		if err := txn.Set(enqKey, enqVal); err != nil {
			return err
		}

		deqKey := append(q.prefix, []byte("_deq")...)
		deqVal := make([]byte, 8)
		binary.BigEndian.PutUint64(deqVal, atomic.LoadUint64(&q.dequeued))
		return txn.Set(deqKey, deqVal)
	})

	if err != nil {
		return err
	}

	if err := q.db.Sync(); err != nil {
		return err
	}

	return q.db.Close()
}
