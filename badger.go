package main

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"strings"
	"time"
)

const decrement = "|"

type Cfg struct {
	CleanupTimer time.Duration
	BadgerCfg    badger.Options
	ErrorHandler func(mess string, err error)
}

type Badger struct {
	db           *badger.DB
	errorHandler func(mess string, err error)
}

func (b *Badger) log(mess string, err error) {
	if b.errorHandler != nil {
		b.errorHandler(mess, err)
	}
}

func (b *Badger) cleanupProc(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for range ticker.C {
		if err := b.db.RunValueLogGC(0.5); err != nil {
			b.errorHandler("RunValueLogGC failed", err)
		}
	}
}

func (b *Badger) Set(key string, val []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), val)
	})

	if err != nil {
		b.log("txnSet failed", err)
	}

	return err
}

func (b *Badger) Get(key string) ([]byte, error) {
	var resp []byte

	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("txn.Get: %w", err)
		}

		err = item.Value(func(val []byte) error {
			resp = make([]byte, len(val))
			copy(resp, val)

			return nil
		})

		return nil
	})

	if err != nil {
		b.log("txn.Get failed", err)
	}

	return resp, err
}

func (b *Badger) Del(key []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})

	if err != nil {
		b.log("txn.Delete failed", err)
	}

	return err
}

func (b Badger) makeListKey(listKey, fileKey string) string {
	return listKey + decrement + fileKey
}

func (b *Badger) Put(listKey, key string, val []byte) error {
	return b.Set(b.makeListKey(listKey, key), val)
}

func (b *Badger) List(listKey string) ([][]byte, error) {
	var resp [][]byte

	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()

			if strings.HasPrefix(string(k), listKey) {
				err := item.Value(func(v []byte) error {
					val := make([]byte, len(v))
					copy(val, v)
					resp = append(resp, val)

					return nil
				})

				if err != nil {
					return err
				}
			}
		}
		return nil
	})

	return resp, err
}


func OpenDatabase(cfg Cfg) (*Badger, error) {
	db, err := badger.Open(cfg.BadgerCfg)
	if err != nil {
		return nil, err
	}

	b := &Badger{
		db:           db,
		errorHandler: cfg.ErrorHandler,
	}

	go b.cleanupProc(cfg.CleanupTimer)

	return b, nil
}
