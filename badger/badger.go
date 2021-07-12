package badger

import (
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"time"
)

const (
	decrement                  = "|"
	defaultCLeaUpDiscardRation = 0.5
)

var (
	ErrInvalidRangeLimit = errors.New("invalid range limit")
	ErrNotFound          = errors.New("key not found")
)


func  makeListKey(listKey, fileKey string) string {
	return listKey + decrement + fileKey
}

type Cfg struct {
	CleanupTimer time.Duration
	BadgerCfg    badger.Options
}

type Badger struct {
	db           *badger.DB
	errorHandler func(mess string, err error)
}

func (b *Badger) Close() error {
	return b.db.Close()
}

func (b *Badger) Size() (lsm int64, vlog int64) {
	return b.db.Size()
}

func (b *Badger) log(mess string, err error) {
	if b.errorHandler != nil {
		b.errorHandler(mess, err)
	}
}

func (b *Badger) CleanUp() error {
	return b.db.RunValueLogGC(defaultCLeaUpDiscardRation)
}

func (b *Badger) RunCleanupProc(d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for range ticker.C {
	again:
		if err := b.CleanUp(); err == nil {
			goto again
		}
	}
}

func (b *Badger) Set(key string, val []byte) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), val)
	})
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

		if errors.Is(err, badger.ErrKeyNotFound) {
			err = ErrNotFound
		}
	}

	return resp, err
}

func (b *Badger) Del(key string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (b *Badger) DeleteFromGroup(listKey, fileKey string) error {
	return b.Del(makeListKey(listKey, fileKey))
}


func (b *Badger) Push(prefixKey, key string, val []byte) error {
	return b.Set(makeListKey(prefixKey, key), val)
}

func (b *Badger) Range(prefixKey string, limit int8) ([][]byte, error) {
	var resp [][]byte

	if limit <= 0 {
		return nil, ErrInvalidRangeLimit
	}

	err := b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte(prefixKey)

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				valCopy := make([]byte, len(val))
				copy(val, valCopy)
				resp = append(resp, val)

				return nil
			})

			if err != nil {
				return fmt.Errorf("item.Value: %w", err)
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

	return &Badger{
		db:           db,
	}, nil
}
