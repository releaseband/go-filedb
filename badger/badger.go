package badger

import (
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v3"
)

const (
	keysListPrefix             = "keys"
	decrement                  = "|"
	defaultCLeaUpDiscardRation = 0.5
)

var (
	ErrInvalidRangeLimit = errors.New("invalid range limit")
	ErrNotFound          = errors.New("key not found")
)

func groupID(groupKey, fileKey string) string {
	return groupKey + decrement + fileKey
}

func keysListID(groupID string) string {
	return keysListPrefix + decrement + groupID
}

type Badger struct {
	db *badger.DB
}

func (b *Badger) Close() error {
	return b.db.Close()
}

func (b *Badger) Size() (lsm int64, vlog int64) {
	return b.db.Size()
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
		if errors.Is(err, badger.ErrKeyNotFound) {
			err = ErrNotFound
		}
	}

	return resp, err
}

func _delete(txn *badger.Txn, key string) error {
	return txn.Delete([]byte(key))
}

func (b *Badger) Del(key string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return _delete(txn, key)
	})
}

func (b *Badger) DeleteFromGroup(groupID string) error {
	return b.db.Update(func(txn *badger.Txn) error {
		if err := _delete(txn, groupID); err != nil {
			return err
		}

		if err := _delete(txn, keysListID(groupID)); err != nil {
			return err
		}

		return nil
	})
}

func (b *Badger) saveKey(key string) error {
	return b.Set(keysListID(key), []byte(key))
}

func (b *Badger) AddToGroup(groupName, key string, val []byte) error {
	listKey := groupID(groupName, key)

	if err := b.saveKey(listKey); err != nil {
		return fmt.Errorf("saveKey: %w", err)
	}

	return b.Set(listKey, val)
}

func getKeys(txn *badger.Txn, listName string) ([]string, error) {
	var resp []string

	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	prefix := []byte(keysListID(listName))

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()

		err := item.Value(func(val []byte) error {
			resp = append(resp, string(val))

			return nil
		})

		if err != nil {
			return nil, fmt.Errorf("item.Value: %w", err)
		}
	}

	return resp, nil
}

func (b *Badger) GetGroup(groupName string, limit int8) (map[string][]byte, error) {
	var resp map[string][]byte

	if limit <= 0 {
		return nil, ErrInvalidRangeLimit
	}

	err := b.db.View(func(txn *badger.Txn) error {
		keys, err := getKeys(txn, groupName)
		if err != nil {
			return fmt.Errorf("getKeys: %w", err)
		}

		resp = make(map[string][]byte, len(keys))

		for _, k := range keys {
			val, err := b.Get(k)
			if err != nil {
				return fmt.Errorf("get(%s) failed: %w", k, err)
			}

			resp[k] = val
		}

		return nil
	})

	return resp, err
}

func OpenDatabase(opt badger.Options) (*Badger, error) {
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	return &Badger{
		db: db,
	}, nil
}
