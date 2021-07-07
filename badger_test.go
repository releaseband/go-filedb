package main

import (
	"errors"
	"github.com/dgraph-io/badger"
	"strconv"
	"testing"
	"time"
)

var errorHandlers []error

func handle(_ string, err error) {
	errorHandlers = append(errorHandlers, err)
}

type tp struct {
	cleanupTimer time.Duration
	dbDir        string
}

func newTp(errHandlerCount int) *tp {
	errorHandlers = make([]error, 0, errHandlerCount)

	return &tp{
		dbDir: ".database",
	}
}
func (p tp) getErrorsCount() int {
	return len(errorHandlers)
}

func (p tp) makeCfg() Cfg {
	return Cfg{
		CleanupTimer: 5 * time.Minute,
		ErrorHandler: handle,
		BadgerCfg:    badger.DefaultOptions(p.dbDir),
	}
}

func k(i int) string {
	return strconv.Itoa(i)
}

func v(i int) []byte {
	return []byte(k(i))
}

func Test_Badger(t *testing.T) {
	const count = 100

	tp := newTp(count)

	cfg := tp.makeCfg()
	bg, err := OpenDatabase(cfg)
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < count; i++ {
		if err = bg.Set(k(i), v(i)); err != nil {
			t.Fatal(err)
		}
	}

	var val []byte
	for i := 0; i < count; i++ {
		val, err = bg.Get(k(i))
		if err != nil {
			t.Fatal(err)
		}

		exp := string(v(i))
		got := string(val)
		if exp != got {
			t.Fatalf("exp=%s, got=%s", exp, got)
		}
	}

	const (
		listKey   = "list"
		listCount = 13
	)

	for i := 0; i < listCount; i++ {
		if err := bg.Put(listKey, k(i), v(i)); err != nil {
			t.Fatal(err)
		}
	}

	vals, err := bg.List(listKey)
	if err != nil {
		t.Fatal(err)
	}

	if len(vals) != listCount {
		t.Fatal("count invalid")
	}

	for i := 0; i < count; i++ {
		s := v(i)

		err = bg.Del(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	if tp.getErrorsCount() != 0 {
		t.Fatalf("error handler should be empty")
	}

	for i := 0; i < count; i++ {
		_, err = bg.Get(k(i))
		if !errors.Is(err, badger.ErrKeyNotFound) {
			t.Fatal("should be err: ErrKeyNotFound")
		}

		got := tp.getErrorsCount()
		exp := i + 1

		if exp != got {
			t.Fatalf("errorHandlers: errors len should be = %d, but got =%d ", exp, got)
		}
	}
}
