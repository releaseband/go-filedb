package badger

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
)

type tp struct {
	cleanupTimer time.Duration
	dbDir        string
	vlogDir      string
}

func newTp() *tp {
	return &tp{
		dbDir: ".database",
	}
}

func (p tp) makeOpt() badger.Options {
	return badger.DefaultOptions(p.dbDir)
}

func k(i int) string {
	return strconv.Itoa(i)
}

func v(i int) []byte {
	return []byte(k(i))
}

type tester struct {
	t *testing.T
}

func newT(t *testing.T) *tester {
	return &tester{t: t}
}

func (tt *tester) shouldBeNil(err error) {
	if err != nil {
		tt.t.Fatal(fmt.Errorf("error should be nil: %w", err))
	}
}

func (tt *tester) shouldEqual(exp string, bGot []byte) {
	got := string(bGot)

	if got != exp {
		tt.t.Fatalf("should be equal: exp='%s', got ='%s'", exp, got)
	}
}

func (tt *tester) shouldBeError(err, expErr error) {
	if err == nil {
		tt.t.Fatal("should be error")
	}

	if !errors.Is(err, expErr) {
		tt.t.Fatalf("exp err:'%s', got err:'%s'", expErr.Error(), err.Error())
	}
}

func Test_Badger(t *testing.T) {
	const count = 100

	tp := newTp()
	tt := newT(t)

	cfg := tp.makeOpt()
	bg, err := OpenDatabase(cfg)

	defer func() {
		err = bg.Close()
		tt.shouldBeNil(err)
	}()

	tt.shouldBeNil(err)

	for i := 0; i < count; i++ {
		err = bg.Set(k(i), v(i))
		tt.shouldBeNil(err)
	}

	var val []byte
	for i := 0; i < count; i++ {
		val, err = bg.Get(k(i))
		tt.shouldBeNil(err)

		exp := string(v(i))
		got := string(val)
		if exp != got {
			t.Fatalf("exp=%s, got=%s", exp, got)
		}
	}

	for i := 0; i < count; i++ {
		err = bg.Del(strconv.Itoa(i))
		tt.shouldBeNil(err)
	}

	for i := 0; i < count; i++ {
		_, err = bg.Get(k(i))
		tt.shouldBeError(err, ErrNotFound)
	}
}

func debugSize(mess string, lsm, vlog int64) {
	fmt.Printf("=== | %s | lsm=%d | vlog=%d | === \n", mess, lsm, vlog)
}

func getPrefix(p int) string {
	var s string
	for i := 0; i < p; i++ {
		s += "fdfdbvcvcegegegegbww"
	}

	return s
}

func getValue(prefix int) func(i int) string {
	p := getPrefix(prefix)

	return func(i int) string {
		return p + strconv.Itoa(i)
	}
}

func getPrefixExist(bg *Badger) (int, error) {
	const (
		prefixKey   = "prefix"
		startPrefix = 2
	)

	set := func(val int) error {
		return bg.Set(prefixKey, []byte(strconv.Itoa(val)))
	}

	val, err := bg.Get(prefixKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return startPrefix, set(startPrefix)
		} else {
			return 0, err
		}
	}

	i, err := strconv.Atoi(string(val))
	if err != nil {
		return 0, err
	}

	i += 3

	return i, set(i)

}

func TestBadger_CleanUp(t *testing.T) {
	const count = 1000

	tp := newTp()
	tt := newT(t)

	cfg := tp.makeOpt()
	bg, err := OpenDatabase(cfg)
	tt.shouldBeNil(err)
	defer func() {
		err = bg.Close()
		tt.shouldBeNil(err)
	}()

	lsm, vlog := bg.Size()

	prefix, err := getPrefixExist(bg)
	tt.shouldBeNil(err)

	getValue := getValue(prefix)

	debugSize("before", lsm, vlog)

	for i := 0; i < count; i++ {
		value := getValue(i)
		tt.shouldBeNil(bg.Set(k(i), []byte(value)))
	}

	lsm, vlog = bg.Size()
	debugSize("added: ", lsm, vlog)

	err = bg.CleanUp()
	lsm, vlog = bg.Size()
	debugSize("cleanUp", lsm, vlog)
	tt.shouldBeError(err, badger.ErrNoRewrite)
}

func TestBadger_Size(t *testing.T) {
	const count = 1000
	tp := newTp()
	tt := newT(t)

	cfg := tp.makeOpt()
	bg, err := OpenDatabase(cfg)
	tt.shouldBeNil(err)

	defer func() {
		err = bg.Close()
		tt.shouldBeNil(err)
	}()

	fmt.Println("size")
	fmt.Println(bg.Size())

	for i := 0; i < count; i++ {
		tt.shouldBeNil(bg.Del(strconv.Itoa(i)))
	}

	fmt.Println("size after delete")
	fmt.Println(bg.Size())

}

func TestBadger_list(t *testing.T) {
	const (
		count   = 100
		listKey = "qwerty"
	)

	tp := newTp()
	tt := newT(t)

	cfg := tp.makeOpt()
	bg, err := OpenDatabase(cfg)
	tt.shouldBeNil(err)

	for i := 0; i < count; i++ {
		err = bg.AddToGroup(listKey, k(i), v(i))
		tt.shouldBeNil(err)
	}

	list, err := bg.GetGroup(listKey, 127)
	tt.shouldBeNil(err)

	if len(list) != count {
		t.Fatal("count invalid")
	}

	for key := range list {
		if !strings.HasPrefix(key, listKey) {
			t.Fatal("range failed: key is invalid")
		}
	}

	for key := range list {
		tt.shouldBeNil(bg.DeleteFromGroup(key))
	}

	list, err = bg.GetGroup(listKey, 127)
	tt.shouldBeNil(err)
	if len(list) != 0 {
		t.Fatal("finished list rang is invalid, func delete failed")
	}
}