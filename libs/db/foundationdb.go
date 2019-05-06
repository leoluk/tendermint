package db

import (
	"fmt"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"sync"
)

func init() {
	registerDBCreator(FoundationDBBackend, func(name, dir string) (DB, error) {
		return NewFoundationDB(dir, name), nil
	}, false)
}

var _ DB = (*FoundationDB)(nil)

type FoundationDB struct {
	mtx sync.Mutex
	db  *fdb.Database
	dir directory.DirectorySubspace
	sub subspace.Subspace
}

func NewFoundationDB(dbName string, clusterfile string) *FoundationDB {
	fdb.MustAPIVersion(600)
	db := fdb.MustOpen(clusterfile, []byte("DB"))

	dir, err := directory.CreateOrOpen(db, []string{"tm"}, nil)
	if err != nil {
		panic(err)
	}

	database := &FoundationDB{
		db:  &db,
		dir: dir,
		sub: dir.Sub(tuple.Tuple{dbName}),
	}

	return database
}

// Implements DB.
func (db *FoundationDB) Get(key []byte) []byte {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	key = nonNilBytes(key)

	value, err := db.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		return tr.Get(db.sub.Pack(tuple.Tuple{key})).MustGet(), nil
	})
	if err != nil {
		panic(err)
	}

	return value.([]byte)
}

// Implements DB.
func (db *FoundationDB) Has(key []byte) bool {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	return db.Get(key) == nil
}

// Implements DB.
func (db *FoundationDB) Set(key []byte, value []byte) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	key = nonNilBytes(key)

	_, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(db.sub.Pack(tuple.Tuple{key}), value)
		return nil, nil
	})
	if err != nil {
		panic(err)
	}

	return
}

// Implements DB.
func (db *FoundationDB) SetSync(key []byte, value []byte) {
	// Writes are always synchronous - FoundationDB is strongly consistent
	db.Set(key, value)
}

// Implements DB.
func (db *FoundationDB) Delete(key []byte) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	key = nonNilBytes(key)

	_, err := db.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Clear(db.sub.Pack(tuple.Tuple{key}))
		return nil, nil
	})
	if err != nil {
		panic(err)
	}

	return
}

// Implements DB.
func (db *FoundationDB) DeleteSync(key []byte) {
	// Writes are always synchronous - FoundationDB is strongly consistent
	db.Delete(key)
}

// Implements DB.
func (db *FoundationDB) Close() {
	// no-op
}

// Implements DB.
func (db *FoundationDB) Print() {
	panic("not implemented")
}

// Implements DB.
func (db *FoundationDB) Stats() map[string]string {
	panic("not implemented")
}

//----------------------------------------
// Iterator

// Implements DB.
func (db *FoundationDB) Iterator(start, end []byte) Iterator {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	// FoundationDB expects clients to retry transactions when they conflict,
	// so we would usually want to wrap and retry them. However, the DB contract
	// specifies that no concurrent writes happen during iteration, so we can
	// just create a transaction and panic on conflicts.

	if start == nil {
		panic("start nil")
	}
	if end == nil {
		panic("end nil")
	}

	tx, err := db.db.CreateTransaction()
	if err != nil {
		panic(err)
	}

	itr := tx.GetRange(tuple.Tuple{start, end}, fdb.RangeOptions{}).Iterator()

	return &fdbDBIterator{
		tx:    &tx,
		itr:   itr,
		start: start,
		end:   end,
	}
}

// Implements DB.
func (db *FoundationDB) ReverseIterator(start, end []byte) Iterator {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	tx, err := db.db.CreateTransaction()
	if err != nil {
		panic(err)
	}

	itr := tx.GetRange(tuple.Tuple{start, end}, fdb.RangeOptions{Reverse: true}).Iterator()

	return &fdbDBIterator{
		tx:    &tx,
		itr:   itr,
		start: start,
		end:   end,
	}
}

type fdbDBIterator struct {
	tx    *fdb.Transaction
	itr   *fdb.RangeIterator
	start []byte
	end   []byte
}

// Compile-time interface assertion
var _ Iterator = (*fdbDBIterator)(nil)

// Implements Iterator.
func (itr *fdbDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

// Implements Iterator.
func (itr *fdbDBIterator) Valid() bool {
	return itr.itr.Advance()
}

// Implements Iterator.
func (itr *fdbDBIterator) Next() {
	// FoundationDB iterators are lazy, so you can't tell whether the cursor
	// is valid without actually advancing it. This means we need to advance
	// it in Valid() and make this a no-op.
}

// Implements Iterator.
func (itr *fdbDBIterator) Key() []byte {
	return itr.itr.MustGet().Key
}

// Implements Iterator.
func (itr *fdbDBIterator) Value() []byte {
	return itr.itr.MustGet().Value
}

// Implements Iterator.
func (itr *fdbDBIterator) Close() {
	itr.tx.Commit().MustGet()
}

//----------------------------------------
// Batch

// Implements DB.
func (db *FoundationDB) NewBatch() Batch {
	// We cannot use transactions to implement batching due to the
	// transaction byte size limit, so we need to buffer operations and
	// apply them piecewise.
	return &fdbBatch{db: db}
}

type fdbBatch struct {
	ops []operation
	db  *FoundationDB
	mtx sync.Mutex
}

// Implements Batch
func (b *fdbBatch) Set(key, value []byte) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.ops = append(b.ops, operation{opTypeSet, key, value})
}

func (b *fdbBatch) Delete(key []byte) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.ops = append(b.ops, operation{opTypeDelete, key, nil})
}

// Implements Batch.
func (b *fdbBatch) Write() {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	var bytes int

	tx, err := b.db.db.CreateTransaction()
	if err != nil {
		panic(err)
	}

	for _, op := range b.ops {
		switch op.opType {
		case opTypeSet:
			tx.Set(b.db.sub.Pack(tuple.Tuple{op.key}), op.value)
		case opTypeDelete:
			tx.Clear(b.db.sub.Pack(tuple.Tuple{op.key}))
		}

		bytes += len(op.key) + len(op.value)

		if bytes >= 3*1000000 /* MiB */ {
			fmt.Println("commit")
			tx.Commit().MustGet()
			bytes = 0
			tx, err = b.db.db.CreateTransaction()
			if err != nil {
				panic(err)
			}
		}
	}

	tx.Commit().MustGet()
}

// Implements Batch.
func (b *fdbBatch) WriteSync() {
	b.Write()
}

// Implements Batch.
func (b *fdbBatch) Close() {
}
