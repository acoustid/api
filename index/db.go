package index

import (
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"log"
	"sync"
	"sync/atomic"
)

var ErrAlreadyClosed = errors.New("already closed")

type DB struct {
	fs       vfs.FileSystem
	mu       sync.Mutex
	txid     uint32
	manifest atomic.Value
	closed   bool
}

func Open(fs vfs.FileSystem, create bool) (*DB, error) {
	var manifest Manifest
	err := manifest.Load(fs, create)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open the manifest")
	}

	for _, segment := range manifest.Segments {
		err = segment.Open(fs)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to open segment %v", segment.ID)
		}
	}

	db := &DB{fs: fs, txid: manifest.ID}
	db.manifest.Store(&manifest)
	return db, nil
}

func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
}

func (db *DB) Add(docID uint32, hashes []uint32) error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.Add(docID, hashes) })
}

func (db *DB) Update(docID uint32, hashes []uint32) error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.Update(docID, hashes) })
}

func (db *DB) Delete(docID uint32) error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.Delete(docID) })
}

func (db *DB) Truncate() error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.Truncate() })
}

func (db *DB) Search(query []uint32) (map[uint32]int, error) {
	snapshot := db.newSnapshot(false)
	defer snapshot.Close()
	return snapshot.Search(query)
}

// Snapshot creates a consistent read-only view of the DB.
func (db *DB) Snapshot() Searcher {
	return db.newSnapshot(false)
}

// Transaction starts a new write transaction. You need to explicitly call Commit for the changes to be applied.
func (db *DB) Transaction() BulkWriter {
	return &Transaction{Snapshot: db.newSnapshot(true), db: db}
}

// RunInTransaction executes the given function in a transaction. If the function does not return an error,
// the transaction will be automatically committed.
func (db *DB) RunInTransaction(fn func(txn BulkWriter) error) error {
	txn := db.Transaction()
	defer txn.Close()

	err := fn(txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *DB) newSnapshot(write bool) *Snapshot {
	manifest := db.manifest.Load().(*Manifest)
	if write {
		manifest = manifest.Clone()
		manifest.ID = 0
	}
	return &Snapshot{manifest: manifest}
}

func (db *DB) createSegment(input ItemReader) (*Segment, error) {
	return CreateSegment(db.fs, atomic.AddUint32(&db.txid, 1), input)
}

func (db *DB) commit(manifest *Manifest) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrAlreadyClosed
	}

	manifest.ID = atomic.AddUint32(&db.txid, 1)

	for _, segment := range manifest.Segments {
		err := segment.SaveUpdate(db.fs, manifest.ID)
		if err != nil {
			manifest.ID = 0
			return errors.Wrap(err, "failed to save segment update")
		}
	}

	err := manifest.Save(db.fs)
	if err != nil {
		manifest.ID = 0
		return errors.Wrap(err, "failed to save manifest")
	}

	db.manifest.Store(manifest)

	log.Printf("committed transaction %d (docs=%v, segments=%v, checksum=0x%08x)",
		manifest.ID, manifest.NumDocs, len(manifest.Segments), manifest.Checksum)

	return nil
}
