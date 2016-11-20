// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"go4.org/syncutil"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"sync/atomic"
)

var debugLog = log.New(ioutil.Discard, "", log.LstdFlags)

var ErrAlreadyClosed = errors.New("already closed")

type DB struct {
	fs              vfs.FileSystem
	mu              sync.RWMutex
	wlock           io.Closer
	txid            uint32
	manifest        atomic.Value
	closed          bool
	numSnapshots    int64
	numTransactions int64
	refs            map[string]int
	orphanedFiles   chan string
	bg              syncutil.Group
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

	db := &DB{fs: fs}
	db.init(&manifest)
	return db, nil
}

func (db *DB) init(manifest *Manifest) {
	db.txid = manifest.ID
	db.manifest.Store(manifest)

	db.refs = make(map[string]int)
	db.incFileRefs(manifest)

	db.orphanedFiles = make(chan string, 16)
	db.bg.Go(db.deleteOrphanedFiles)
}

func (db *DB) deleteOrphanedFiles() error {
	for name := range db.orphanedFiles {
		err := db.fs.Remove(name)
		if err != nil {
			log.Printf("[ERROR] failed to delete file %q: %v", name, err)
		} else {
			debugLog.Printf("deleted file %q", name)
		}
	}
	return nil
}

func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()

	close(db.orphanedFiles)
	db.bg.Wait()

	if db.wlock != nil {
		db.wlock.Close()
		db.wlock = nil
		log.Println("released write lock")
	}

	db.closed = true
}

// Note: This must be called under a locked mutex.
func (db *DB) incFileRefs(m *Manifest) {
	for _, segment := range m.Segments {
		for _, name := range segment.fileNames() {
			db.refs[name]++
		}
	}
}

// Note: This must be called under a locked mutex.
func (db *DB) decFileRefs(m *Manifest) {
	for _, segment := range m.Segments {
		for _, name := range segment.fileNames() {
			db.refs[name]--
			if db.refs[name] <= 0 {
				log.Printf("file %q is no longer needed", name)
				db.orphanedFiles <- name
			}
		}
	}
}

func (db *DB) Add(docID uint32, hashes []uint32) error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.Add(docID, hashes) })
}

func (db *DB) Delete(docID uint32) error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.Delete(docID) })
}

func (db *DB) DeleteAll() error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.DeleteAll() })
}

func (db *DB) Import(stream ItemReader) error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.Import(stream) })
}

func (db *DB) Compact() error {
	return db.RunInTransaction(func(txn BulkWriter) error { return txn.(*Transaction).compact() })
}

func (db *DB) Search(query []uint32) (map[uint32]int, error) {
	snapshot := db.newSnapshot()
	defer snapshot.Close()
	return snapshot.Search(query)
}

// Snapshot creates a consistent read-only view of the DB.
func (db *DB) Snapshot() Searcher {
	return db.newSnapshot()
}

func (db *DB) closeTransaction(tx *Transaction) error {
	numTransactions := atomic.AddInt64(&db.numTransactions, -1)
	if numTransactions == 0 {
		debugLog.Printf("closed transaction %p", tx)
	} else {
		debugLog.Printf("closed transaction %p, %v transactions still open", tx, numTransactions)
	}
	return nil
}

// Transaction starts a new write transaction. You need to explicitly call Commit for the changes to be applied.
func (db *DB) Transaction() (BulkWriter, error) {
	snapshot := db.newSnapshot()

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, ErrAlreadyClosed
	}

	if db.wlock == nil {
		lock, err := db.fs.Lock("write.lock")
		if err != nil {
			return nil, errors.Wrap(err, "unable to acquire write lock")
		}
		log.Println("acquired write lock")
		db.wlock = lock
	}

	tx := &Transaction{snapshot: snapshot, db: db, closeFn: db.closeTransaction}
	tx.init()

	atomic.AddInt64(&db.numTransactions, 1)

	debugLog.Printf("created transaction %p (base=%v)", tx, tx.snapshot.manifest.ID)

	return tx, nil
}

// RunInTransaction executes the given function in a transaction. If the function does not return an error,
// the transaction will be automatically committed.
func (db *DB) RunInTransaction(fn func(txn BulkWriter) error) error {
	txn, err := db.Transaction()
	if err != nil {
		return err
	}
	defer txn.Close()

	err = fn(txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *DB) closeSnapshot(snapshot *Snapshot) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	db.decFileRefs(snapshot.manifest)

	numSnapshots := atomic.AddInt64(&db.numSnapshots, -1)
	if numSnapshots == 0 {
		debugLog.Printf("closed snapshot %p (%v)", snapshot, snapshot.manifest.ID)
	} else {
		debugLog.Printf("closed snapshot %p (%v), %v snapshots still open", snapshot, snapshot.manifest.ID, numSnapshots)
	}

	return nil
}

func (db *DB) newSnapshot() *Snapshot {
	db.mu.RLock()
	defer db.mu.RUnlock()

	snapshot := &Snapshot{
		manifest: db.manifest.Load().(*Manifest),
		closeFn:  db.closeSnapshot,
	}

	db.incFileRefs(snapshot.manifest)
	atomic.AddInt64(&db.numSnapshots, 1)

	debugLog.Printf("created snapshot %p (id=%v)", snapshot, snapshot.manifest.ID)

	return snapshot
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

	if !manifest.HasChanges() {
		return nil
	}

	id := atomic.AddUint32(&db.txid, 1)
	base := db.manifest.Load().(*Manifest)

	err := manifest.Commit(db.fs, id, base)
	if err != nil {
		return errors.Wrap(err, "manifest commit failed")
	}

	db.incFileRefs(manifest)
	db.decFileRefs(base)

	db.manifest.Store(manifest)

	log.Printf("committed transaction %d (docs=%v, items=%v, segments=%v, checksum=%d)",
		manifest.ID, manifest.NumDocs-manifest.NumDeletedDocs, manifest.NumItems, len(manifest.Segments), manifest.Checksum)

	return nil
}

func (db *DB) Reader() ItemReader {
	var readers []ItemReader
	manifest := db.manifest.Load().(*Manifest)
	for _, segment := range manifest.Segments {
		readers = append(readers, segment.Reader())
	}
	return MergeItemReaders(readers...)
}

func (db *DB) NumSegments() int {
	manifest := db.manifest.Load().(*Manifest)
	return len(manifest.Segments)
}

func (db *DB) NumDocs() int {
	manifest := db.manifest.Load().(*Manifest)
	return manifest.NumDocs
}

func (db *DB) NumDeletedDocs() int {
	manifest := db.manifest.Load().(*Manifest)
	return manifest.NumDeletedDocs
}

// Contains returns true if the DB contains the given docID.
func (db *DB) Contains(docID uint32) bool {
	manifest := db.manifest.Load().(*Manifest)
	for _, segment := range manifest.Segments {
		if segment.Contains(docID) {
			return true
		}
	}
	return false
}
