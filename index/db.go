package index

import (
	"encoding/json"
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"log"
	"sync"
	"sync/atomic"
)

const ManifestFilename = "manifest.json"

var ErrAlreadyClosed = errors.New("already closed")

type Manifest struct {
	ID       uint32     `json:"id"`
	NumDocs  int        `json:"ndocs"`
	NumItems int        `json:"nitems"`
	Checksum uint32     `json:"checksum"`
	Segments []*Segment `json:"segments"`
}

func (m *Manifest) Clone() *Manifest {
	return &Manifest{
		ID:       m.ID,
		NumDocs:  m.NumDocs,
		NumItems: m.NumItems,
		Checksum: m.Checksum,
		Segments: append([]*Segment{}, m.Segments...),
	}
}

func (m *Manifest) AddSegment(s *Segment) {
	m.NumDocs += s.Meta.NumDocs
	m.NumItems += s.Meta.NumItems
	m.Checksum += s.Meta.Checksum
	m.Segments = append(m.Segments, s)
}

func (m *Manifest) RemoveSegment(s *Segment) {
	segments := m.Segments[:0]
	for _, s2 := range m.Segments {
		if s2 == s {
			m.NumDocs -= s2.Meta.NumDocs
			m.NumItems -= s2.Meta.NumItems
			m.Checksum -= s2.Meta.Checksum
		} else {
			segments = append(segments, s2)
		}
	}
	m.Segments = segments
}

func (m *Manifest) Save(fs vfs.FileSystem, name string) error {
	file, err := fs.CreateAtomicFile(name)
	if err != nil {
		return errors.Wrap(err, "create failed")
	}
	defer file.Close()

	encoded, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return errors.Wrap(err, "JSON serialization failed")
	}

	_, err = file.Write(encoded)
	if err != nil {
		return errors.Wrap(err, "write failed")
	}

	err = file.Commit()
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	return nil
}

type DB struct {
	fs       vfs.FileSystem
	mu       sync.Mutex
	txid     uint32
	manifest atomic.Value
	closed   bool
}

func Open(fs vfs.FileSystem, create bool) (*DB, error) {
	var manifest Manifest
	file, err := fs.OpenFile(ManifestFilename)
	if err != nil {
		if vfs.IsNotExist(err) && create {
			log.Printf("creating new database in %v", fs)
		} else {
			return nil, err
		}
	} else {
		log.Printf("opening database %v", fs)
		err = json.NewDecoder(file).Decode(&manifest)
		if err != nil {
			return nil, err
		}
		log.Printf("manifest=%v", manifest)
		for i, segment := range manifest.Segments {
			err = segment.Open(fs)
			if err != nil {
				return nil, err
			}
			log.Printf("segment[%v]=%v", i, segment)
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
	return db.RunInTransaction(func(txn *Transaction) error { return txn.Add(docID, hashes) })
}

func (db *DB) Search(query []uint32) (map[uint32]int, error) {
	snapshot := db.newSnapshot(false)
	defer snapshot.Close()
	return snapshot.Search(query)
}

// Snapshot creates a consistent read-only view of the DB.
func (db *DB) Snapshot() *Snapshot {
	return db.newSnapshot(false)
}

// Transaction starts a new write transaction. You need to explicitly call Commit for the changes to be applied.
func (db *DB) Transaction() *Transaction {
	return &Transaction{Snapshot: db.newSnapshot(true), fs: db.fs, commitFn: db.commit}
}

// RunInTransaction executes the given function in a transaction. If the function does not return an error,
// the transaction will be automatically committed.
func (db *DB) RunInTransaction(fn func(txn *Transaction) error) error {
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
		manifest.ID = atomic.AddUint32(&db.txid, 1)
		log.Printf("started transaction %d", manifest.ID)
	}
	return &Snapshot{manifest: manifest}
}

func (db *DB) commit(manifest *Manifest) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return ErrAlreadyClosed
	}

	err := manifest.Save(db.fs, ManifestFilename)
	if err != nil {
		return errors.Wrap(err, "failed to save manifest")
	}

	db.manifest.Store(manifest)

	log.Printf("committed transaction %d (docs=%v, segments=%v, checksum=0x%08x)",
		manifest.ID, manifest.NumDocs, len(manifest.Segments), manifest.Checksum)

	return nil
}
