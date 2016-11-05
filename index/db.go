package index

import (
	"encoding/json"
	"github.com/acoustid/go-acoustid/util/vfs"
	"log"
	"sync"
	"sync/atomic"
)

const ManifestFilename = "manifest.json"

type Manifest struct {
	ID        uint32     `json:"id"`
	NumDocs   int        `json:"ndocs"`
	NumValues int        `json:"nvalues"`
	Checksum  uint32     `json:"checksum"`
	Segments  []*Segment `json:"segments"`
}

func (m *Manifest) Clone() *Manifest {
	return &Manifest{
		ID:        m.ID,
		NumDocs:   m.NumDocs,
		NumValues: m.NumValues,
		Checksum:  m.Checksum,
		Segments:  append([]*Segment{}, m.Segments...),
	}
}

func (m *Manifest) AddSegment(s *Segment) {
	m.NumDocs += s.Meta.NumDocs
	m.NumValues += s.Meta.NumValues
	m.Checksum += s.Meta.Checksum
	m.Segments = append(m.Segments, s)
}

func (m *Manifest) RemoveSegment(s *Segment) {
	segments := m.Segments[:0]
	for _, s2 := range m.Segments {
		if s2 == s {
			m.NumDocs -= s2.Meta.NumDocs
			m.NumValues -= s2.Meta.NumValues
			m.Checksum -= s2.Meta.Checksum
		} else {
			segments = append(segments, s2)
		}
	}
	m.Segments = segments
}

type DB struct {
	fs       vfs.FileSystem
	mu       sync.Mutex
	txid     uint32
	manifest atomic.Value
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
}

func (db *DB) Add(docid uint32, hashes []uint32) error {
	txn := db.newTransaction()
	defer txn.Close()

	err := txn.AddDoc(docid, hashes)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *DB) Search(query []uint32) (map[uint32]int, error) {
	snapshot := db.newSnapshot()
	defer snapshot.Close()
	return snapshot.Search(query)
}

func (db *DB) newSnapshot() *Snapshot {
	return &Snapshot{
		db:       db,
		manifest: db.manifest.Load().(*Manifest),
	}
}

func (db *DB) newTransaction() *Transaction {
	manifest := db.manifest.Load().(*Manifest).Clone()
	manifest.ID = atomic.AddUint32(&db.txid, 1)

	log.Printf("started transaction %d", manifest.ID)

	return &Transaction{
		Snapshot: Snapshot{
			db:       db,
			manifest: manifest,
		},
	}
}

func (db *DB) commit(txn *Transaction) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	file, err := db.fs.CreateAtomicFile("manifest.json")
	if err != nil {
		return err
	}
	defer file.Close()

	manifest := txn.manifest

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(manifest)
	if err != nil {
		return err
	}

	err = file.Commit()
	if err != nil {
		return err
	}

	db.manifest.Store(manifest)

	log.Printf("committed transaction %d (docs=%v, segments=%v, checksum=0x%08x)",
		manifest.ID, manifest.NumDocs, len(manifest.Segments), manifest.Checksum)

	return nil
}
