package index

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
)

const ManifestFilename = "manifest.json"

type Manifest struct {
	ID       uint32     `json:"id"`
	NumDocs  int        `json:"ndocs"`
	NumTerms int        `json:"nterms"`
	Checksum uint32     `json:"checksum"`
	Segments []*Segment `json:"segments"`
}

func (m *Manifest) Clone() *Manifest {
	return &Manifest{
		ID:       m.ID,
		NumDocs:  m.NumDocs,
		NumTerms: m.NumTerms,
		Checksum: m.Checksum,
		Segments: append([]*Segment{}, m.Segments...),
	}
}

type DB struct {
	dir      Dir
	mu       sync.Mutex
	manifest atomic.Value
}

func Open(dir Dir, create bool) (*DB, error) {
	var manifest Manifest
	file, err := dir.OpenFile(ManifestFilename)
	if err != nil {
		if IsNotExist(err) && create {
			log.Printf("creating new database in %v", dir)
		} else {
			return nil, err
		}
	} else {
		log.Printf("opening database %v", dir)
		err = json.NewDecoder(file).Decode(&manifest)
		if err != nil {
			return nil, err
		}
		log.Printf("manifest=%v", manifest)
		for i, segment := range manifest.Segments {
			err = segment.Open(dir)
			if err != nil {
				return nil, err
			}
			log.Printf("segment[%v]=%v", i, segment)
		}
	}

	db := &DB{dir: dir}
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
	db.mu.Lock()
	defer db.mu.Unlock()

	manifest := db.manifest.Load().(*Manifest).Clone()
	manifest.ID += 1

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

	file, err := db.dir.CreateFile("manifest.json")
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
