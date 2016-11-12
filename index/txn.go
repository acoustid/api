package index

import (
	"github.com/pkg/errors"
	"log"
)

type Transaction struct {
	*Snapshot
	db        *DB
	committed bool
	counter   uint8
	buffer    ItemBuffer
}

const MaxBufferedItems = 1024 * 1024

var (
	ErrCommitted = errors.New("transaction is already committed")
)

func (txn *Transaction) Add(docID uint32, terms []uint32) error {
	if txn.committed {
		return ErrCommitted
	}

	txn.buffer.Add(docID, terms)
	log.Printf("added doc %v to the transaction buffer", docID)

	if txn.buffer.NumItems() > MaxBufferedItems {
		err := txn.Flush()
		if err != nil {
			return errors.Wrap(err, "flush failed")
		}
	}

	return nil
}

func (txn *Transaction) Delete(docID uint32) error {
	if txn.committed {
		return ErrCommitted
	}

	for _, segment := range txn.manifest.Segments {
		if segment.Delete(docID) {
			log.Printf("found %v in segment %v", docID, segment.ID)
		}
	}

	if txn.buffer.Delete(docID) {
		log.Printf("deleted doc %v from the transaction buffer", docID)
	}

	return nil
}

func (txn *Transaction) Update(docID uint32, terms []uint32) error {
	err := txn.Delete(docID)
	if err != nil {
		return errors.Wrap(err, "delete failed")
	}
	err = txn.Add(docID, terms)
	if err != nil {
		return errors.Wrap(err, "add failed")
	}
	return nil
}

func (txn *Transaction) Truncate() error {
	txn.buffer.Reset()
	txn.manifest.Segments = txn.manifest.Segments[:0]
	txn.manifest.NumDocs = 0
	txn.manifest.NumItems = 0
	return nil
}

func (txn *Transaction) NumDocs() int {
	n := txn.buffer.NumDocs()
	for _, segment := range txn.manifest.Segments {
		n += segment.Meta.NumDocs
	}
	return n
}

func (txn *Transaction) NumItems() int {
	n := txn.buffer.NumItems()
	for _, segment := range txn.manifest.Segments {
		n += segment.Meta.NumItems
	}
	return n
}

func (txn *Transaction) Flush() error {
	if txn.committed {
		return ErrCommitted
	}

	if txn.buffer.Empty() {
		return nil
	}

	segment, err := txn.db.createSegment(txn.buffer.Reader())
	if err != nil {
		return errors.Wrap(err, "failed to create a new segment")
	}

	txn.manifest.AddSegment(segment)
	log.Printf("flushed %v docs from the transaction buffer to segment %v", txn.buffer.NumDocs(), segment.ID)

	txn.buffer.Reset()

	return nil
}

func (txn *Transaction) Commit() error {
	if txn.committed {
		return ErrCommitted
	}

	err := txn.Flush()
	if err != nil {
		return errors.Wrap(err, "flush failed")
	}

	err = txn.db.commit(txn.manifest)
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	txn.committed = true
	return nil
}
