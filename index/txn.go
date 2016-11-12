package index

import (
	"github.com/pkg/errors"
	"log"
)

type Transaction struct {
	*Snapshot
	db     *DB
	buffer ItemBuffer
}

const MaxBufferedItems = 1024 * 1024

var ErrCommitted = errors.New("transaction is already committed")

func (txn *Transaction) NumDocs() int {
	n := txn.buffer.NumDocs()
	for _, segment := range txn.manifest.Segments {
		n += segment.Meta.NumDocs - segment.Meta.NumDeletedDocs
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

func (txn *Transaction) Add(docID uint32, terms []uint32) error {
	if txn.Committed() {
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
	if txn.Committed() {
		return ErrCommitted
	}

	for _, segment := range txn.manifest.Segments {
		if segment.Delete(docID) {
			log.Printf("deleted doc %v from segment %v", docID, segment.ID)
		}
	}

	if txn.buffer.Delete(docID) {
		log.Printf("deleted doc %v from the transaction buffer", docID)
	}

	return nil
}

func (txn *Transaction) Update(docID uint32, terms []uint32) error {
	if txn.Committed() {
		return ErrCommitted
	}

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
	if txn.Committed() {
		return ErrCommitted
	}

	txn.buffer.Reset()
	txn.manifest.Reset()

	log.Print("removed all segments")
	return nil
}

func (txn *Transaction) Flush() error {
	if txn.Committed() {
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
	txn.buffer.Reset()

	log.Printf("flushed %v docs from the transaction buffer to segment %v", segment.Meta.NumDocs, segment.ID)
	return nil
}

func (txn *Transaction) Commit() error {
	if txn.Committed() {
		return ErrCommitted
	}

	err := txn.Flush()
	if err != nil {
		return errors.Wrap(err, "flush failed")
	}

	return txn.db.commit(txn.manifest)
}

func (txn *Transaction) Committed() bool {
	return txn.manifest.ID != 0
}
