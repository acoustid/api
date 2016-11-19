// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

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

func (txn *Transaction) Add(docID uint32, terms []uint32) error {
	if txn.Committed() {
		return ErrCommitted
	}

	if txn.buffer.Delete(docID) {
		log.Printf("deleted doc %v from the transaction buffer", docID)
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

	if txn.buffer.Delete(docID) {
		log.Printf("deleted doc %v from the transaction buffer", docID)
	}

	txn.manifest.Delete(docID)

	return nil
}

func (txn *Transaction) DeleteAll() error {
	if txn.Committed() {
		return ErrCommitted
	}

	txn.buffer.Reset()
	txn.manifest.DeleteAll()

	log.Print("removed all segments")
	return nil
}

func (txn *Transaction) Import(stream ItemReader) error {
	if txn.Committed() {
		return ErrCommitted
	}

	segment, err := txn.db.createSegment(stream)
	if err != nil {
		return errors.Wrap(err, "failed to create a new segment")
	}

	txn.manifest.AddSegment(segment)

	log.Printf("imported %v docs to segment %v", segment.Meta.NumDocs, segment.ID)
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

func (txn *Transaction) compact() error {
	err := txn.Flush()
	if err != nil {
		return errors.Wrap(err, "flush failed")
	}

	mp := NewTieredMergePolicy()
	merges := mp.FindMerges(txn.manifest.Segments, 0)

	for _, merge := range merges {
		var readers []ItemReader
		for _, segment := range merge.Segments {
			readers = append(readers, segment.Reader())
		}
		segment, err := txn.db.createSegment(MergeItemReaders(readers...))
		if err != nil {
			return errors.Wrap(err, "segment merge failed")
		}
		txn.manifest.AddSegment(segment)
		for _, segment := range merge.Segments {
			txn.manifest.RemoveSegment(segment)
		}
		break
	}

	return nil
}
