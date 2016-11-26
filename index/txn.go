// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/pkg/errors"
	"go4.org/syncutil"
	"log"
)

type Transaction struct {
	snapshot        *Snapshot
	manifest        *Manifest
	db              *DB
	buffer          *ItemBuffer
	close           syncutil.Once
	closeFn         func(tx *Transaction) error
	writers         syncutil.Group
	createdSegments chan *Segment
}

const MaxBufferedItems = 10 * 1024 * 1024

var ErrCommitted = errors.New("transaction is already committed")

func (tx *Transaction) init() {
	tx.manifest = tx.snapshot.manifest.Clone()
	tx.buffer = new(ItemBuffer)
	tx.createdSegments = make(chan *Segment)
}

func (txn *Transaction) Add(docID uint32, terms []uint32) error {
	if txn.Committed() {
		return ErrCommitted
	}

	if txn.buffer.Delete(docID) {
		//log.Printf("deleted doc %v from the transaction buffer", docID)
	}

	txn.buffer.Add(docID, terms)
	//log.Printf("added doc %v to the transaction buffer", docID)

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
		//log.Printf("deleted doc %v from the transaction buffer", docID)
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

	//log.Printf("imported %v docs to segment %v", segment.Meta.NumDocs, segment.ID)
	return nil
}

func (txn *Transaction) Flush() error {
	if txn.Committed() {
		return ErrCommitted
	}

	done := false
	for !done {
		select {
		case segment := <-txn.createdSegments:
			txn.manifest.AddSegment(segment)
		default:
			done = true
		}
	}

	if txn.buffer.Empty() {
		return nil
	}

	buffer := txn.buffer
	txn.buffer = new(ItemBuffer)

	txn.writers.Go(func() error {
		segment, err := txn.db.createSegment(buffer.Reader())
		if err != nil {
			return errors.Wrap(err, "failed to create a new segment")
		}
		txn.createdSegments <- segment
		return nil
	})

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

	go func() {
		txn.writers.Wait()
		close(txn.createdSegments)
	}()

	for segment := range txn.createdSegments {
		txn.manifest.AddSegment(segment)
	}

	err = txn.writers.Err()
	if err != nil {
		return errors.Wrap(err, "segment writer failed")
	}

	return txn.db.commit(func(base *Manifest) (*Manifest, error) {
		if base.ID != txn.snapshot.manifest.ID {
			err := txn.manifest.rebase(base)
			if err != nil {
				return nil, err
			}
		}
		return txn.manifest, nil
	})
}

func (txn *Transaction) Committed() bool {
	return txn.manifest.ID != 0
}

func (tx *Transaction) Close() error {
	err1 := tx.close.Do(func() error { return tx.closeFn(tx) })
	err2 := tx.snapshot.Close()
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}
