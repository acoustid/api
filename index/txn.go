// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package index

import (
	"github.com/pkg/errors"
	"go4.org/syncutil"
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

func (txn *Transaction) newBuffer() {
}

func (txn *Transaction) init() {
	txn.manifest = txn.snapshot.manifest.Clone()
	txn.buffer = new(ItemBuffer)
	txn.createdSegments = make(chan *Segment)
}

func (txn *Transaction) Add(docID uint32, terms []uint32) error {
	if txn.Committed() {
		return ErrCommitted
	}

	if txn.buffer.Delete(docID) {
		debugLog.Printf("deleted doc %v from the transaction buffer", docID)
	}

	txn.buffer.Add(docID, terms)
	debugLog.Printf("added doc %v to the transaction buffer", docID)

	txn.maybeFlush()

	return nil
}

func (txn *Transaction) Delete(docID uint32) error {
	if txn.Committed() {
		return ErrCommitted
	}

	if txn.buffer.Delete(docID) {
		debugLog.Printf("deleted doc %v from the transaction buffer", docID)
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

	debugLog.Print("removed all segments")
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
	debugLog.Printf("added imported segment %v", segment.ID)

	return nil
}

func (txn *Transaction) flush() {
	if txn.buffer.Empty() {
		return
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
}

func (txn *Transaction) maybeFlush() {
	done := false
	for !done {
		select {
		case segment := <-txn.createdSegments:
			txn.manifest.AddSegment(segment)
			debugLog.Printf("added asynchronously created segment %v", segment.ID)
		default:
			done = true
		}
	}

	if txn.buffer.NumItems() > MaxBufferedItems {
		txn.flush()
	}
}

func (txn *Transaction) waitForWriters() error {
	done := make(chan error)
	go func() { done <- txn.writers.Err() }()

	for {
		select {
		case segment := <-txn.createdSegments:
			txn.manifest.AddSegment(segment)
			debugLog.Printf("added asynchronously created segment %v", segment.ID)
		case err := <-done:
			return err
		}
	}
}

func (txn *Transaction) Commit() error {
	if txn.Committed() {
		return ErrCommitted
	}

	txn.flush()

	err := txn.waitForWriters()
	if err != nil {
		return errors.Wrap(err, "background bsegment writer failed")
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

func (txn *Transaction) Close() error {
	return txn.close.Do(func() error {
		var errs []error
		var err error
		err = txn.waitForWriters()
		if err != nil {
			errs = append(errs, err)
		}
		err = txn.snapshot.Close()
		if err != nil {
			errs = append(errs, err)
		}
		err = txn.closeFn(txn)
		if err != nil {
			errs = append(errs, err)
		}
		if len(errs) > 0 {
			return errs[0]
		}
		return nil
	})
}
