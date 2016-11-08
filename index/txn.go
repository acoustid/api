package index

import (
	"github.com/acoustid/go-acoustid/util/vfs"
	"github.com/pkg/errors"
	"math"
)

type Transaction struct {
	*Snapshot
	fs        vfs.FileSystem
	commitFn  func(m *Manifest) error
	committed bool
	counter   uint8
	buffer    ItemBuffer
}

const MaxBufferedItems = 10 * 1024 * 1024

var (
	ErrCommitted       = errors.New("transaction is already committed")
	ErrTooManySegments = errors.New("too many segments")
)

func (txn *Transaction) Add(docID uint32, terms []uint32) error {
	if txn.committed {
		return ErrCommitted
	}

	txn.buffer.Add(docID, terms)

	if txn.buffer.NumItems() > MaxBufferedItems {
		err := txn.Flush()
		if err != nil {
			return errors.Wrap(err, "flush failed")
		}
	}

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

	segment, err := txn.createSegment(txn.buffer.Reader())
	if err != nil {
		return errors.Wrap(err, "failed to create a new segment")
	}

	txn.manifest.AddSegment(segment)

	txn.buffer.Reset()

	return nil
}

func (txn *Transaction) createSegment(input ItemReader) (*Segment, error) {
	if txn.counter == math.MaxUint8 {
		return nil, ErrTooManySegments
	}
	txn.counter += 1
	return CreateSegment(txn.fs, NewSegmentID(txn.manifest.ID, txn.counter), input)
}

func (txn *Transaction) Commit() error {
	if txn.committed {
		return ErrCommitted
	}

	err := txn.Flush()
	if err != nil {
		return errors.Wrap(err, "flush failed")
	}

	err = txn.commitFn(txn.manifest)
	if err != nil {
		return errors.Wrap(err, "commit failed")
	}

	txn.committed = true
	return nil
}
