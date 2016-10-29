package index

import (
	"errors"
	"math"
)

type Transaction struct {
	Snapshot
	counter uint8
	closed  bool
}

var (
	ErrTransactionClosed = errors.New("transaction is already closed")
	ErrTooManySegments   = errors.New("too many segments")
)

func (txn *Transaction) AddDoc(docID uint32, terms []uint32) error {
	if txn.closed {
		return ErrTransactionClosed
	}

	segment, err := txn.createSegment(SingleDocIterator(docID, terms))
	if err != nil {
		return err
	}

	m := txn.manifest
	m.NumDocs += segment.Meta.NumDocs
	m.NumTerms += segment.Meta.NumTerms
	m.Checksum += segment.Meta.Checksum
	m.Segments = append(m.Segments, segment)

	return nil
}

func (txn *Transaction) createSegment(input TermsIterator) (*Segment, error) {
	if txn.counter == math.MaxUint8 {
		return nil, ErrTooManySegments
	}
	txn.counter += 1
	return CreateSegment(txn.db.dir, NewSegmentID(txn.manifest.ID, txn.counter), input)
}

func (txn *Transaction) Commit() error {
	err := txn.db.commit(txn)
	if err != nil {
		return err
	}
	txn.closed = true
	return nil
}
