package index

import (
	"errors"
	"math"
)

type Transaction struct {
	Snapshot
	counter   uint8
	committed bool
	values    []Value
	numDocs   int
}

var (
	ErrCommitted       = errors.New("transaction is already committed")
	ErrTooManySegments = errors.New("too many segments")
)

func (txn *Transaction) AddDoc(docID uint32, terms []uint32) error {
	if txn.committed {
		return ErrCommitted
	}

	for _, term := range terms {
		txn.values = append(txn.values, Value{DocID: docID, Term: term})
	}
	txn.numDocs += 1

	if len(txn.values) > 1024*1024 {
		err := txn.Flush()
		if err != nil {
			return err
		}
	}

	return nil
}

func (txn *Transaction) Flush() error {
	if txn.committed {
		return ErrCommitted
	}

	if len(txn.values) == 0 {
		return nil
	}

	segment, err := txn.createSegment(NewValueSliceReader(txn.numDocs, txn.values))
	if err != nil {
		return err
	}

	txn.manifest.AddSegment(segment)

	txn.values = txn.values[:0]
	txn.numDocs = 0

	return nil
}

func (txn *Transaction) createSegment(input ValueReader) (*Segment, error) {
	if txn.counter == math.MaxUint8 {
		return nil, ErrTooManySegments
	}
	txn.counter += 1
	return CreateSegment(txn.db.dir, NewSegmentID(txn.manifest.ID, txn.counter), input)
}

func (txn *Transaction) Commit() error {
	if txn.committed {
		return ErrCommitted
	}

	err := txn.Flush()
	if err != nil {
		return err
	}

	err = txn.db.commit(txn)
	if err != nil {
		return err
	}

	txn.committed = true
	return nil
}
