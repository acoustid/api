package index

import (
	"io"
)

// ValueReader is an abstraction for iterating over Value.
type ValueReader interface {
	// NumDocs returns the number of docs this source of this reader contains.
	NumDocs() int

	// Read reads a block of Values.
	ReadBlock() (values []Value, err error)
}

type ValueSliceReader struct {
	numDocs int
	values  []Value
}

func NewValueSliceReader(numDocs int, values []Value) ValueReader {
	SortValues(values)
	return &ValueSliceReader{numDocs: numDocs, values: values}
}

func (r *ValueSliceReader) NumDocs() int {
	return r.numDocs
}

func (r *ValueSliceReader) ReadBlock() (values []Value, err error) {
	values = r.values
	r.values = r.values[:0]
	if len(values) == 0 {
		err = io.EOF
	}
	return
}

type MultiValueReader struct {
	reader1, reader2 ValueReader
	block1, block2   []Value
	buf              []Value
}

func MergeValueReaders(readers... ValueReader) ValueReader {
	switch len(readers) {
	case 0:
		return nil
	case 1:
		return readers[0]
	case 2:
		return &MultiValueReader{reader1: readers[0], reader2: readers[1]}
	}
	mid := len(readers)/2
	reader1 := MergeValueReaders(readers[:mid]...)
	reader2 := MergeValueReaders(readers[mid:]...)
	return &MultiValueReader{reader1: reader1, reader2: reader2}
}

func (r *MultiValueReader) NumDocs() int {
	return r.reader1.NumDocs() + r.reader2.NumDocs()
}

func (r *MultiValueReader) ReadBlock() (values []Value, err error) {
	if len(r.block1) == 0 && r.reader1 != nil {
		r.block1, err = r.reader1.ReadBlock()
		if err != nil {
			if err != io.EOF {
				return
			}
			r.reader1 = nil
			err = nil
		}
	}

	if len(r.block2) == 0 && r.reader2 != nil {
		r.block2, err = r.reader2.ReadBlock()
		if err != nil {
			if err != io.EOF {
				return
			}
			r.reader2 = nil
			err = nil
		}
	}

	if len(r.block1) > 0 && len(r.block2) > 0 {
		n := len(r.block1) + len(r.block2)
		if n <= cap(r.buf) {
			values = r.buf[:n]
		} else {
			values = make([]Value, n)
			r.buf = values
		}
		for i := range values {
			v1 := r.block1[0]
			v2 := r.block2[0]
			if v1.Term <= v2.Term || (v1.Term == v2.Term && v1.DocID <= v2.DocID) {
				values[i] = v1
				r.block1 = r.block1[1:]
				if len(r.block1) == 0 {
					values = values[:i+1]
					break
				}
			} else {
				values[i] = v2
				r.block2 = r.block2[1:]
				if len(r.block2) == 0 {
					values = values[:i+1]
					break
				}
			}
		}
		return
	}

	if len(r.block1) > 0 {
		values = r.block1
		r.block1 = r.block1[:0]
		return
	}

	if len(r.block2) > 0 {
		values = r.block2
		r.block2 = r.block2[:0]
		return
	}

	err = io.EOF
	return
}
