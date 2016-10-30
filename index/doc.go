package index

type SingleDocReader struct {
	docID uint32
	terms []uint32
	pos   int
}

func NewSingleDocReader(docID uint32, terms []uint32) ValueReader {
	SortUint32s(terms)
	return &SingleDocReader{docID: docID, terms: terms}
}

func (r *SingleDocReader) NumDocs() int {
	return 1
}

func (r *SingleDocReader) ReadValues(values []Value) (int, error) {
	n := len(r.terms) - r.pos
	for i := range values[:n] {
		values[i] = Value{Term: r.terms[r.pos+i], DocID: r.docID}
		n = i
	}
	r.pos += n
	return n, nil
}

type ValueSliceReader struct {
	numDocs int
	values  []Value
	pos     int
}

func NewValueSliceReader(numDocs int, values []Value) ValueReader {
	SortValues(values)
	return &ValueSliceReader{numDocs: numDocs, values: values}
}

func (r *ValueSliceReader) NumDocs() int {
	return r.numDocs
}

func (r *ValueSliceReader) ReadValues(values []Value) (int, error) {
	n := copy(values, r.values[r.pos:])
	r.pos += n
	return n, nil
}
