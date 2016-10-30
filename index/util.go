package index

import "sort"

type Uint32Slice []uint32

func (s Uint32Slice) Len() int           { return len(s) }
func (s Uint32Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// SortUint32s sorts a slice of uint32s in increasing order.
func SortUint32s(s []uint32) { sort.Sort(Uint32Slice(s)) }

// TermsIterator is an abstraction for iterating over TermDocIDs.
type TermsIterator interface {
	// NumDocs returns the number of docs this iterator contains.
	NumDocs() int

	// SeekTo fast-forwards the iterator to a particular term.
	// Returns true in found if the term was found, in which case
	// you can use Read to get the data.
	SeekTo(term uint32) (found bool, err error)

	// Read reads a block of TermDocIDs. It tries to fill the given buffer
	// and returns the number of items added there. If the result is 0,
	// the EOF has been reached. Note that this might return a non-zero result
	// and an error at the same time.
	Read(data []TermDocID) (n int, err error)
}

// TermDocID is a (term,docid) pair packed into a 64-bit integer.
type TermDocID uint64

func PackTermDocID(term uint32, docID uint32) TermDocID {
	return TermDocID(uint64(term)<<32 | uint64(docID))
}

func (x TermDocID) Unpack() (term uint32, docID uint32) {
	term = x.Term()
	docID = x.DocID()
	return
}

func (x TermDocID) Pack() uint64 {
	return uint64(x)
}

func (x TermDocID) Term() uint32  { return uint32((x >> 32) & 0xffffffff) }
func (x TermDocID) DocID() uint32 { return uint32(x & 0xffffffff) }
