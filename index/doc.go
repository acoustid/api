package index

import (
	"sort"
	"github.com/cznic/sortutil"
)

type singleDocIterator struct {
	docid uint32
	terms []uint32
}

func SingleDocIterator(docid uint32, terms []uint32) TermsIterator {
	sort.Sort(sortutil.Uint32Slice(terms))
	return &singleDocIterator{docid: docid, terms: terms}
}

func (it *singleDocIterator) NumDocs() int {
	return 1
}

func (it *singleDocIterator) SeekTo(term uint32) (found bool, err error) {
	i := sort.Search(len(it.terms), func(i int) bool { return it.terms[i] >= term })
	if i < len(it.terms) && it.terms[i] == term {
		found = true
	} else {
		found = false
	}
	it.terms = it.terms[i:]
	return
}

func (r *singleDocIterator) Read(data []TermDocID) (n int, err error) {
	n = len(data)
	remaining := len(r.terms)
	if n >= remaining {
		n = remaining
	}
	for i := 0; i < n; i++ {
		data[i] = PackTermDocID(r.terms[i], r.docid)
	}
	r.terms = r.terms[n:]
	return
}
