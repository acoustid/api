package index

import "sort"

type Uint32Slice []uint32

func (s Uint32Slice) Len() int           { return len(s) }
func (s Uint32Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// SortUint32s sorts a slice of uint32s in increasing order.
func SortUint32s(s []uint32) { sort.Sort(Uint32Slice(s)) }

// Value represents one (term,docID) pair in an inverted index.
type Value struct {
	Term  uint32
	DocID uint32
}

type ValueSlice []Value

func (s ValueSlice) Len() int { return len(s) }
func (s ValueSlice) Less(i, j int) bool {
	return s[i].Term < s[j].Term || (s[i].Term == s[j].Term && s[i].DocID < s[j].DocID)
}
func (s ValueSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// SortValues sorts a slice of Values in increasing order.
func SortValues(s []Value) { sort.Sort(ValueSlice(s)) }
