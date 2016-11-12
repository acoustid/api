package bitset

type Uint32Set interface {
	Add(i uint32)
	Remove(i uint32)
	Contains(i uint32) bool
}
