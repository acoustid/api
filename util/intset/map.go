package intset

type MapSet struct {
	data map[uint32]struct{}
}

func NewMapSet() *MapSet {
	var s MapSet
	s.data = make(map[uint32]struct{})
	return &s
}

func (s *MapSet) Add(x uint32) {
	s.data[x] = struct{}{}
}

func (s *MapSet) Delete(x uint32) {
	delete(s.data, x)
}

func (s MapSet) Contains(x uint32) bool {
	_, exists := s.data[x]
	return exists
}
