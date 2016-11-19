// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package intset

type FixedBitSet struct {
	min, max uint32
	data     []uint64
}

func NewFixedBitSet(min, max uint32) *FixedBitSet {
	var s FixedBitSet
	s.min = min
	s.max = max
	s.data = make([]uint64, 1+int(max-min))
	return &s
}

func (s *FixedBitSet) Add(x uint32) {
	x -= s.min
	i := x / 64
	mask := uint64(1) << (x % 64)
	s.data[i] |= mask
}

func (s *FixedBitSet) Contains(x uint32) bool {
	if x < s.min || x > s.max {
		return false
	}
	x -= s.min
	i := x / 64
	mask := uint64(1) << (x % 64)
	return s.data[i]&mask != 0
}
