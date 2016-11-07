package bitset

import (
	"encoding/binary"
	"io"
	"log"
)

type FixedBitSet struct {
	data     []uint64
	min, max uint32
}

func New(min, max uint32) *FixedBitSet {
	size := (max - min + 64) / 64
	return &FixedBitSet{data: make([]uint64, size), min: min, max: max}
}

func (bs *FixedBitSet) Add(i uint32) {
	if i < bs.min || i > bs.max {
		log.Panicf("bitset: %v is outside of the allowed range [%v,%v]", i, bs.min, bs.max)
	}
	i -= bs.min
	j := i / 64
	m := uint64(1) << (i % 64)
	bs.data[j] |= uint64(m)
}

func (bs *FixedBitSet) Remove(i uint32) {
	if i < bs.min || i > bs.max {
		log.Panicf("bitset: %v is outside of the allowed range [%v,%v]", i, bs.min, bs.max)
	}
	i -= bs.min
	j := i / 64
	m := uint64(1) << (i % 64)
	bs.data[j] &^= m
}

func (bs *FixedBitSet) Contains(i uint32) bool {
	if i < bs.min || i > bs.max {
		return false
	}
	i -= bs.min
	j := i / 64
	m := uint64(1) << (i % 64)
	return bs.data[j]&m != 0
}

func (bs *FixedBitSet) WriteTo(w io.Writer) (int64, error) {
	err := binary.Write(w, binary.LittleEndian, bs.data)
	if err != nil {
		return 0, err
	}
	return int64(binary.Size(bs.data)), nil
}

func (bs *FixedBitSet) ReadFrom(r io.Reader) (int64, error) {
	err := binary.Read(r, binary.LittleEndian, bs.data)
	if err != nil {
		return 0, err
	}
	return int64(binary.Size(bs.data)), nil
}
