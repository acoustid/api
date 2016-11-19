// Copyright (C) 2016  Lukas Lalinsky
// Distributed under the MIT license, see the LICENSE file for details.

package intset

import (
	"math/rand"
	"testing"
)

func BenchmarkFixedBitSet_Contains(b *testing.B) {
	s := NewFixedBitSet(0, 0xffff)
	for i := 0; i < 1000; i++ {
		x := rand.Uint32() & 0xffff
		s.Add(x)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Contains(rand.Uint32() & 0xffff)
	}
}

func BenchmarkFixedBitSet_Add(b *testing.B) {
	b.ReportAllocs()
	s := NewFixedBitSet(0, 0xffff)
	for i := 0; i < b.N; i++ {
		for i := 0; i < 1000; i++ {
			s.Add(rand.Uint32() & 0xfffff)
		}
	}
}
