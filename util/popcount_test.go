package util

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"math/rand"
)

func TestPopCount32(t *testing.T) {
	var bits [32]uint
	for i := 0; i < 32; i++ {
		bits[i] = uint(i)
	}
	for i := 1; i <= 32; i++ {
		for j := 0; j < 8; j++ {
			var x uint32
			p := rand.Perm(32)
			for k := range p[:i] {
				x |= uint32(1) << bits[k]
			}
			assert.Equal(t, i, PopCount32(x))
		}
	}
}

func TestPopCount64(t *testing.T) {
	var bits [64]uint
	for i := 0; i < 64; i++ {
		bits[i] = uint(i)
	}
	for i := 1; i <= 64; i++ {
		for j := 0; j < 8; j++ {
			var x uint64
			p := rand.Perm(64)
			for k := range p[:i] {
				x |= uint64(1) << bits[k]
			}
			assert.Equal(t, i, PopCount64(x))
		}
	}
}

func BenchmarkPopCount32(b *testing.B) {
	var bits [32]uint
	for i := 0; i < 32; i++ {
		bits[i] = uint(i)
	}
	var x [1024]uint32
	for i := range x {
		p := rand.Perm(32)
		b := rand.Int() % 32
		for k := range p[:b] {
			x[i] |= uint32(1) << bits[k]
		}
	}
	b.ResetTimer()
	b.SetBytes(4)
	for i := 0; i < b.N; i++ {
		PopCount32(x[i%len(x)])
	}
}

func BenchmarkPopCount64(b *testing.B) {
	var bits [64]uint
	for i := 0; i < 64; i++ {
		bits[i] = uint(i)
	}
	var x [1024]uint64
	for i := range x {
		p := rand.Perm(64)
		b := rand.Int() % 64
		for k := range p[:b] {
			x[i] |= uint64(1) << bits[k]
		}
	}
	b.ResetTimer()
	b.SetBytes(8)
	for i := 0; i < b.N; i++ {
		PopCount64(x[i%len(x)])
	}
}
