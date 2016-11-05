package intcompress

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestUnpackInt3Array(t *testing.T) {
	assert.Equal(t, []uint8{}, UnpackUint3Slice([]byte{}))
	assert.Equal(t, []uint8{1, 2}, UnpackUint3Slice([]byte{0x11}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5}, UnpackUint3Slice([]byte{0xd1, 0x58}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6, 7, 0}, UnpackUint3Slice([]byte{0xd1, 0x58, 0x1f}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5}, UnpackUint3Slice([]byte{0xd1, 0x58, 0x1f, 0xd1, 0x58}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0}, UnpackUint3Slice([]byte{0xd1, 0x58, 0x1f, 0xd1, 0x58, 0x1f}))
}

func TestUnpackInt5Array(t *testing.T) {
	assert.Equal(t, []uint8{}, UnpackUint5Slice([]byte{}))
	assert.Equal(t, []uint8{}, UnpackUint5Slice([]byte{}))
	assert.Equal(t, []uint8{1}, UnpackUint5Slice([]byte{0x1}))
	assert.Equal(t, []uint8{1, 2, 3}, UnpackUint5Slice([]byte{0x41, 0xc}))
	assert.Equal(t, []uint8{1, 2, 3, 4}, UnpackUint5Slice([]byte{0x41, 0x0c, 0x2}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6}, UnpackUint5Slice([]byte{0x41, 0x0c, 0x52, 0xc}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6, 7, 8}, UnpackUint5Slice([]byte{0x41, 0x0c, 0x52, 0xcc, 0x41}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9}, UnpackUint5Slice([]byte{0x41, 0x0c, 0x52, 0xcc, 0x41, 0x9}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, UnpackUint5Slice([]byte{0x41, 0x0c, 0x52, 0xcc, 0x41, 0x49, 0x2d}))
	assert.Equal(t, []uint8{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, UnpackUint5Slice([]byte{0x41, 0x0c, 0x52, 0xcc, 0x41, 0x49, 0x2d, 0x6}))
}

func BenchmarkUnpackInt3Array(b *testing.B) {
	r := rand.New(rand.NewSource(1234))
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(r.Uint32() & 0xff)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		UnpackUint3Slice(data)
	}
}

func BenchmarkUnpackInt5Array(b *testing.B) {
	r := rand.New(rand.NewSource(1234))
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(r.Uint32() & 0xff)
	}
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		UnpackUint5Slice(data)
	}
}
