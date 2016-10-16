package chromaprint

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"math/rand"
)

func TestUnpackInt3Array(t *testing.T) {
	assert.Equal(t, []int{ }, unpackInt3Array([]byte{ }))
	assert.Equal(t, []int{ 1, 2 }, unpackInt3Array([]byte{ 0x11 }))
	assert.Equal(t, []int{ 1, 2, 3, 4, 5 }, unpackInt3Array([]byte{ 0xd1, 0x58 }))
	assert.Equal(t, []int{ 1, 2, 3, 4, 5, 6, 7, 0 }, unpackInt3Array([]byte{ 0xd1, 0x58, 0x1f }))
	assert.Equal(t, []int{ 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5 }, unpackInt3Array([]byte{ 0xd1, 0x58, 0x1f, 0xd1, 0x58 }))
	assert.Equal(t, []int{ 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0 }, unpackInt3Array([]byte{ 0xd1, 0x58, 0x1f, 0xd1, 0x58, 0x1f }))
}

func TestUnpackInt5Array(t *testing.T) {
	assert.Equal(t, []int{  }, unpackInt5Array([]byte{  }))
	assert.Equal(t,	[]int{ 1 },	unpackInt5Array([]byte{ 0x1 }))
	assert.Equal(t,	[]int{ 1, 2, 3 }, unpackInt5Array([]byte{ 0x41, 0xc }))
	assert.Equal(t,	[]int{ 1, 2, 3, 4 }, unpackInt5Array([]byte{ 0x41, 0x0c, 0x2 }))
	assert.Equal(t,	[]int{ 1, 2, 3, 4, 5, 6 }, unpackInt5Array([]byte{ 0x41, 0x0c, 0x52, 0xc }))
	assert.Equal(t,	[]int{ 1, 2, 3, 4, 5, 6, 7, 8 }, unpackInt5Array([]byte{ 0x41, 0x0c, 0x52, 0xcc, 0x41 }))
	assert.Equal(t,	[]int{ 1, 2, 3, 4, 5, 6, 7, 8, 9 },	unpackInt5Array([]byte{ 0x41, 0x0c, 0x52, 0xcc, 0x41, 0x9 }))
	assert.Equal(t,	[]int{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 },	unpackInt5Array([]byte{ 0x41, 0x0c, 0x52, 0xcc, 0x41, 0x49, 0x2d }))
	assert.Equal(t,	[]int{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 },	unpackInt5Array([]byte{ 0x41, 0x0c, 0x52, 0xcc, 0x41, 0x49, 0x2d, 0x6 }))
}

func BenchmarkUnpackInt3Array(b *testing.B) {
	data := make([]byte, 1024)
	for n := 0; n < b.N; n++ {
		for i := range data {
			data[i] = byte(rand.Uint32() & 0xff)
		}
		unpackInt3Array(data)
	}
}

func BenchmarkUnpackInt5Array(b *testing.B) {
	data := make([]byte, 1024)
	for n := 0; n < b.N; n++ {
		for i := range data {
			data[i] = byte(rand.Uint32() & 0xff)
		}
		unpackInt5Array(data)
	}
}
