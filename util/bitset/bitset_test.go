package bitset

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestFixedBitSet(t *testing.T) {
	bs := New(1, 64)
	assert.Equal(t, uint32(0), bs.min)
	assert.Equal(t, uint32(64), bs.max)

	bs.Add(1)
	bs.Add(64)
	assert.Equal(t, []uint64{0x2, 0x1}, bs.data)

	assert.False(t, bs.Contains(0))
	assert.True(t, bs.Contains(1))
	assert.False(t, bs.Contains(2))
	assert.True(t, bs.Contains(64))
	assert.False(t, bs.Contains(65))

	bs.Remove(2)
	assert.Equal(t, []uint64{0x2, 0x1}, bs.data)
	assert.True(t, bs.Contains(1))
	assert.False(t, bs.Contains(2))
	assert.True(t, bs.Contains(64))

	bs.Remove(1)
	assert.Equal(t, []uint64{0x0, 0x1}, bs.data)
	assert.False(t, bs.Contains(1))
	assert.False(t, bs.Contains(2))
	assert.True(t, bs.Contains(64))
}
