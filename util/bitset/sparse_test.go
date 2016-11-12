package bitset

import (
	"testing"
	"github.com/stretchr/testify/require"
)

func TestSparseBitSet(t *testing.T) {
	set := NewSparseBitSet()
	set.Add(1)
	require.True(t, set.Contains(1))
	require.False(t, set.Contains(0))
	require.False(t, set.Contains(2))
	set.Add(100)
	require.True(t, set.Contains(100))
	require.False(t, set.Contains(101))
	set.Remove(100)
	require.False(t, set.Contains(100))
	for i := uint32(0); i < 1024 * 16; i++ {
		set.Add(i)
		require.True(t, set.Contains(i))
		set.Remove(i)
		require.False(t, set.Contains(i))
	}
}
